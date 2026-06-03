from fastapi import FastAPI, UploadFile, File, Form, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
import threading
import os
import tempfile
import logging
import time
import sqlite3
import json
from tasks import create_task, update_task, get_task, request_stop
from processor_web import process_csv
from local_cache import (
    get_unsynced_tracks, get_unsynced_links, mark_synced,
    init_dbs, seed_tracks_from_supabase, seed_links_from_supabase, TRACKS_DB
)
from backup_web import batch_upsert_tracks
from cache_web import batch_insert_links, _supabase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "").split(",")
if not ALLOWED_ORIGINS or ALLOWED_ORIGINS == [""]:
    ALLOWED_ORIGINS = ["*"]
    logger.warning("ALLOWED_ORIGINS not set – allowing all origins.")

# Admin API secret (only for /admin/* endpoints)
API_SECRET = os.environ.get("API_SECRET", None)
if not API_SECRET:
    logger.error("API_SECRET environment variable not set – admin endpoints will be unprotected!")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=500)

# ------------------------------------------------------------
# Middleware: require API key ONLY for admin endpoints
# ------------------------------------------------------------
@app.middleware("http")
async def require_api_key(request: Request, call_next):
    # Allow preflight OPTIONS requests
    if request.method == "OPTIONS":
        return await call_next(request)

    # Public paths (no API key required)
    public_paths = [
        "/status/keepalive",
        "/process",
        "/status/",
        "/result/",
        "/cancel/",
        "/"
    ]
    if any(request.url.path.startswith(p) for p in public_paths):
        return await call_next(request)

    # Admin paths require API key
    if request.url.path.startswith("/admin/"):
        api_key = request.headers.get("X-API-Key")
        if not API_SECRET or api_key != API_SECRET:
            return JSONResponse(status_code=403, content={"error": "Forbidden – invalid or missing admin API key"})

    return await call_next(request)

# ------------------------------------------------------------
# Admin endpoint: reset entire local cache and re‑seed from Supabase
# ------------------------------------------------------------
@app.post("/admin/reset-cache")
async def reset_cache(request: Request):
    api_key = request.headers.get("X-API-Key")
    if not API_SECRET or api_key != API_SECRET:
        return JSONResponse(status_code=403, content={"error": "Forbidden"})

    try:
        if os.path.exists("tracks_cache.db"):
            os.remove("tracks_cache.db")
            logger.info("Deleted tracks_cache.db")
        if os.path.exists("links_cache.db"):
            os.remove("links_cache.db")
            logger.info("Deleted links_cache.db")

        init_dbs()
        seed_tracks_from_supabase(_supabase)
        seed_links_from_supabase(_supabase)

        return {"status": "Cache reset and re‑seeded successfully from Supabase"}
    except Exception as e:
        logger.error(f"Reset cache failed: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

# ------------------------------------------------------------
# Admin endpoint: clear a single Spotify URI from the local cache
# ------------------------------------------------------------
@app.post("/admin/clear-uri")
async def clear_uri(request: Request):
    api_key = request.headers.get("X-API-Key")
    if not API_SECRET or api_key != API_SECRET:
        return JSONResponse(status_code=403, content={"error": "Forbidden"})

    try:
        body = await request.json()
    except:
        return JSONResponse(status_code=400, content={"error": "Invalid JSON body"})

    spotify_uri = body.get("spotify_uri")
    if not spotify_uri:
        return JSONResponse(status_code=400, content={"error": "Missing 'spotify_uri' field"})

    try:
        # Delete from tracks_cache
        conn_tracks = sqlite3.connect("tracks_cache.db")
        cursor_tracks = conn_tracks.cursor()
        cursor_tracks.execute("DELETE FROM tracks_cache WHERE spotify_uri = ?", (spotify_uri,))
        conn_tracks.commit()
        rows_tracks = cursor_tracks.rowcount
        conn_tracks.close()

        # Delete from links_cache
        conn_links = sqlite3.connect("links_cache.db")
        cursor_links = conn_links.cursor()
        cursor_links.execute("DELETE FROM links_cache WHERE spotify_uri = ?", (spotify_uri,))
        conn_links.commit()
        rows_links = cursor_links.rowcount
        conn_links.close()

        return {
            "status": "success",
            "deleted_from_tracks": rows_tracks,
            "deleted_from_links": rows_links,
            "spotify_uri": spotify_uri
        }
    except Exception as e:
        logger.error(f"Failed to clear URI {spotify_uri}: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

# ------------------------------------------------------------
# NEW: Admin endpoint – refresh a single URI from Supabase (on‑demand seeding)
# ------------------------------------------------------------
@app.post("/admin/refresh-uri")
async def refresh_uri(request: Request):
    api_key = request.headers.get("X-API-Key")
    if not API_SECRET or api_key != API_SECRET:
        return JSONResponse(status_code=403, content={"error": "Forbidden"})

    try:
        body = await request.json()
    except:
        return JSONResponse(status_code=400, content={"error": "Invalid JSON body"})

    spotify_uri = body.get("spotify_uri")
    if not spotify_uri:
        return JSONResponse(status_code=400, content={"error": "Missing 'spotify_uri' field"})

    try:
        # 1. Fetch track from Supabase (if exists)
        track_res = _supabase.table("tracks").select("*").eq("spotify_uri", spotify_uri).execute()
        track_data = track_res.data[0] if track_res.data else None

        # 2. Fetch link from Supabase (if exists)
        link_res = _supabase.table("links").select("*").eq("spotify_uri", spotify_uri).execute()
        link_data = link_res.data[0] if link_res.data else None

        # 3. Update tracks_cache
        conn_tracks = sqlite3.connect("tracks_cache.db")
        cursor_tracks = conn_tracks.cursor()
        if track_data:
            # Replace or insert, set synced = 1
            cursor_tracks.execute(
                "INSERT OR REPLACE INTO tracks_cache (spotify_uri, full_row_json, synced) VALUES (?, ?, 1)",
                (spotify_uri, json.dumps(track_data))
            )
        else:
            # No track in Supabase – delete from local cache
            cursor_tracks.execute("DELETE FROM tracks_cache WHERE spotify_uri = ?", (spotify_uri,))
        conn_tracks.commit()
        conn_tracks.close()

        # 4. Update links_cache
        conn_links = sqlite3.connect("links_cache.db")
        cursor_links = conn_links.cursor()
        if link_data:
            youtube_link = link_data.get("youtube_link")
            duration_seconds = link_data.get("duration_seconds")
            title = link_data.get("title")
            cursor_links.execute(
                """INSERT OR REPLACE INTO links_cache 
                   (spotify_uri, youtube_link, duration_seconds, title, full_row_json, synced) 
                   VALUES (?, ?, ?, ?, ?, 1)""",
                (spotify_uri, youtube_link, duration_seconds, title, json.dumps(link_data))
            )
        else:
            cursor_links.execute("DELETE FROM links_cache WHERE spotify_uri = ?", (spotify_uri,))
        conn_links.commit()
        conn_links.close()

        return {
            "status": "success",
            "spotify_uri": spotify_uri,
            "track_updated": track_data is not None,
            "link_updated": link_data is not None
        }
    except Exception as e:
        logger.error(f"Failed to refresh URI {spotify_uri}: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

# ------------------------------------------------------------
# One‑time seeding
# ------------------------------------------------------------
def initialize_local_cache():
    init_dbs()
    try:
        with sqlite3.connect(TRACKS_DB) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM tracks_cache")
            count = cursor.fetchone()[0]
            if count > 0:
                logger.info("Local cache already populated, skipping seed.")
                return
    except sqlite3.OperationalError:
        pass
    logger.info("Local cache is empty. Seeding from Supabase...")
    try:
        seed_tracks_from_supabase(_supabase)
        seed_links_from_supabase(_supabase)
        logger.info("Seeding completed successfully.")
    except Exception as e:
        logger.error(f"Seeding failed: {e}")

initialize_local_cache()

# ------------------------------------------------------------
# Background sync worker
# ------------------------------------------------------------
def sync_worker():
    while True:
        try:
            unsynced_tracks = get_unsynced_tracks(limit=100)
            if unsynced_tracks:
                rows = []
                uris = []
                for uri, row_json in unsynced_tracks:
                    try:
                        rows.append(json.loads(row_json))
                        uris.append(uri)
                    except Exception as e:
                        logger.error(f"Failed to parse track JSON for {uri}: {e}")
                if rows:
                    batch_upsert_tracks(rows)
                    mark_synced(uris, "tracks")
                    logger.info(f"Synced {len(rows)} tracks to Supabase")
            unsynced_links = get_unsynced_links(limit=100)
            if unsynced_links:
                rows = []
                uris = []
                for link_row in unsynced_links:
                    try:
                        if link_row["full_row_json"]:
                            row_dict = json.loads(link_row["full_row_json"])
                        else:
                            row_dict = {
                                "spotify_uri": link_row["spotify_uri"],
                                "youtube_link": link_row["youtube_link"],
                                "duration_seconds": link_row["duration_seconds"],
                                "title": link_row["title"]
                            }
                        rows.append(row_dict)
                        uris.append(link_row["spotify_uri"])
                    except Exception as e:
                        logger.error(f"Failed to parse link JSON for {link_row['spotify_uri']}: {e}")
                if rows:
                    batch_insert_links(rows)
                    mark_synced(uris, "links")
                    logger.info(f"Synced {len(rows)} links to Supabase")
        except Exception as e:
            logger.error(f"Sync worker error: {e}")
        time.sleep(60)

sync_thread = threading.Thread(target=sync_worker, daemon=True)
sync_thread.start()

# ------------------------------------------------------------
# Frontend serving (optional)
# ------------------------------------------------------------
FRONTEND_PATH = os.path.join(os.path.dirname(__file__), "..", "docs", "index.html")
if os.path.exists(FRONTEND_PATH):
    @app.get("/")
    async def serve_frontend():
        with open(FRONTEND_PATH, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
else:
    @app.get("/")
    async def root():
        return {"message": "Backend API is running. Use /docs for interactive documentation."}

@app.get("/status/keepalive")
async def keepalive():
    return {"status": "alive"}

@app.post("/process")
async def start_processing(
    file: UploadFile = File(...),
    api_key: str = Form(...),
    delay: float = Form(2.0),
    limit_type: str = Form("full"),
    limit_value: int = Form(0),
):
    input_path = os.path.join(tempfile.gettempdir(), f"input_{file.filename}")
    with open(input_path, "wb") as f:
        f.write(await file.read())
    task_id = create_task()
    output_path = os.path.join(tempfile.gettempdir(), f"output_{task_id}.csv")
    update_task(task_id, status='running', result_path=output_path)
    settings = {
        'api_key': api_key,
        'delay': delay,
        'limit_type': limit_type,
        'limit_value': limit_value
    }
    threading.Thread(
        target=process_csv,
        args=(input_path, output_path, settings, task_id, task_id),
        daemon=True
    ).start()
    return {"task_id": task_id}

@app.get("/status/{task_id}")
async def get_status(task_id: str):
    task = get_task(task_id)
    if not task:
        return {"error": "Task not found"}
    return {
        "status": task['status'],
        "logs": task['logs'],
        "progress": task['progress'],
        "download_ready": task['status'] == 'done'
    }

@app.get("/result/{task_id}")
async def download_result(task_id: str):
    task = get_task(task_id)
    if not task or task['status'] != 'done':
        return {"error": "Not ready"}
    return FileResponse(task['result_path'], filename="processed.csv")

@app.post("/cancel/{task_id}")
async def cancel_task(task_id: str):
    request_stop(task_id)
    return {"status": "cancel requested"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

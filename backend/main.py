from fastapi import FastAPI, UploadFile, File, Form, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
import threading
import os
import tempfile
import logging
import time
import sqlite3                     # <-- added for cache check
import json
from tasks import create_task, update_task, get_task, request_stop
from processor_web import process_csv
from local_cache import (
    get_unsynced_tracks, get_unsynced_links, mark_synced,
    init_dbs, seed_tracks_from_supabase, seed_links_from_supabase, TRACKS_DB
)
from backup_web import batch_upsert_tracks
from cache_web import batch_insert_links, _supabase   # <-- import Supabase client

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------
# SECURITY CONFIGURATION (read from environment variables)
# ------------------------------------------------------------
ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "").split(",")
if not ALLOWED_ORIGINS or ALLOWED_ORIGINS == [""]:
    ALLOWED_ORIGINS = ["*"]
    logger.warning("ALLOWED_ORIGINS not set – allowing all origins. Set this to your frontend domain.")

API_SECRET = os.environ.get("API_SECRET", None)
if not API_SECRET:
    logger.error("API_SECRET environment variable not set – API will be unprotected!")

app = FastAPI()

# CORS: only your frontend domains
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["POST", "GET"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=500)

# ------------------------------------------------------------
# Middleware: enforce API key on all endpoints except health check
# ------------------------------------------------------------
@app.middleware("http")
async def require_api_key(request: Request, call_next):
    if request.url.path.startswith("/status/keepalive") or (request.url.path == "/" and request.method == "GET"):
        return await call_next(request)
    api_key = request.headers.get("X-API-Key")
    if not API_SECRET or api_key != API_SECRET:
        return JSONResponse(status_code=403, content={"error": "Forbidden – invalid or missing API key"})
    return await call_next(request)

# ------------------------------------------------------------
# One‑time seeding: populate local cache from Supabase if empty
# ------------------------------------------------------------
def initialize_local_cache():
    """Seed the local SQLite cache from Supabase if it's empty."""
    init_dbs()   # ensure tables exist
    try:
        with sqlite3.connect(TRACKS_DB) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM tracks_cache")
            count = cursor.fetchone()[0]
            if count > 0:
                logger.info("Local cache already populated, skipping seed.")
                return
    except sqlite3.OperationalError:
        # Table may not exist (init_dbs already created it, but just in case)
        pass

    logger.info("Local cache is empty. Seeding from Supabase...")
    try:
        seed_tracks_from_supabase(_supabase)
        seed_links_from_supabase(_supabase)
        logger.info("Seeding completed successfully.")
    except Exception as e:
        logger.error(f"Seeding failed: {e}")

# Run the seeding once at startup
initialize_local_cache()

# ------------------------------------------------------------
# Background sync worker (uploads pending tracks/links to Supabase)
# ------------------------------------------------------------
def sync_worker():
    while True:
        try:
            # Sync tracks
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

            # Sync links
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

# Start the sync daemon thread
sync_thread = threading.Thread(target=sync_worker, daemon=True)
sync_thread.start()

# ------------------------------------------------------------
# Frontend serving (if docs/index.html exists)
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

# ------------------------------------------------------------
# Health check (no API key required)
# ------------------------------------------------------------
@app.get("/status/keepalive")
async def keepalive():
    return {"status": "alive"}

# ------------------------------------------------------------
# Process CSV endpoint (requires API key via header)
# ------------------------------------------------------------
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

# ------------------------------------------------------------
# Status endpoint (requires API key via header)
# ------------------------------------------------------------
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

# ------------------------------------------------------------
# Result download endpoint (requires API key via header)
# ------------------------------------------------------------
@app.get("/result/{task_id}")
async def download_result(task_id: str):
    task = get_task(task_id)
    if not task or task['status'] != 'done':
        return {"error": "Not ready"}
    return FileResponse(task['result_path'], filename="processed.csv")

# ------------------------------------------------------------
# Cancel endpoint (requires API key via header)
# ------------------------------------------------------------
@app.post("/cancel/{task_id}")
async def cancel_task(task_id: str):
    request_stop(task_id)
    return {"status": "cancel requested"}

# ------------------------------------------------------------
# Run the server (only when executed directly)
# ------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
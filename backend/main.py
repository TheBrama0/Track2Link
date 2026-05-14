from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import threading
import os
import tempfile
from tasks import create_task, get_task
from processor_web import process_csv

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Optional: serve frontend HTML when running locally (for testing)
# On Render, this path will likely not exist, so it's ignored.
FRONTEND_PATH = os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html")
if os.path.exists(FRONTEND_PATH):
    @app.get("/")
    async def serve_frontend():
        with open(FRONTEND_PATH, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
else:
    @app.get("/")
    async def root():
        return {"message": "Backend API is running. Use /docs for interactive documentation."}

# Health check endpoint for Render (and keep-alive pings)
@app.get("/status/keepalive")
async def keepalive():
    return {"status": "alive"}

@app.post("/process")
async def start_processing(
    file: UploadFile = File(...),
    api_key: str = Form(...),
    delay: float = Form(1.0),
    limit_type: str = Form("full"),
    limit_value: int = Form(0),
):
    # Save uploaded file to system temp directory (works on Windows/Linux/Mac)
    file_path = os.path.join(tempfile.gettempdir(), file.filename)
    with open(file_path, "wb") as f:
        f.write(await file.read())

    task_id = create_task()
    task = get_task(task_id)
    task['status'] = 'running'

    settings = {
        'api_key': api_key,
        'delay': delay,
        'limit_type': limit_type,
        'limit_value': limit_value
    }
    # Run processing in background thread
    # Use task_id as both the session identifier (username) and the task ID
    threading.Thread(
        target=process_csv,
        args=(file_path, settings, task_id, task_id),   # username = task_id
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

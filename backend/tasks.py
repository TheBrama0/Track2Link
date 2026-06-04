import uuid
import threading
import time

_tasks = {}
_lock = threading.Lock()

# ------------------------------------------------------------
# Task management
# ------------------------------------------------------------
def create_task():
    task_id = str(uuid.uuid4())
    with _lock:
        _tasks[task_id] = {
            'logs': [],
            'progress': 0,
            'status': 'pending',
            'result_path': None,
            'stop_requested': False,
            'created_at': time.time(),
            'completed_at': None
        }
    return task_id

def get_task(task_id):
    with _lock:
        return _tasks.get(task_id)

def update_task(task_id, **kwargs):
    """Thread‑safe partial update of a task."""
    with _lock:
        if task_id in _tasks:
            _tasks[task_id].update(kwargs)
            # If status changed to a terminal state, record completion time
            if kwargs.get('status') in ('done', 'error', 'cancelled'):
                if _tasks[task_id].get('completed_at') is None:
                    _tasks[task_id]['completed_at'] = time.time()

def request_stop(task_id):
    with _lock:
        if task_id in _tasks:
            _tasks[task_id]['stop_requested'] = True

# ------------------------------------------------------------
# Cleanup: remove tasks older than max_age_seconds
# ------------------------------------------------------------
def cleanup_old_tasks(max_age_seconds=3600):
    """Remove tasks that have been in a terminal state for longer than max_age_seconds."""
    now = time.time()
    with _lock:
        to_delete = []
        for task_id, task in _tasks.items():
            if task.get('status') in ('done', 'error', 'cancelled'):
                completed_at = task.get('completed_at')
                if completed_at and (now - completed_at) > max_age_seconds:
                    to_delete.append(task_id)
        for task_id in to_delete:
            del _tasks[task_id]
        if to_delete:
            print(f"[TaskCleanup] Removed {len(to_delete)} old tasks. Remaining: {len(_tasks)}")

def _cleanup_worker():
    """Background thread that periodically cleans up old tasks."""
    while True:
        time.sleep(300)   # every 5 minutes
        cleanup_old_tasks()

# Start the cleanup daemon thread automatically when this module is imported
_cleanup_thread = threading.Thread(target=_cleanup_worker, daemon=True)
_cleanup_thread.start()
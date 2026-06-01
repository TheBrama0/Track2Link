import uuid
import threading

_tasks = {}
_lock = threading.Lock()

def create_task():
    task_id = str(uuid.uuid4())
    with _lock:
        _tasks[task_id] = {
            'logs': [],
            'progress': 0,
            'status': 'pending',
            'result_path': None,
            'stop_requested': False
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

def request_stop(task_id):
    with _lock:
        if task_id in _tasks:
            _tasks[task_id]['stop_requested'] = True
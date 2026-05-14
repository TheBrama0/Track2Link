import threading
import uuid

tasks = {}   # task_id -> {'logs': [], 'progress': 0, 'status': 'running', 'result_file': None}

def create_task():
    task_id = str(uuid.uuid4())
    tasks[task_id] = {
        'logs': [],
        'progress': 0,
        'status': 'pending',
        'result_path': None
    }
    return task_id

def get_task(task_id):
    return tasks.get(task_id)
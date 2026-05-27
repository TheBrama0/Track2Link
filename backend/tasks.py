import uuid

tasks = {}   # task_id -> {'logs': [], 'progress': 0, 'status': 'running', 'result_path': None, 'stop_requested': False}

def create_task():
    task_id = str(uuid.uuid4())
    tasks[task_id] = {
        'logs': [],
        'progress': 0,
        'status': 'pending',
        'result_path': None,
        'stop_requested': False          # <-- added
    }
    return task_id

def get_task(task_id):
    return tasks.get(task_id)

def request_stop(task_id):
    task = get_task(task_id)
    if task:
        task['stop_requested'] = True
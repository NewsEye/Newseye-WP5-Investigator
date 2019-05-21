from flask import current_app
from flask_login import current_user
from app.analysis.planner import TaskPlanner
from app.main.db_utils import generate_tasks
from app.models import Task, User
import threading
import time
import asyncio


def execute_tasks(queries, return_tasks=True):
    """
    Generate tasks from queries and execute them.
    :param queries: a single query or a list of queries
    :param return_tasks: If true, the task object (or a list of task objects) is returned to the user in json format.
    If false, only the task_id (or a list of task_ids) is returned
    :return: A list of task_objects or task_ids corresponding to the queries.
    """
    task_uuids = generate_tasks(queries)
    t = threading.Thread(target=task_thread,
                         args=[current_app._get_current_object(), current_user.id, task_uuids])
    t.setDaemon(False)
    t.start()

    # Wait until the thread has started the tasks before responding to the user
    i = 0
    while Task.query.filter(Task.uuid.in_(task_uuids), Task.task_status == 'created').count() > 0:
        time.sleep(1)

    if return_tasks:
        return Task.query.filter(Task.uuid.in_(task_uuids)).all()
    else:
        return task_uuids


def task_thread(app, user_id, task_uuid):
    with app.app_context():
        loop = asyncio.new_event_loop()
        planner = TaskPlanner(loop, User.query.get(user_id))
        loop.run_until_complete(
            planner.execute_user_task(task_uuid))

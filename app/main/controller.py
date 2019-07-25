from flask import current_app
from flask_login import current_user
from app.main.planner import TaskPlanner
from app.main.db_utils import generate_tasks
from app.models import TaskInstance, User
import threading
import time
import asyncio


def execute_tasks(queries):
    """
    Generate tasks from queries and execute them.
    :param queries: a single query or a list of queries
    :return: A list of task_objects or task_ids corresponding to the queries.
    """
    task_uuids = generate_tasks(queries)
    t = threading.Thread(target=task_thread,
                         args=[current_app._get_current_object(), current_user.id, task_uuids])
    t.setDaemon(False)
    t.start()

    # Wait until the thread has started the tasks before responding to the user
    i = 0
    while TaskInstance.query.filter(TaskInstance.uuid.in_(task_uuids), TaskInstance.task_status == 'created').count() > 0:
        time.sleep(1)

    current_app.logger.debug(TaskInstance.query.filter(TaskInstance.uuid.in_(task_uuids)).all())
        
    return TaskInstance.query.filter(TaskInstance.uuid.in_(task_uuids)).all()


def task_thread(app, user_id, task_uuid):
    with app.app_context():      
        planner = TaskPlanner(User.query.get(user_id))
        asyncio.run(planner.execute_user_task(task_uuid))
            

from flask import current_app
from flask_login import current_user
from app.main.planner import TaskPlanner
from app.investigator.investigator import Investigator
from app.utils.db_utils import generate_task, generate_investigator_run
from app.models import Task, User, InvestigatorRun
import threading
import time
import asyncio


def execute_task(args):
    """
    Generate tasks from queries and execute them.
    :param queries: a single query or a list of queries
    :return: A list of task_objects or task_ids corresponding to the queries.
    """

    task_uuid = generate_task(args)

    # TODO: allow user to cancel task
    # currently each user query starts  a new thred (why?) and it's impossible to call tasks that are already running
    t = threading.Thread(
        target=task_thread, args=[current_app._get_current_object(), current_user.id, task_uuid],
    )
    t.setDaemon(False)
    t.start()

    # Wait until the thread has started the tasks before responding to the user
    while Task.query.filter(Task.uuid == task_uuid, Task.task_status == "created").count() > 0:
        time.sleep(0.1)

    current_app.logger.debug(Task.query.filter(Task.uuid == task_uuid).one_or_none())

    return Task.query.filter(Task.uuid == task_uuid).one_or_none()


def task_thread(app, user_id, task_uuid):
    with app.app_context():
        planner = TaskPlanner(User.query.get(user_id))
        asyncio.run(planner.execute_user_task(task_uuid))


def investigator_run(args):
    """
    Currently works exactly the same as the previous function (for the single task).
    Starts a new thread and initializes an investigator run within this thread
    """

    run_uuid = generate_investigator_run(args)

    t = threading.Thread(
        target=run_thread, args=[current_app._get_current_object(), current_user.id, run_uuid, args],
    )
    t.setDaemon(False)
    t.start()

    while (
        InvestigatorRun.query.filter(
            InvestigatorRun.uuid == run_uuid, InvestigatorRun.run_status == "created"
        ).count()
        > 0
    ):
        time.sleep(0.1)

    return InvestigatorRun.query.filter(InvestigatorRun.uuid == run_uuid).one_or_none()


def run_thread(app, user_id, run_uuid, user_args):
    with app.app_context():
        planner = TaskPlanner(User.query.get(user_id))
        investigator = Investigator(run_uuid, planner)
        asyncio.run(investigator.initialize_run(user_args))
        asyncio.run(investigator.act())

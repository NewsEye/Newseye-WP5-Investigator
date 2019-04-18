from flask import current_app
from flask_login import current_user
from app import db
from app.search.search_utils import search_database
from app.analysis.analysis_utils import async_analysis
from app.models import Result, Task, User
from sqlalchemy.exc import IntegrityError
from datetime import datetime
import threading
import time
import asyncio
import uuid


def execute_tasks(queries, return_tasks=True):
    """
    Generate tasks from queries and execute them.
    :param queries: a single query or a list of queries
    :param switch_task: If true, the current task for the user will be updated to the one generated. If multiple
    queries are run in parallel, the current task will not be updated.
    :param return_tasks: If true, the task object (or a list of task objects) is returned to the user in json format.
    If false, only the task_id (or a list of task_ids) is returned
    :return: A list of task_objects or task_ids corresponding to the queries.
    """
    task_uuids = generate_tasks(queries)
    t = threading.Thread(target=execute_task_thread,
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


def execute_task_thread(app, user_id, task_uuids):
    with app.app_context():
        loop = asyncio.new_event_loop()
        loop.run_until_complete(
            execute_async_tasks(user=User.query.get(user_id), task_uuids=task_uuids, return_tasks=False))


async def execute_async_tasks(user, queries=None, task_uuids=None, return_tasks=True, parent_id=None):

    # check if it needs to return a single output or a list of outputs
    if (task_uuids and isinstance(task_uuids, list)) or (not task_uuids and isinstance(queries, list)):
        return_list = True
    else:
        return_list = False


    if task_uuids:
        # to call from API (uuid is given by API)
        tasks = Task.query.filter(Task.uuid.in_(task_uuids)).all()
    else:
        # task generation from within the system
        # e.g. called by other utility
        # generate task objects
        tasks = generate_tasks(queries=queries, user=user, parent_id=parent_id, return_tasks=True)
        task_uuids = [task.uuid for task in tasks]

    # Todo: delay estimates: based on old runtime history for similar tasks?
    # ToDo: Add timeouts for the results: timestamps are already stored, simply rerun the query if the timestamp
    #  is too old.

    for task in tasks:
        task.task_started = datetime.utcnow()
        # to update data obtained in previous searches
        if task.task_result and not task.task_parameters.get('force_refresh'):
            task.task_status = 'finished'
            task.task_finished = datetime.utcnow()
        else:
            task.task_status = 'running'
        # Remove the 'force_refresh parameter, if it is set. (We don't really want to store that, especially to the
        # result object.
        task.task_parameters = {key: value for key, value in task.task_parameters.items() if key != 'force_refresh'}

    db.session.commit()

    # sorting tasks into searches and analyses
    # then calling from the API this will be only one type
    # calling from within that may be both

    searches_to_run = [task for task in tasks if task.task_type == 'search' and task.task_status == 'running']
    analysis_to_run = [task for task in tasks if task.task_type == 'analysis' and task.task_status == 'running']


    if searches_to_run:
        # runs searches on the external database
        search_results = await search_database([task.task_parameters for task in searches_to_run])
        # stores results in the internal database
        store_results(searches_to_run, search_results)

    if analysis_to_run:
        # waiting for tasks to be done
        # calls main processing function
        analysis_results = await async_analysis(analysis_to_run)
        # store in the database
        store_results(analysis_to_run, analysis_results)

    # now the database is updated, just fetch them by their uuids
    if return_tasks:
        # return objects
        result = Task.query.filter(Task.uuid.in_(task_uuids)).all()
    else:
        # or just id
        result = task_uuids

    if return_list:
        return result
    else:
        return result[0]


def generate_tasks(queries, user=current_user, parent_id=None, return_tasks=False):

    # turns queries into Task objects
    # stores them in the datbase
    # returns task objects or task ids


    if not isinstance(queries, list):
        queries = [queries]

    if not queries:
        return []

    tasks = []

    for query in queries:
        if not isinstance(query, tuple):
            raise ValueError('query should be of the type tuple')
        task = Task(task_type=query[0], task_parameters=query[1], hist_parent_id=parent_id, user_id=user.id, task_status='created')
        tasks.append(task)

    while True:
        try:
            db.session.add_all(tasks)
            db.session.commit()
            break
        except IntegrityError as e:
            current_app.logger.error("Got a UUID collision? Trying with different UUIDs. Exception: {}".format(e))
            db.session.rollback()
            for task in tasks:
                task.uuid = uuid.uuid4()

    if return_tasks:
        return tasks
    else:
        return [task.uuid for task in tasks]


def store_results(tasks, task_results):
    # Store the new results to the database after everything has been finished
    # Todo: Should we offer the option to store results as soon as they are ready? Or do that by default?
    # Speedier results vs. more sql calls. If different tasks in the same query take wildly different amounts of
    # time, it would make sense to store the finished ones immediately instead of waiting for the last one, but I
    # doubt this would be the case here.

    for task, result in zip(tasks, task_results):
        task.task_finished = datetime.utcnow()
        if isinstance(result, ValueError):
            current_app.logger.error("ValueError: {}".format(result))
            task.task_status = 'failed: {}'.format(result)
        elif isinstance(result, Exception):
            current_app.logger.error("Unexpected exception: {}".format(result))
            task.task_status = 'failed: Unexpected exception: {}'.format(result)
        else:
            task.task_status = 'finished'
            res = Result.query.filter_by(task_type=task.task_type, task_parameters=task.task_parameters).one_or_none()
            if not res:
                db.session.commit()
                try:
                    res = Result(task_type=task.task_type, task_parameters=task.task_parameters)
                    db.session.add(res)
                    db.session.commit()
                # If another thread created the query in the meanwhile, this should recover from that, and simply overwrite the result with the newest one.
                # If the filter still returns None after IntegrityError, we log the event, ignore the result and continue
                except IntegrityError:
                    db.session.rollback()
                    res = Result.query.filter_by(task_type=task.task_type,
                                               task_parameters=task.task_parameters).one_or_none()
                    if not res:
                        current_app.logger.error("Unable to create or retrieve Result for {}. Store results failed!".format(task))
                        continue
            res.result = result
            res.last_updated = datetime.utcnow()
    current_app.logger.info("Storing results into database")
    db.session.commit()

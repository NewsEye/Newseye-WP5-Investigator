from flask import current_app
from sqlalchemy.exc import IntegrityError
from flask_login import current_user
from app import db
from app.models import Task, Result
import uuid
from datetime import datetime


def generate_tasks(queries, user=current_user, parent_id=None, return_tasks=False):
    # turns queries into Task objects
    # stores them in the database
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

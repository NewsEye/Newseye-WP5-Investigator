from flask import current_app
from flask_login import current_user
from app import db
from app.main.search_tools import search_database
from app.main.analysis_tools import AnalysisTools
from app.models import Query, Task, User
from sqlalchemy.exc import IntegrityError
from datetime import datetime
import threading
import time
import asyncio
import uuid


class SystemCore(object):
    def __init__(self):
        self._analysis = AnalysisTools(self)

    def get_tasks_by_task_id(self, task_ids):
        return Task.query.filter(Task.uuid.in_(task_ids))

    def get_history(self, make_tree=True):
        tasks = Task.query.filter_by(user_id=current_user.id)
        user_history = dict(zip([task.uuid for task in tasks], [task.dict() for task in tasks]))
        if not make_tree:
            return user_history
        tree = {'root': []}
        if not user_history:
            return tree
        for task in user_history.values():
            parent = task['hist_parent_id']
            if parent:
                if 'children' not in user_history[parent].keys():
                    user_history[parent]['children'] = []
                user_history[parent]['children'].append(task)
            else:
                tree['root'].append(task)
        return tree

    def run_query_task(self, queries, switch_task=False, return_tasks=True):
        """
        Generate tasks from queries and execute them.
        :param queries: a single query or a list of queries
        :param switch_task: If true, the current task for the user will be updated to the one generated. If multiple
        queries are run in parallel, the current task will not be updated.
        :param return_tasks: If true, the task object (or a list of task objects) is returned to the user in json format.
        If false, only the task_id (or a list of task_ids) is returned
        :return: A list of task_objects or task_ids corresponding to the queries.
        """
        task_uuids = self.generate_tasks(queries)
        t = threading.Thread(target=self.execute_task_thread, args=[current_app._get_current_object(), current_user.id, task_uuids])
        t.setDaemon(False)
        t.start()

        # Wait until the thread has started the tasks before responding to the user
        i = 0
        while Task.query.filter(Task.uuid.in_(task_uuids), Task.task_status == 'created').count() > 0:
            time.sleep(1)

        if switch_task:
            current_user.current_task = task_uuids[0]
            db.session.commit()
        if return_tasks:
            return Task.query.filter(Task.uuid.in_(task_uuids)).all()
        else:
            return task_uuids

    def execute_task_thread(self, app, user_id, task_uuids):

        with app.app_context():
            loop = asyncio.new_event_loop()
            loop.run_until_complete(self.execute_async_tasks(user=User.query.get(user_id), task_uuids=task_uuids, return_tasks=False))

    async def execute_async_tasks(self, user, queries=None, task_uuids=None, return_tasks=True, parent_id=None):

        if (task_uuids and isinstance(task_uuids, list)) or (not task_uuids and isinstance(queries, list)):
            return_list = True
        else:
            return_list = False

        if task_uuids:
            tasks = Task.query.filter(Task.uuid.in_(task_uuids)).all()
        else:
            tasks = self.generate_tasks(queries=queries, user=user, parent_id=parent_id, return_tasks=True)
            task_uuids = [task.uuid for task in tasks]

        # Todo: delay estimates: based on old runtime history for similar tasks?
        # ToDo: Add timeouts for the results: timestamps are already stored, simply rerun the query if the timestamp is too old.
        # TODO: Figure out a sensible usage for the task_history timestamps
        # Todo: Also rerun the tasks, if the results have been deleted from the database

        new_tasks = [task for task in tasks if task.task_status == 'created']

        if new_tasks:
            for task in new_tasks:
                if task.task_result:
                    task.task_status = 'finished'
                else:
                    task.task_status = 'running'
                task.last_updated = datetime.utcnow()
                task.last_accessed = datetime.utcnow()
            db.session.commit()

        searches_to_run = [task for task in new_tasks if task.query_type == 'search' and task.task_status == 'running']
        analysis_to_run = [task for task in new_tasks if task.query_type == 'analysis' and task.task_status == 'running']

        if searches_to_run:
            search_results = await search_database([task.query_parameters for task in searches_to_run])
            self.store_results(searches_to_run, search_results)

        if analysis_to_run:
            analysis_results = await self._analysis.async_analysis(analysis_to_run)
            self.store_results(analysis_to_run, analysis_results)

        if return_tasks:
            result = Task.query.filter(Task.uuid.in_(task_uuids)).all()
        else:
            result = task_uuids

        if return_list:
            return result
        else:
            return result[0]

    @staticmethod
    def generate_tasks(queries, user=current_user, parent_id=None, return_tasks=False):

        # TODO: Spot and properly handle duplicate tasks when added within the same request

        if not isinstance(queries, list):
            queries = [queries]

        # If queries contains dictionaries, assume they are of type 'search' and fix the format
        if isinstance(queries[0], dict):
            queries = [('search', item) for item in queries]
        elif not isinstance(queries[0], tuple):
            raise ValueError

        # ToDo: need to check that this is a correct type. For now we'll assume that it is.
        if not parent_id:
            parent_id = user.current_task_id

        # Remove the target_id from query parameters
        target_ids = [query[1].pop('target_id', None) for query in queries]

        # Assume empty query as input for analysis with no input specified
        for query, target_id in zip(queries, target_ids):
            if query[0] == 'analysis':
                if target_id is None and query[1].get('target_search') is None:
                    query[1]['target_search'] = {'q': []}

        existing_tasks = [Task.query.filter_by(user_id=user.id, data_parent_id=target_id, hist_parent_id=parent_id, query_type=query[0], query_parameters=query[1]).one_or_none() for query, target_id in zip(queries, target_ids)]

        tasks = []
        new_tasks = []

        for idx, query in enumerate(queries):
            task = existing_tasks[idx]
            if task is None:
                task = Task(query_type=query[0], query_parameters=query[1], data_parent_id=target_ids[idx], hist_parent_id=parent_id, user_id=user.id, task_status='created')
                new_tasks.append(task)
            tasks.append(task)

        if new_tasks:
            while True:
                try:
                    db.session.add_all(new_tasks)
                    db.session.commit()
                    break
                except IntegrityError:
                    db.session.rollback()
                    for task in new_tasks:
                        task.uuid = uuid.uuid4()
                    pass

        if return_tasks:
            return tasks
        return [task.uuid for task in tasks]

    @staticmethod
    def store_results(tasks, task_results):
        # Store the new results to the database after everything has been finished
        # Todo: Should we offer the option to store results as soon as they are ready? Or do that by default?
        # Speedier results vs. more sql calls. If different tasks in the same query take wildly different amounts of
        # time, it would make sense to store the finished ones immediately instead of waiting for the last one, but I
        # doubt this would be the case here.

        for task, result in zip(tasks, task_results):
            task.task_status = 'finished'
            # TODO: What timestamps need to be updated?
            q = Query.query.filter_by(query_type=task.query_type, query_parameters=task.query_parameters).one_or_none()
            if not q:
                q = Query(query_type=task.query_type, query_parameters=task.query_parameters)
                try:
                    db.session.add(q)
                # If another thread created the query in the meanwhile, this should recover from that, and simply overwrite the result with the newest one.
                # If the filter still returns None after IntegrityError, we log the event, ignore the result and continue
                except IntegrityError:
                    q = Query.query.filter_by(query_type=task.query_type,
                                              query_parameters=task.query_parameters).one_or_none()
                    if not q:
                        current_app.logger.error("Unable to create or retrieve Query for {}. Store results failed!".format(task))
                        continue
            q.query_result = result
            q.last_accessed = datetime.utcnow()
            q.last_updated = datetime.utcnow()
        current_app.logger.info("Storing results into database")
        db.session.commit()

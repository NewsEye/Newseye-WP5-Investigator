from flask import current_app
from flask_login import current_user
from app import db
from app.assistant.database_access import *
from app.assistant.analysis import *
from app.models import Query, Task, User
from sqlalchemy.exc import IntegrityError
from datetime import datetime
import threading
import time
import asyncio


class SystemCore(object):
    def __init__(self):
        self._blacklight_api = BlacklightAPI()
        self._PSQL_api = PSQLAPI()
        self._analysis = AnalysisTools(self)

    def get_current_task(self):
        return current_user.current_task

    def get_tasks_by_task_id(self, task_ids):
        return Task.query.filter(Task.uuid.in_(task_ids))

    def get_history(self, username, make_tree=True):
        history = self._PSQL_api.get_user_history(username)
        if not make_tree:
            return history
        tree = {'root': []}
        if not history:
            return tree
        for task in history.values():
            parent = task['parent_id']
            if parent:
                if 'children' not in history[parent].keys():
                    history[parent]['children'] = []
                history[parent]['children'].append(task)
            else:
                tree['root'].append(task)
        return tree

    def run_query_task(self, username, queries, switch_task=False, return_tasks=True, store_results=True):
        """
        Generate tasks from queries and execute them.
        :param username: the user who is requesting the queries
        :param queries: a single query or a list of queries
        :param switch_task: If true, the current task for the user will be updated to the one generated. If multiple
        queries are run in parallel, the current task will not be updated.
        :param return_tasks: If true, the task object (or a list of task objects) is returned to the user in json format.
        If false, only the task_id (or a list of task_ids) is returned
        :return: A list of task_objects or task_ids corresponding to the queries.
        """
        task_uuids = self.generate_tasks(queries)

        t = threading.Thread(target=self.execute_task_thread, args=[current_app._get_current_object(), task_uuids, store_results])
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

    def execute_task_thread(self, app, task_uuids, store_results):

        with app.app_context():
            loop = asyncio.new_event_loop()
            loop.run_until_complete(self.execute_async_tasks(task_uuids=task_uuids, return_tasks=False))

    async def execute_async_tasks(self, queries=None, task_uuids=None, return_tasks=True, parent_id=None):
        if (task_uuids and isinstance(task_uuids, list)) or (not task_uuids and isinstance(queries, list)):
            return_list = True
        else:
            return_list = False

        if not task_uuids:
            task_uuids = self.generate_tasks(queries, parent_id=parent_id)
        tasks = Task.query.filter(Task.uuid.in_(task_uuids)).all()

        # Todo: delay estimates: based on old runtime history for similar tasks?
        # ToDo: Add timeouts for the results: timestamps are already stored, simply rerun the query if the timestamp is too old.
        # TODO: Figure out a sensible usage for the task_history timestamps

        tasks_to_run = [task for task in tasks if task.task_status == 'created']
        searches_to_run = [task for task in tasks_to_run if task.query_type == 'search']
        analysis_to_run = [task for task in tasks_to_run if task.query_type == 'analysis']

        if tasks_to_run:
            for task in tasks_to_run:
                task.task_status = 'running'
                task.last_updated = datetime.utcnow()
                task.last_accessed = datetime.utcnow()
            db.session.commit()

        if analysis_to_run:
            # Fetch the data required by the analysis tasks
            extra_queries = [task.query_parameters.get('target_query') for task in analysis_to_run]
            data_parent_ids = await self.execute_async_tasks(queries=extra_queries, return_tasks=False)

            for task, source_id in zip(analysis_to_run, data_parent_ids):
                task.data_parent_id = source_id
                # Set the parent of any analysis task to be the corresponding query task
                # TODO: Check this
                task.hist_parent_id = source_id

        if searches_to_run:
            search_results = await self._blacklight_api.async_query([task.query_parameters for task in searches_to_run])
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

    def generate_tasks(self, queries, parent_id=None):

        # TODO: Option for choosing whether to return tasks or task_uuids
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
            parent_id = current_user.current_task_id

        for query in queries:
            if query[0] == 'analysis':
                target_query = query[1].get('target_query')
                if not target_query:
                    target_id = query[1].get('target_id')
                    if target_id:
                        target_query = Task.query.get(target_id).query_parameters
                        query[1]['target_query'] = target_query

                    # If neither is specified, use an empty query as the target_query
                    else:
                        query[1]['target_query'] = {'q': []}

                # Remove the target_id parameter
                query[1].pop('target_id', None)

        existing_tasks = [Task.query.filter_by(user_id=current_user.id, hist_parent_id=parent_id, query_type=query[0], query_parameters=query[1]).first() for query in queries]

        tasks = []
        new_tasks = []

        for idx, query in enumerate(queries):
            task = existing_tasks[idx]
            if task is None:
                task = Task(query_type=query[0], query_parameters=query[1], hist_parent_id=parent_id, user_id=current_user.id, task_status='created')
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
                    pass

        return [task.uuid for task in tasks]

    def store_results(self, tasks, task_results):
        # Store the new results to the database after everything has been finished
        # Todo: Should we offer the option to store results as soon as they are ready? Or do that by default?
        # Speedier results vs. more sql calls. If different tasks in the same query take wildly different amounts of
        # time, it would make sense to store the finished ones immediately instead of waiting for the last one, but I
        # doubt this would be the case here.

        # Do not store the target_id even if one has been temporarily set
        # TODO: Check whether this is still needed
        for task in tasks:
            task.query_parameters.pop('target_id', None)

        for task, result in zip(tasks, task_results):
            task.task_status = 'finished'
            # TODO: What timestamps need to be updated?
            q = Query.query.filter_by(query_type=task.query_type, query_parameters=task.query_parameters).first()
            if not q:
                q = Query(query_type=task.query_type, query_parameters=task.query_parameters)
                db.session.add(q)
            q.query_result = result
            q.last_accessed = datetime.utcnow()
            q.last_updated = datetime.utcnow()

        db.session.commit()
        print("Storing results into database")

    def get_results(self, task_ids):
        if not isinstance(task_ids, list):
            task_ids = [task_ids]
        results = Task.query.filter(Task.uuid.in_(task_ids)).all()
        if results:
            return results
        else:
            raise TypeError

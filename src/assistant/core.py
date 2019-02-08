from assistant.task import Task
from assistant.database_access import *
from assistant.analysis import *
import threading
import time
import assistant.config as conf
import datetime as dt
import asyncio


class SystemCore(object):
    def __init__(self):
        self._blacklight_api = BlacklightAPI()
        self._PSQL_api = PSQLAPI()
        self._analysis = AnalysisTools(self, self._PSQL_api)

    def add_user(self, username, new_username):
        # Todo: add user types + limit user creation etc. to admins
        # Only user 'admin' can add new users at the moment
        if username == 'admin':
            self._PSQL_api.add_user(new_username)
        else:
            raise ValueError()

    def login_user(self, username):
        # Raises IndexError if username doesn't exist
        last_login = self._PSQL_api.get_last_login(username)
        self._PSQL_api.set_last_login(username, dt.datetime.now())
        return last_login

    def get_task(self, username):
        return self._PSQL_api.get_current_task(username)

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
                if task['task_type'] == 'query':
                    if 'children' not in history[parent].keys():
                        history[parent]['children'] = []
                    history[parent]['children'].append(task)
                else:
                    if 'analysis' not in history[parent].keys():
                        history[parent]['analysis'] = []
                    history[parent]['analysis'].append(task)
            else:
                tree['root'].append(task)
        return tree

    def run_query_task(self, username, queries, switch_task=False, return_task=True, store_results=True):
        """
        Generate tasks from queries and execute them.
        :param username: the user who is requesting the queries
        :param queries: a single query or a list of queries
        :param switch_task: If true, the current task for the user will be updated to the one generated. If multiple
        queries are run in parallel, the current task will not be updated.
        :param return_task: If true, the task object (or a list of task objects) is returned to the user in json format.
        If false, only the task_id (or a list of task_ids) is returned
        :return: A list of task_objects or task_ids corresponding to the queries.
        """
        tasks = self.generate_tasks(username, queries)

        t = threading.Thread(target=self.execute_task_thread, args=[username, tasks, store_results])
        t.setDaemon(False)
        t.start()

        # Wait until the thread has started the tasks until responding to the user
        i = 0
        while i < len(tasks):
            if tasks[i]['task_status'] == 'created':
                time.sleep(.5)
            else:
                i += 1

        if switch_task:
            self._PSQL_api.set_current_task(username, tasks[0]['task_id'])
        if return_task:
            return tasks
        else:
            return [item['task_id'] for item in tasks]

    def execute_task_thread(self, username, tasks, store_results):

        loop = asyncio.new_event_loop()
        loop.run_until_complete(self.execute_async_tasks(username=username, tasks=tasks, store_results=store_results, return_tasks=False))

    async def execute_async_tasks(self, username, queries=None, tasks=None, store_results=True, return_tasks=True):
        if not tasks:
            tasks = self.generate_tasks(username, queries)

        # Todo: delay estimates: based on old runtime history for similar tasks?
        # ToDo: Add timeouts for the results: timestamps are already stored, simply rerun the query if the timestamp is too old.

        tasks_to_run = [task for task in tasks if task['task_status'] == 'created']
        queries_to_run = [task for task in tasks_to_run if task['task_type'] == 'query']
        analysis_to_run = [task for task in tasks_to_run if task['task_type'] == 'analysis']

        if not tasks_to_run:
            print("All results already found in the local database")
        else:
            for task in tasks_to_run:
                task['task_status'] = 'running'
            self._PSQL_api.update_status(username, tasks_to_run)

            # Todo: Improvement to run all the extra queries in parallel
            # Todo: use generate_tasks() to generate the extra query tasks, and add them to tasks_to_run and queries_to_run
            # ToDo: Then remember to make sure that the query results are stored to the database before running the analysis
            # ToDo: tasks and adjust the code to use only queries instead of target_ids: just send the query results to the
            # ToDo: analysis tasks
            for task in analysis_to_run:
                query = task['task_parameters'].get('target_query', None)
                # This is stupid, need to avoid the whole target_id thing completely
                if query:
                    result = await self.execute_async_tasks(username, query, return_tasks=False)
                    task['task_parameters']['target_id'] = str(result[0])

            # Note: now we are running first all the queries and then all the analysis, which is suboptimal, but
            # I would expect a single tasklist to include only tasks of one type. If this is not the case, then it
            # might be useful to add functionality to run everything in parallel.
            if queries_to_run:
                query_results = await self._blacklight_api.async_query([task['task_parameters'] for task in queries_to_run])
                for task, result in zip(queries_to_run, query_results):
                    task['task_result'] = result
                    task['task_status'] = 'finished'
                if store_results:
                    self.store_results(username, queries_to_run)

            if analysis_to_run:
                analysis_results = await self._analysis.async_query(username, [task['task_parameters'] for task in analysis_to_run])
                for task, result in zip(analysis_to_run, analysis_results):
                    task['task_result'] = result
                    task['task_status'] = 'finished'
                if store_results:
                    self.store_results(username, analysis_to_run)

        if return_tasks:
            return tasks
        else:
            return [item['task_id'] for item in tasks]

    def generate_tasks(self, username, queries):

        # TODO: Spot and properly handle duplicate tasks when added within the same request

        if type(queries) is not list:
            queries = [queries]

        # If queries contains dictionaries, assume they are of type 'query' and fix the format
        if type(queries[0]) is dict:
            queries = [('query', item) for item in queries]
        elif type(queries[0]) is not tuple:
            raise ValueError

        # ToDo: need to check that this is a correct type. For now we'll assume that it is.
        current_task_id = self._PSQL_api.get_current_task_id(username)

        # Todo: Use _only_ target_queries. If target id is specified, fetch the corresponding queries instead
        for query in queries:
            if query[0] == 'analysis':
                target_query = query[1].get('target_query', None)
                if not target_query:
                    target_id = query[1].get('target_id', None)
                    if target_id:
                        target_query = self._PSQL_api.get_results_by_task_id(target_id)[target_id]['task_parameters']
                        query[1]['target_query'] = target_query

                    # If neither is specified, use an empty query as the target_query
                    else:
                        query[1]['target_query'] = {'q': []}

                # Remove the target_id parameters
                query[1].pop('target_id', None)

        # (task_id, query, task_status, parent_id)
        old_tasks = self._PSQL_api.get_user_tasks_by_query(username, queries)

        if old_tasks:
            old_tasks = list(zip(*old_tasks))
        else:
            old_tasks = [[]] * 4

        old_results = self._PSQL_api.get_results_by_query(queries)

        if old_results:
            old_results = list(zip(*old_results))
        else:
            old_results = [[]] * 2

        tasks = []
        new_tasks = []
        new_results = []

        for query in queries:
            task = Task(task_type=query[0], task_parameters=query[1], parent_id=current_task_id, username=username)
            tasks.append(task)

            try:
                i = old_tasks[1].index(query)
                task['task_id'] = old_tasks[0][i]
                task['parent_id'] = old_tasks[3][i]
                task['task_status'] = old_tasks[2][i]
            except ValueError:
                new_tasks.append(task)
            try:
                i = old_results[0].index(query)
                task['task_result'] = old_results[1][i]
            except ValueError:
                new_results.append(task)

        if new_results:
            self._PSQL_api.add_results(new_results)

        if new_tasks:
            new_task_ids = self._PSQL_api.add_tasks(new_tasks)

            # Add the correct ids to tasks
            for task, id in zip(new_tasks, new_task_ids):
                task['task_id'] = id

        return tasks

    def store_results(self, username, tasks):
        # Store the new results to the database after everything has been finished
        # Todo: Should we offer the option to store results as soon as they are ready? Or do that by default?
        # Speedier results vs. more sql calls. If different tasks in the same query take wildly different amounts of
        # time, it would make sense to store the finished ones immediately instead of waiting for the last one, but I
        # doubt this would be the case here.

        # If the target_query is specified in the task, do not store the target_id
        for task in tasks:
            if task['task_parameters'].get('target_query', None):
                task['task_parameters'].pop('target_id')
        print("Storing results into database")
        self._PSQL_api.update_results(username, tasks)

    def get_results(self, task_ids):
        if type(task_ids) is not list:
            task_ids = [task_ids]
        results = self._PSQL_api.get_results_by_task_id(task_ids)
        if results:
            return results
        else:
            raise TypeError

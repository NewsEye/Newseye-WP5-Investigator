from assistant.task import Task
from assistant.database_access import *
from operator import itemgetter
import assistant.analysis as aa
import threading
import time
import uuid
import assistant.config as conf
import pandas as pd
import datetime as dt


class SystemCore(object):
    def __init__(self):
        # self._current_users = {}
        self._blacklight_api = BlacklightAPI()
        self._PSQL_api = PSQLAPI()

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

    def get_unique_id(self, index):
        new_id = uuid.uuid4()
        while new_id in index.keys():
            new_id = uuid.uuid4()
        return new_id

    def get_task(self, username):
        return self._PSQL_api.get_current_task(username)

    # def set_query(self, username, query):
    #     """
    #     Set the currently active task for the given user.
    #     :param username: The user affected by the method.
    #     :param query: A query that is to be set as the current task. If a task corresponding to the query doesn't exist,
    #     one will be created, but not executed. If multiple corresponding tasks exist, one of them will be selected.
    #     :return: The task_id for the Task corresponding to query
    #     """
    #
    #     existing_results = self._PSQL_api.find_tasks(username, [('query', query)])
    #     if existing_results:
    #         task_id = list(zip(*existing_results))[0][0]
    #     else:
    #         # If a task corresponding to the query doesn't exist, generate one adding it to the database.
    #         task_id = self._PSQL_api.add_query(username, query)
    #     self._PSQL_api.set_current_task(username, task_id)
    #     return task_id

    # def find_query(self, username, query):
    #     for key, state in self._current_users[username]['history'].items():
    #         if state.query == query:
    #             return state
    #     return None

    # def get_state(self, username, state_id=None):
    #     if not state_id:
    #         return self._current_users.get(username)['state']
    #     else:
    #         return self._current_users.get(username)['history'][state_id]
    #
    # def set_state(self, username, state):
    #     self._current_users.get(username)['state'] = state

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

    # def clear_query(self, username):
    #     self.set_query(username, {
    #         'q': '',
    #     })

    @staticmethod
    def run(task=None, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        if task is None:
            return loop.run_forever()
        else:
            return loop.run_until_complete(task)

    def run_query_task(self, username, queries, switch_task=False, threaded=True, return_task=True):
        """
        Generate tasks from queries and execute them.
        :param username: the user who is requesting the queries
        :param queries: a single query or a list of queries
        :param switch_task: If true, the current task for the user will be updated to the one generated. If multiple
        queries are run in parallel, the current task will not be updated.
        :param threaded: If true, the queries will be run in a separate thread. If set to false, no response will be
        sent to the user until the queries are finished, regardless of how long they take. When set to true, if the
        queries take a long time to complete, the user will get a reply showing that the queries are still running, and
        they can retrieve the results at a later time.
        :param return_task: If true, the task object (or a list of task objects) is returned to the user in json format.
        If false, only the task_id (or a list of task_ids) is returned
        :return: A list of task_objects or task_ids corresponding to the queries.
        """
        tasks = self.generate_tasks(username, queries)

        # If the tasks are run as threaded
        if threaded:
            t = threading.Thread(target=self.query_thread, args=[tasks])
            t.setDaemon(False)
            t.start()
            time.sleep(3)
        else:
            self.query_thread(tasks)

        if switch_task:
            self._PSQL_api.set_current_task(username, tasks[0]['task_id'])
        if return_task:
            return tasks
        else:
            return [item['task_id'] for item in tasks]

    def generate_tasks(self, username, queries):

        if type(queries) is not list:
            queries = [queries]

        # If queries contains dictionaries, assume they are of type 'query' and fix the format
        if type(queries[0]) is dict:
            queries = [('query', item) for item in queries]
        elif type(queries[0]) is not tuple:
            raise ValueError

        # ToDo: need to check that this is a correct type. For now we'll assume that it is.
        current_task_id = self._PSQL_api.get_current_task_id(username)

        # Todo: check whether this works with analysis_results as well now
        # ToDo: change this so we get the query tuples instead of just the task_parameters part??
        existing_results = self._PSQL_api.find_existing_results(queries)

        if existing_results:
            query_types, old_queries, old_results = list(zip(*existing_results))
        else:
            query_types, old_queries, old_results = [[]] * 3

        tasks = []
        new_queries = []

        for query in queries:
            try:
                i = old_queries.index(query[1])
                task = Task(task_type=query[0], task_parameters=query[1], parent_id=current_task_id,
                            username=username, task_result=old_results[i])
            except ValueError:
                task = Task(task_type=query[0], task_parameters=query[1], parent_id=current_task_id, username=username,
                            task_result=conf.UNFINISHED_TASK_RESULT)
                new_queries.append(task)
            tasks.append(task)

        if new_queries:
            self._PSQL_api.add_queries(new_queries)
        task_ids = self._PSQL_api.add_tasks(tasks)

        # Add the correct ids to tasks
        for i, task in enumerate(tasks):
            task['task_id'] = task_ids[i]

        return tasks

    def query_thread(self, tasks):

        # Todo: delay estimates
        # ToDo: Add timeouts for the results: timestamps are already stored, simply rerun the query if the timestamp is too old.
        # Todo: Better to pass the whole results list to the threaded part and do the selection of the queries to be re-executed there,
        # ToDO: based on whether the result exists and how old it is?

        # Todo: Differentiate between currently running tasks and tasks that haven't been started yet
        tasks_to_run = [task for task in tasks if task['task_result'] == conf.UNFINISHED_TASK_RESULT]

        queries_to_run = [task['task_parameters'] for task in tasks_to_run]

        if len(queries_to_run) == 0:
            print("All query results already found in the local database")
            return

        asyncio.set_event_loop(asyncio.new_event_loop())
        results = self.run(self._blacklight_api.async_query(queries_to_run))

        for i, task in enumerate(tasks_to_run):
            task['task_result'] = results[i]

        # Store the new results to the database
        print("Got the query results: storing into database")
        self._PSQL_api.update_results(tasks_to_run)

    def run_analysis(self, username, args):
        tool_name = args.get('tool')
        req_args = aa.TOOL_ARGS[tool_name]
        if len(args) != len(req_args) + 1:
            raise TypeError("Invalid number of arguments for the chosen tool")
        current_query = self._PSQL_api.get_current_task(username)
        tool_args = [self._PSQL_api, current_query]
        for arg_name in req_args:
            tool_args.append(args.get(arg_name))
        analysis_result = aa.TOOL_LIST[tool_name](*tool_args)
        return analysis_result

    def topic_analysis(self, username):
        current_query = self._PSQL_api.get_current_task(username)
        query_results = current_query['result']
        if query_results is None or query_results == conf.UNFINISHED_TASK_RESULT:
            raise TypeError("No query results available for analysis")
        for item in query_results['included']:
            if item['id'] == conf.PUB_YEAR_FACET and item['type'] == 'facet':
                pub_dates = [(date['attributes']['value'], date['attributes']['hits']) for date in item['attributes']['items']]
                break
        else:
            raise TypeError("Query results don't contain required facet {}".format(conf.PUB_YEAR_FACET))
        pub_dates.sort()
        last_query = current_query['task_parameters']
        queries = [{'f[{}][]'.format(conf.PUB_YEAR_FACET): item[0]} for item in pub_dates]
        for query in queries:
            query.update(last_query)
        result_ids = self.run_query_task(username, queries, return_task=False, threaded=False)
        t_counts = []
        for id in result_ids:
            query, query_results = itemgetter('task_parameters', 'result')(self._PSQL_api.get_query_by_id(username, id))
            year = query['f[{}][]'.format(conf.PUB_YEAR_FACET)]
            total_hits = query_results['meta']['pages']['total_count']
            for item in query_results['included']:
                if item['id'] == conf.TOPIC_FACET and item['type'] == 'facet':
                    t_counts.extend([[year, topic['attributes']['value'], topic['attributes']['hits'], topic['attributes']['hits'] / total_hits] for topic in item['attributes']['items']])
                    break
            else:
                raise TypeError("Query results don't contain required facet '{}'".format(conf.TOPIC_FACET))
        df = pd.DataFrame(t_counts, columns=['year', 'topic', 'count', 'rel_count'])
        abs_counts = df.pivot(index='topic', columns='year',values='count').fillna(0)
        rel_counts = df.pivot(index='topic', columns='year',values='rel_count').fillna(0)
        analysis_results = {
            'analysis_type': 'topic_analysis',
            'analysis_result': {
                'absolute_counts': abs_counts.to_dict(orient='index'),
                'relative_counts': rel_counts.to_dict(orient='index')
            }
        }
        self._PSQL_api.add_analysis(username, current_query['task_id'], analysis_results)
        return analysis_results

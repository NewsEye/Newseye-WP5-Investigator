from assistant.query import Query
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

    def get_query(self, username):
        return self._PSQL_api.get_current_query(username)

    def set_query(self, username, query):
        """
        Set the currently active query for the given user.
        :param username:
        :param query:
        :return:
        """

        query_ids = self._PSQL_api.find_query(username, query)
        if query_ids:
            query_id = query_ids[0]
        else:
            # # Otherwise, set the query as the current query for the user
            query_id = self._PSQL_api.add_query(username, query)
        self._PSQL_api.set_current_query(username, query_id)
        return query_id

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
        queries = self._PSQL_api.get_user_queries(username)
        analysis = self._PSQL_api.get_user_analysis(username)
        if not make_tree:
            return queries
        tree = {'root': {'children': []}}
        for item in queries.values():
            parent = item['parent_id']
            if parent:
                if 'children' not in queries[parent].keys():
                    queries[parent]['children'] = []
                queries[parent]['children'].append(item)
            else:
                tree['root']['children'].append(item)
        if analysis:
            for item in analysis:
                query_id = item['query_id']
                if 'analysis' not in queries[query_id].keys():
                    queries[query_id]['analysis'] = []
                queries[query_id]['analysis'].append(item)
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

    def run_query(self, username, query, switch_query=False, threaded=True, return_dict=True):
        """
        Runs query for the user defined by username.
        Parameters:
            switch_query: if set to True updates the current_query value for the user. If multiple queries are run in parallel, the current_query will be set to the first query
            in the list.
            threaded: If True, the query is run in a separate thread. Otherwise the query is run in the main thread.
            return_dict: If True, the query returns a dict (or dicts) containing the query and its result.
                         Otherwise only the query_id (or query_ids) is returned.
            Note: return_dict=False in combination with store_results=False results is useless, since the results are not
                  stored and therefore cannot be retrieved afterwards using the query_id.
        """
        query_is_a_list = type(query) is list
        if query_is_a_list:
            querylist = query
        else:
            querylist = [query]

        current_query_id = self._PSQL_api.get_current_query_id(username)

        # Todo: delay estimates
        default_result = {
            'message': 'Still running',
        }

        # ToDo: Add timeouts for the results: timestamps are already stored, simply rerun the query if the timestamp is too old.
        # Todo: Better to pass the whole results list to the threaded part and do the selection of the queries to be re-executed there,
        # ToDO: based on whether the result exists and how old it is?
        existing_results = self._PSQL_api.find_queries(username, querylist)

        if existing_results:
            q_id, q, p_id, res = list(zip(*existing_results))
        else:
            q_id, q, p_id, res = [[]] * 4

        new_queries = []
        results = []
        for query in querylist:
            try:
                i = q.index(query)
                query_object = Query(query_id=q_id[i], query=query, parent_id=p_id[i], username=username, result=res[i])
            except ValueError:
                query_object = Query(query=query, parent_id=current_query_id, username=username, result=default_result)
                new_queries.append(query_object)
            results.append(query_object)

        new_query_ids = self._PSQL_api.add_queries(new_queries)

        # Add the correct ids to new_queries
        for i, query in enumerate(new_queries):
            query['query_id'] = new_query_ids[i]

        # Run the queries
        if len(new_queries) > 0:

            # If the queries are run as threaded
            if threaded:
                t = threading.Thread(target=self.query_thread, args=[new_queries])
                t.setDaemon(False)
                t.start()
                time.sleep(4)
            else:
                self.query_thread(new_queries)

        if switch_query:
            self._PSQL_api.set_current_query(username, results[0]['query_id'])
        if return_dict:
            return results
        else:
            return [item['query_id'] for item in results]

    def query_thread(self, queries):
        # Todo: add separate query_started and query_finished timestamps
        if type(queries) is list:
            querylist = queries
        else:
            querylist = [queries]
        # Todo: Move the delay stuff into database_access
        delay = [query.query.pop('test_delay', [0])[0] for query in querylist]
        if delay[0]:
            time.sleep(int(delay[0]))
        queries = [query.query for query in querylist]
        asyncio.set_event_loop(asyncio.new_event_loop())
        results = self.run(self._blacklight_api.async_query(queries))
        for i, query in enumerate(querylist):
            query.result = results[i]

        # Store the results to database
        print("Got the query results: storing into database")
        self._PSQL_api.update_results(querylist)

    # ToDo: Combine the analysis and query tables as one table containing both types??
    # Then the current_query field would point to the latest query or analysis run. Also, splitting a result into subqueries
    # e.g. based on time could be seen as a query/analysis of it's own, with a corresponding id (and the result field
    # containing the ids of the subqueries), and the analysis tools could be simply passed the id of the splitting result

    def run_analysis(self, username, args):
        tool_name = args.get('tool')
        req_args = aa.TOOL_ARGS[tool_name]
        if len(args) != len(req_args) + 1:
            raise TypeError("Invalid number of arguments for the chosen tool")
        current_query = self._PSQL_api.get_current_query(username)
        tool_args = [self._PSQL_api, current_query]
        for arg_name in req_args:
            tool_args.append(args.get(arg_name))
        analysis_result = aa.TOOL_LIST[tool_name](*tool_args)
        return analysis_result

    def topic_analysis(self, username):
        current_query = self._PSQL_api.get_current_query(username)
        query_results = current_query['result']
        if query_results is None or query_results.get('message', '') == 'Still running':
            raise TypeError("No query results available for analysis")
        for item in query_results['included']:
            if item['id'] == conf.PUB_YEAR_FACET and item['type'] == 'facet':
                pub_dates = [(date['attributes']['value'], date['attributes']['hits']) for date in item['attributes']['items']]
                break
        else:
            raise TypeError("Query results don't contain required facet {}".format(conf.PUB_YEAR_FACET))
        pub_dates.sort()
        last_query = current_query['query']
        queries = [{'f[{}][]'.format(conf.PUB_YEAR_FACET): item[0]} for item in pub_dates]
        for query in queries:
            query.update(last_query)
#        self.set_query(username, queries)
        result_ids = self.run_query(username, queries, return_dict=False, threaded=False)
        t_counts = []
        for id in result_ids:
            query, query_results = itemgetter('query', 'result')(self._PSQL_api.get_query_by_id(username, id))
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
        self._PSQL_api.add_analysis(current_query['query_id'], analysis_results)
        return analysis_results

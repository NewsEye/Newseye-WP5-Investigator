from assistant.state import State
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
        self._current_users = {}
        self._blacklight_api = BlacklightAPI()
        self._PSQL_api = PSQLAPI()

    def add_user(self, username, new_username):
        # Todo: add user types + limit user creation etc. to admins
        if username == 'jariavik':
            self._PSQL_api.add_user(new_username)
            query = {
                'q': '',
            }
            new_state = State()
            history = {
                'root': new_state,
                new_state.id: new_state
            }
            new_user = {
                'query': query,
                'state': new_state,
                'history': history
            }
            self._current_users[new_username] = new_user
        else:
            raise ValueError()

    def login_user(self, username):
        # Raises IndexError if username doesn't exist
        last_login = self._PSQL_api.get_last_login(username)
        self._PSQL_api.set_last_login(username, dt.datetime.now())
        return last_login

    def get_unique_id(self, index):
        new_id = uuid.uuid4().hex
        while new_id in index.keys():
            new_id = uuid.uuid4().hex
        return new_id

    def get_query(self, username):
        return self._current_users.get(username)['query']

    def set_query(self, username, query):
        # If the query has already been made earlier, change the state to the one corresponding to the query
        state = self.find_query(username, query)
        if state:
            self.set_state(username, state)
        # The same using PSQL
        query_ids = self._PSQL_api.find_query(username, query)
        if len(query_ids) > 0:
            query_id = query_ids[0]
        else:
            # Otherwise, set the query as the current query for the user
            self._current_users.get(username)['query'] = query
            # The same using PSQL
            query_id = self._PSQL_api.add_query(username, query)
        self._PSQL_api.set_user_query(username, query_id)

    def find_query(self, username, query):
        for key, state in self._current_users[username]['history'].items():
            if state.last_query == query:
                return state
        return None

    def get_state(self, username, state_id=None):
        if not state_id:
            return self._current_users.get(username)['state']
        else:
            return self._current_users.get(username)['history'][state_id]

    def set_state(self, username, state):
        self._current_users.get(username)['state'] = state

    def get_history(self, username, make_tree=True):
        history = self._current_users.get(username)['history']
        if not make_tree:
            return history
        tree = {}
        root = history['root'].copy()
        tree['root'] = root
        queue = []
        queue.insert(0, root)
        while len(queue) > 0:
            state = queue.pop()
            new_children = [history[id].copy() for id in state['children']]
            state['children'] = new_children
            for child in new_children:
                queue.insert(0, child)
        return tree

    def clear_query(self, username):
        self.set_query(username, {
            'q': '',
        })

    @staticmethod
    def run(task=None, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        if task is None:
            return loop.run_forever()
        else:
            return loop.run_until_complete(task)

    def run_query(self, username, query, switch_state=True, threaded=True, return_type='state', store_results=True):
        """
        Runs query for the user defined by username.
        Parameters:
            switch_state: if set to True changes the current state for the user to the state containing the results of the
            query. If multiple queries are run in parallel, the state will be set to the result state of the first query
            in the list.
            threaded: If True, the query is run in a separate thread. Otherwise the query is run in the main thread.
            return_type: 'state': the query returns the state (or states) containing the query and its result.
                         'id': only the id of the result state (or states) is returned.
            store_results: If True, the result states are stored into user history, otherwise they are lost.
            Note: return_type='id' in combination with store_results=False results is useless, since the results are not
                  stored and therefore cannot be retrieved afterwards using the id.
        """
        self.set_query(username, query)
        current_query, current_state, state_history = itemgetter('query', 'state', 'history')(self._current_users.get(username))
        if current_state.last_query and current_query == current_state.last_query:
            if return_type == 'state':
                return [current_state]
            elif return_type == 'id':
                return [current_state.id]
            else:
                raise TypeError("Invalid parameter value for return_type: {}. Use either 'state' or 'id'")

        query_is_a_list = type(current_query) is list
        if query_is_a_list:
            querylist = current_query
        else:
            querylist = [current_query]

        new_states = []
        result_ids = []

        for query in querylist:
            new_state = State(state_id=self.get_unique_id(state_history), last_query=query, parent_id=current_state.id)
            result_ids.append(new_state.id)
            new_states.append(new_state)

            # If store_results is False, the new state is not stored anywhere and is forgotten after the query results are returned
            if store_results:
                state_history[new_state.id] = new_state
                current_state.add_child(new_state.id)

            # If for some reason the query is run in the main thread, we should ignore the test_delay parameter
            if not threaded:
                new_state.last_query.pop('test_delay', None)

            # Todo: proper delay estimates
            delay = new_state.last_query.get('test_delay', [0])[0]
            estimate = 5
            if delay:
                estimate += int(delay)
            new_state.query_results = {
                'message': 'Still running',
                'time_remaining': estimate
            }
        if threaded:
            t = threading.Thread(target=self.query_thread, args=[new_states])
            t.setDaemon(False)
            t.start()
            time.sleep(2)
        else:
            self.query_thread(new_states)
        if switch_state:
            self.set_state(username, new_states[0])
        if return_type == 'state':
            return new_states
        elif return_type == 'id':
            return result_ids
        else:
            raise TypeError("Invalid parameter value for return_type: {}. Use either 'state' or 'id'")

    def query_thread(self, states):
        if type(states) is list:
            statelist = states
        else:
            statelist = [states]
        # Todo: Move the delay stuff into database_access
        delay = [state.last_query.pop('test_delay', [0])[0] for state in statelist]
        if delay[0]:
            time.sleep(int(delay[0]))
        queries = [state.last_query for state in statelist]
        asyncio.set_event_loop(asyncio.new_event_loop())
        results = self.run(self._blacklight_api.async_query(queries))
        for i, state in enumerate(statelist):
            state.query_results = results[i]

    def run_analysis(self, username, args):
        tool_name = args.get('tool')
        req_args = aa.TOOL_ARGS[tool_name]
        if len(args) != len(req_args) + 1:
            raise TypeError("Invalid number of arguments for the chosen tool")
        current_state = itemgetter('state')(self._current_users.get(username))
        tool_args = [current_state]
        for arg_name in req_args:
            tool_args.append(args.get(arg_name))
        aa.TOOL_LIST[tool_name](*tool_args)
        return current_state

    def topic_analysis(self, username):
        current_state, state_history = itemgetter('state', 'history')(self._current_users.get(username))
        query_results = current_state.query_results
        if query_results is None:
            raise TypeError("No query results available for analysis")
        for item in query_results['included']:
            if item['id'] == conf.PUB_YEAR_FACET and item['type'] == 'facet':
                pub_dates = [(date['attributes']['value'], date['attributes']['hits']) for date in item['attributes']['items']]
                break
        else:
            raise TypeError("Query results don't contain required facet {}".format(conf.PUB_YEAR_FACET))
        pub_dates.sort()
        last_query = current_state.last_query
        queries = [{'f[{}][]'.format(conf.PUB_YEAR_FACET): item[0]} for item in pub_dates]
        for query in queries:
            query.update(last_query)
        self.set_query(username, queries)
        result_ids = self.run_multiquery(username)
        t_counts = []
        for id in result_ids:
            query, query_results = itemgetter('last_query', 'query_results')(state_history.get(id))
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
        current_state.analysis_results['topic_analysis'] = {
            'absolute_counts': abs_counts.to_dict(orient='index'),
            'relative_counts': rel_counts.to_dict(orient='index')
        }
        return current_state

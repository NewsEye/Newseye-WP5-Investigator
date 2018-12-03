from assistant.state import State
from assistant.database_access import *
from operator import itemgetter
import assistant.analysis as aa
import threading
import time
import uuid


class SystemCore(object):
    def __init__(self):
        self._current_users = {}
        self._database_api = DatabaseAPI()

    def add_user(self, user_id):
        if user_id in self._current_users.keys():
            raise TypeError("User with id {} already exists".format(user_id))
        query = {
            'q': '',
        }
        current_state = State()
        history = {
            'root': current_state,
            current_state.id: current_state
        }
        new_user = {
            'query': query,
            'state': current_state,
            'history': history
        }
        self._current_users[user_id] = new_user

    def get_unique_id(self, index):
        new_id = uuid.uuid4().hex
        while new_id in index.keys():
            new_id = uuid.uuid4().hex
        return new_id

    def forget_user(self, user_id):
        if user_id is not 'default':
            self._current_users.pop(user_id)

    def get_query(self, user_id):
        return self._current_users.get(user_id)['query']

    def set_query(self, user_id, query):
        # If the query has already been made earlier, change the state to the one corresponding to the query
        state = self.find_query(user_id, query)
        if state:
            self.set_state(user_id, state)
        # Otherwise, set the query as the current query for the user
        self._current_users.get(user_id)['query'] = query

    def find_query(self, user_id, query):
        for key, state in self._current_users[user_id]['history'].items():
            if state.last_query == query:
                return state
        return None

    def get_state(self, user_id, state_id=None):
        if not state_id:
            return self._current_users.get(user_id)['state']
        else:
            return self._current_users.get(user_id)['history'][state_id]

    def set_state(self, user_id, state):
        self._current_users.get(user_id)['state'] = state

    def get_history(self, user_id, make_tree=True):
        history = self._current_users.get(user_id)['history']
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

    def add_keyword(self, user_id, keyword):
        self._current_users.get(user_id)['query']['q'].append(keyword)

    def remove_keyword(self, user_id, keyword):
        self._current_users.get(user_id)['query']['q'].remove(keyword)

    def set_facet(self, user_id, facet, value):
        self._current_users.get(user_id)['query'][facet] = value

    def remove_facet(self, user_id, facet):
        self._current_users.get(user_id)['query'].pop(facet)

    def clear_query(self, user_id):
        self.set_query(user_id, {
            'q': '',
        })

    def threaded_query(self, state):
        delay = state.last_query.pop('test_delay', [0])[0]
        estimate = 10
        if delay:
            estimate += int(delay)
        state.query_results = {
            'message': 'Still running',
            'time_estimate': estimate
        }
        if delay:
            time.sleep(int(delay))
        result = self._database_api.run_query(state.last_query)
        state.query_results = result

    def blocking_query(self, state):
        state.last_query.pop('test_delay', None)
        state.query_results = self._database_api.run_query(state.last_query)

    def run_query(self, user_id, switch_state=True, threaded=True):
        current_query, current_state, state_history = itemgetter('query', 'state', 'history')(self._current_users.get(user_id))
        if current_state.last_query and current_query == current_state.last_query:
            return current_state
        new_state = State(state_id=self.get_unique_id(state_history), last_query=current_query, parent_id=current_state.id)
        # If switch_state is False, the new state is not stored anywhere and is forgotten after the query results are returned
        if switch_state:
            state_history[new_state.id] = new_state
            current_state.add_child(new_state.id)
        if threaded:
            t = threading.Thread(target=self.threaded_query, args=[new_state])
            t.setDaemon(False)
            t.start()
            time.sleep(5)
        else:
            self.blocking_query(new_state)
        if switch_state:
            self.set_state(user_id, new_state)
        return new_state

    def run_analysis(self, user_id, args):
        tool_name = args.get('tool')
        req_args = aa.TOOL_ARGS[tool_name]
        if len(args) != len(req_args) + 1:
            raise TypeError("Invalid number of arguments for the chosen tool")
        current_state = itemgetter('state')(self._current_users.get(user_id))
        tool_args = [current_state]
        for arg_name in req_args:
            tool_args.append(args.get(arg_name))
        aa.TOOL_LIST[tool_name](*tool_args)
        return current_state

    def topic_analysis(self, user_id):
        current_state, state_history = itemgetter('state', 'history')(self._current_users.get(user_id))
        query_results = current_state.query_results
        if query_results is None:
            raise TypeError("No query results available for analysis")
        for item in query_results['included']:
            if item['id'] == 'pub_date' and item['type'] == 'facet':
                pub_dates = [(date['attributes']['value'], date['attributes']['hits']) for date in item['attributes']['items']]
                break
        else:
            raise TypeError("Query results don't contain required facet 'pub_date'")
        pub_dates.sort()
        last_query = current_state.last_query
        queries = [{'f[pub_date][]': item[0]} for item in pub_dates]
        for query in queries:
            query.update(last_query)
        self.set_query(user_id, queries)
        return self.run_multiquery(user_id)

        # start_year = args.get('start_year')
        # end_year = args.get('end_year')
        # if start_year is None or end_year is None:
        #     raise TypeError("Invalid arguments for the chosen tool")
        # state, history = itemgetter('state', 'history')(self._current_users.get(user_id))
        # # Choose the intervals for the analysis
        # if end_year - start_year > 40:
        #     skip = 10
        # elif end_year - start_year > 20:
        #     skip = 5
        # else:
        #     skip = 1
        # slots = [{}] * math.ceil((end_year - start_year + 1) / skip)

    async def async_query(self, state):
        delay = state.last_query.pop('test_delay', [0])[0]
        estimate = 10
        if delay:
            estimate += int(delay)
        state.query_results = {
            'message': 'Still running',
            'time_estimate': estimate
        }
        if delay:
            await asyncio.sleep(int(delay))
        result = await self._database_api.ai_query(state.last_query)
        print("Query done. Result: {}".format(result))
        state.query_results = result

    @staticmethod
    def run(task=None, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        if task is None:
            return loop.run_forever()
        else:
            return loop.run_until_complete(task)

    # TODO: Refactor to handle both single and multiple queries with the same threaded function

    def run_multiquery(self, user_id):
        current_query, current_state, state_history = itemgetter('query', 'state', 'history')(self._current_users.get(user_id))
        print("Running multiple queries")
        if type(current_query) is not list:
            current_query = [
                current_query
            ]
        asyncio.set_event_loop(asyncio.new_event_loop())
        results = self.run(self.async_multiquery(current_query))
        print("Updating history")
        for i, query in enumerate(current_query):
            new_state = State(state_id=self.get_unique_id(state_history), last_query=query, parent_id=current_state.id)
            new_state.query_results = results[i]
            state_history[new_state.id] = new_state
            current_state.add_child(new_state.id)
        return current_state

    async def async_multiquery(self, queries):
        tasks = []
        async with aiohttp.ClientSession() as session:
            for query in queries:
                tasks.append(self._database_api.ai_query(session, query))
            results = await asyncio.gather(*tasks, return_exceptions=True)
        print("Queries finished, returning results")
        return results

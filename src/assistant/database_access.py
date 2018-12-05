import asyncio
import aiohttp


class DatabaseAPI(object):

    def __init__(self):
        self.baseUri = "https://demo.projectblacklight.org/catalog.json"
        self.default_params = {
            'utf8': "%E2%9C%93",
        }

    async def fetch(self, session, params={}):
        async with session.get(url=self.baseUri, params=self.fix_query_for_aiohttp(params)) as response:
            return await response.json()

    # Runs the query/queries using aiohttp. If a single query is given, the return value is a dict containing the
    # result. If queries is a list, the return value is a list containing the results in the corresponding order.
    async def async_query(self, queries):
        query_is_a_list = type(queries) is list
        if not query_is_a_list:
            queries = [queries]
        tasks = []
        async with aiohttp.ClientSession() as session:
            for query in queries:
                params = self.default_params.copy()
                params.update(query)
                print("Log, appending query: {}".format(params))
                tasks.append(self.fetch(session, params))
            results = await asyncio.gather(*tasks, return_exceptions=True)
        print("Queries finished, returning results")
        if not query_is_a_list:
            results = results[0]
        return results

    def fix_query_for_aiohttp(self, query):
        new_query = []
        for key in query.keys():
            if type(query[key]) is list:
                new_query.extend([(key, value) for value in query[key]])
            else:
                new_query.append((key, query[key]))
        return new_query

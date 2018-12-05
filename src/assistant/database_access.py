import requests
import asyncio
import aiohttp


class DatabaseAPI(object):

    def __init__(self):
        self.baseUri = "https://demo.projectblacklight.org/catalog.json"
        self.default_params = {
            'utf8': "%E2%9C%93",
        }

    def run_query(self, query):
        params = self.default_params.copy()
        params.update(query)
        print("Log, running query: {}".format(params))
        return requests.get(url=self.baseUri, params=params).json()

    async def ai_query(self, session, query):
        params = self.default_params.copy()
        params.update(query)
        print("Log, running query: {}".format(params))
        async with session.get(url=self.baseUri, params=self.fix_query_for_aiohttp(params)) as response:
            return await response.json()

    async def fetch(self, session, url):
        async with session.get(url) as response:
            return await response.json()

    def fix_query_for_aiohttp(self, query):
        new_query = []
        for key in query.keys():
            if type(query[key]) is list:
                new_query.extend([(key, value) for value in query[key]])
            else:
                new_query.append((key, query[key]))
        # new_query = [(key, value) for key in query.keys() for value in query[key]]
        print(new_query)
        return new_query

    async def async_multiquery(self, queries):
        tasks = []
        async with aiohttp.ClientSession() as session:
            for query in queries:
                tasks.append(self.ai_query(session, query))
            results = await asyncio.gather(*tasks, return_exceptions=True)
        print("Queries finished, returning results")
        return results

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
        async with session.get(url=self.baseUri, params=params) as response:
            return await response.json()

    async def fetch(self, session, url):
        async with session.get(url) as response:
            return await response.json()


import asyncio
import aiohttp
from config import Config
from flask import current_app
from aiohttp.web import HTTPUnauthorized


async def fetch(session, params={}):
    async with session.get(url=Config.BLACKLIGHT_URI, params=fix_query_for_aiohttp(params)) as response:
        if response.status == 401:
            raise HTTPUnauthorized
        result_body = await response.json()
        if Config.DATABASE_IN_USE == 'newseye':
            return result_body['response']
        elif Config.DATABASE_IN_USE == 'demo':
            return result_body


# Runs the query/queries using aiohttp. The return value is a list containing the results in the corresponding order.
async def search_database(queries, return_all=False):
    if not isinstance(queries, list):
        queries = [queries]
    tasks = []
    try:
        async with aiohttp.ClientSession(cookies=Config.COOKIES, headers=Config.HEADERS) as session:
            for query in queries:
                params = Config.BLACKLIGHT_DEFAULT_PARAMETERS.copy()
                params.update(query)
                current_app.logger.info("Log, appending search: {}".format(params))
                tasks.append(fetch(session, params))
            results = await asyncio.gather(*tasks)
    except HTTPUnauthorized:
        tasks = []
        Config.HEADERS['Authorization'] = await get_token()
        async with aiohttp.ClientSession(cookies=Config.COOKIES, headers=Config.HEADERS) as session:
            for query in queries:
                params = Config.BLACKLIGHT_DEFAULT_PARAMETERS.copy()
                params.update(query)
                current_app.logger.info("Log, appending search: {}".format(params))
                tasks.append(fetch(session, params))
            results = await asyncio.gather(*tasks)
    current_app.logger.info("Tasks finished, returning results")
    return results


# Unlike the requests package, aiohttp doesn't support key: [value_list] pairs for defining multiple values for
# a single parameter. Instead, a list of (key, value) tuples is used.
def fix_query_for_aiohttp(query):
    new_query = []
    for key in query.keys():
        if isinstance(query[key], list):
            new_query.extend([(key, value) for value in query[key]])
        else:
            new_query.append((key, query[key]))
    return new_query


async def get_token():
    async with aiohttp.ClientSession() as session:
        payload = {
            'email': Config.NEWSEYE_USERNAME,
            'password': Config.NEWSEYE_PASSWORD
        }
        async with session.post("https://platform.newseye.eu/authenticate", json=payload) as response:
            body = await response.json()
    return body['auth_token']

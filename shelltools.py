from investigator import *

from pprint import pprint

import asyncio
from app.main.controller import execute_async_tasks as execute

user = User.query.filter_by(username='pivovaro').first()
loop = asyncio.get_event_loop()

def run(command, q):
    query = (command, q)
    task = loop.run_until_complete(execute(user, queries=query))
    return task
    

def search(q):
    # e.g. q = {'q': 'maito'}
    return run('search', q)

def analyze(q):
    # e.g. q = {'utility': 'extract_facets', 'target_search': {'q': 'maito'}}
    return run('analysis', q)



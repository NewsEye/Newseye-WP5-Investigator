# tools to search from flask shell
from investigator import *

from pprint import pprint

import asyncio
from app.analysis.planner import TaskPlanner
from app.main.db_utils import generate_tasks

user = User.query.filter_by(username="pivovaro").first()
loop = asyncio.get_event_loop()
planner = TaskPlanner(loop, user)


def run(command, q):
    query = (command, q)
    task = generate_tasks(query, user=user, return_tasks=True)[0]
    loop.run_until_complete(planner.execute_and_store(task))
    return task


def search(q):
    # e.g. q = {'q': 'maito'}
    return run("search", q)


def analyze(q):
    # e.g. q = {'utility': 'extract_facets', 'target_search': {'q': 'maito'}}
    return run("analysis", q)

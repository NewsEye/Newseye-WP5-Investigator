from app.main.db_utils import store_results
import asyncio
from flask import current_app
from config import Config
from app.investigator import DEFAULT_PATTERNS


class Investigator(object):
    
    def __init__(self, planner, task):
       self.planner = planner
       self.main_task = task
       self.force_refresh = task.force_refresh
       self.task_result = {}
       self.interestingness = 0.0

    async def investigate(self):
        '''
           1st pattern: extract facets, run in parallel
           2nd pattern: extract languages, run topics
        '''

        patterns = [Pattern(self.planner, self.main_task, self.task_result, self.interestingness) for Pattern in DEFAULT_PATTERNS]       
        subtasks = await asyncio.gather(*[pattern() for pattern in patterns])

        
        



        
        

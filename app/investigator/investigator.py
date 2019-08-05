from app.main.db_utils import store_results
import asyncio
from flask import current_app
from config import Config
from app.investigator import DEFAULT_PATTERNS

def max_interestingness(interestingness):
    if isinstance(interestingness, float):        
        return interestingness
    elif isinstance(interestingness, list):
        return max([max_interestingness(i) for i in interestingness])
    elif isinstance(interestingness, dict):
        return max([max_interestingness(i) for i in interestingness.values()])
    else:
        return interestingness


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

        patterns = [Pattern(self.planner.user, self.main_task, self) for Pattern in DEFAULT_PATTERNS]
        subtasks = await asyncio.gather(*[pattern() for pattern in patterns])
        current_app.logger.debug("SUBTASKS: %s" %subtasks)
        subtasks = [task for tasklist in subtasks for task in tasklist]
        
        await self.run_subtasks_and_update_results(subtasks)
        
    async def run_subtasks_and_update_results(self, subtasks):
        """ Generate and runs may tasks in parallel, assesses results and generate new tasks if needed.
               Stores data in the database as soon as they ready

               1. gets list of subtasks
               2. runs in parallel, store in db as soon as ready
               3. result of the main task is a list of task uuid (children tasks) + interestness

         """
        
        for subtask in asyncio.as_completed([self.execute_and_store(s) for s in subtasks]):
            done_subtask = await subtask
            # the subtask result is already stored, now we have to add subtask into list of task results
            subtask_interestingness = max_interestingness(done_subtask.task_result.interestingness)
            if subtask_interestingness > self.interestingness:
                self.interestingness = subtask_interestingness
            if not str(done_subtask.uuid) in self.task_result:
                self.task_result[str(done_subtask.uuid)] = {"utility_name" : done_subtask.utility,
                                                            "utility_parameters" : done_subtask.utility_parameters,
                                                            "interestingness" : subtask_interestingness}
            current_app.logger.debug("TASK_RESULT: %s" %self.task_result)
            store_results([self.main_task], [self.task_result],
                          set_to_finished=False, interestingness=self.interestingness)
            


    async def execute_and_store(self, subtask):
        if subtask.force_refresh:
            for uuid, done_task in self.task_result.items():
                if (done_task['utility_name'] == subtask.utility and
                    done_task['utility_parameters'] == subtask.utility_parameters):
                    # don't repeat task even if force_refresh
                    # (inherited from the main task) is True---they
                    # are refreshed already in this investigation
                    # loop, nothing should change in between this way
                    # we can define patterns independently, without
                    # working if some utils are repeated and refreshed
                    subtask.force_refresh = False
                    break
        # if task not found, run it
        return await self.planner.execute_and_store(subtask)
        
        

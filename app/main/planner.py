from app import db
from app.main.db_utils import generate_tasks, store_results, verify_analysis_parameters
from app.models import TaskInstance
from app.search.search_utils import search_database
from app.analysis import UTILITY_MAP, INPUT_TYPE_MAP
from datetime import datetime
from flask import current_app
import asyncio
from app.investigator import DEFAULT_UTILITIES

class TaskPlanner(object):
    # TODO: neads major clean up in regards which functions get a single tasks and which get a list of tasks as an input
    # some functions gets lists but use onlt first value
    # others truly expect lists but execute tehm one by one instead of running in parallel 
    
    
    def __init__(self, user):
        self.user = user
        
    async def execute_user_task(self, task_uuids=None):
        tasks = TaskInstance.query.filter(TaskInstance.uuid.in_(task_uuids)).all()
        await self.execute_and_store_tasks(tasks)

    async def async_analysis(self, tasks):
        """ Generate asyncio tasks and run them, returning when all tasks are done"""

        # generates coroutines out of task objects
        async_tasks = [UTILITY_MAP[task.utility](task) for task in tasks]
        
        # here tasks are actually executed asynchronously
        # returns list of results *or* exceptions if a task fail
        results = await asyncio.gather(*async_tasks, return_exceptions=True)
        current_app.logger.info("%s finished, returning results" %[t.utility for t in tasks])
        return results


    async def investigate(self, task, utilities=DEFAULT_UTILITIES):
        """ Generate and runs may tasks in parallel, assesses results and generate new tasks if needed.
            Stores data in the database as soon as they ready

        1. gets list of utilities
        2. runs in parallel, store in db as soon as ready
        3. result of the main task is a list of task uuid (children tasks) + interestness

        default utilites: 
        1. topics: to run topics we need to know language, so it will be done on a second iteration
        2. tf-idf
        3. facets

        """
       
        subtasks = generate_tasks(user=task.user,
                               queries = [('analysis', {'search_query' : task.search_query, 'utility' : u, 'force_refresh' : task.force_refresh}) for u in utilities],
                               parent_id=task.uuid,
                               return_tasks=True)


        
        for subtask in asyncio.as_completed([self.execute_and_store(s) for s in subtasks]):
            done_subtask = await subtask
            current_app.logger.debug("Subtask %s, result %s" %(done_subtask, done_subtask.result_id ))
            # TODO:
            # 1.
            # store results (task id + interestness)
            # include sleep for testing (from insomnia)
            # 2.
            # make a complex task: facets (languages) + topics


    async def execute_and_store_tasks(self, tasks):
        # TODO: parallel                             
        for task in tasks:
            await self.execute_and_store(task)


    async def execute_and_store(self, task):
        '''this function executes exactly one task'''
        
        
        # Todo: delay estimates: based on old runtime history for similar tasks?
        # ToDo: Add timeouts for the results: timestamps are already stored, simply rerun the query if the timestamp
        #  is too old.
        
        task.task_started = datetime.utcnow()
        # to update data obtained in previous searches
        if not task.force_refresh and task.task_result:
            current_app.logger.debug("NOT RUNNING %s, result exists" %task.utility)
            task.task_status = 'finished'
            task.task_finished = datetime.utcnow()
        else:
            task.task_status = 'running'

        db.session.commit()

        if task.task_status == 'finished':
            return task
        
        if task.task_type == 'search':
            # runs searches on the external database
            search_results = await search_database([task.task_parameters])
            # stores results in the internal database
            store_results([task], search_results)

        if task.task_type == 'analysis':
            required_task = await self.get_prerequisite_tasks(task)
            if required_task:
                await self.execute_and_store(required_task)                
                task.source_uuid = required_task.uuid
                db.session.commit()
                
            # waiting for tasks to be done
            # calls main processing function
            analysis_results = await self.async_analysis([task])
            # store in the database
            store_results([task], analysis_results)

        if task.task_type == 'investigator':
            await self.investigate(task)

        return task


                                     

    @staticmethod
    def get_source_utility(utility):
        
            source_utility = INPUT_TYPE_MAP.get(utility.input_type, None)
            if not source_utility:
                source_utilities = [key for key, value in UTILITY_MAP.items() if key != utility.utility_name
                                and value.output_type == utility.input_type]
                if source_utilities:
                    if len(source_utilities) > 1:
                        # TODO: output more than one source task
                        current_app.logger.debug("More than one source utility for %s : %s, taking the first one"
                                             %(utility.utility_name, source_utilities))
                    source_utility = source_utilities[0]
            return source_utility
                

                
    async def get_prerequisite_tasks(self, task):
        # TODO: Fix the task history to work in the new way (original task is the parent and everything generated
        #  by the planner are under it)
        input_task_uuid = task.source_uuid
        if input_task_uuid:
            input_task = InstanceTask.query.filter_by(uuid=input_task_uuid).first()
            current_app.logger.debug("input_task_uuid %s" %input_task_uuid)
            if input_task is None:
                raise ValueError('Invalid source_uuid')
            task.search_query=input_task.search_query
            db.session.commit()
            return input_task


        search_parameters = task.search_query
        if search_parameters is None:
            return None
        utility = UTILITY_MAP[task.utility] 
        if utility.input_type == 'search_query':
            return None

        task_parameters = {'utility': self.get_source_utility(utility),
                                   'utility_parameters': {},
                                   'search_query': search_parameters,
                                   'force_refresh' : task.force_refresh}
        _, input_task = verify_analysis_parameters(('analysis', task_parameters))
        input_task = generate_tasks(user=task.user, queries=('analysis', task_parameters), parent_id=task.uuid,
                                            return_tasks=True)
        # Generate tasks outputs a list, here with a length of one, so we only take the contents, and not the list
        return input_task[0]

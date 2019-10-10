from app import db
from app.main.db_utils import generate_tasks, store_results, verify_analysis_parameters
from app.models import TaskInstance
from app.search.search_utils import search_database
from app.analysis import UTILITY_MAP, INPUT_TYPE_MAP
from datetime import datetime
from flask import current_app
import asyncio
from app.investigator.investigator import Investigator

class TaskPlanner(object):
    
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
        results = await asyncio.gather(*async_tasks, return_exceptions=(not current_app.debug))
        for t in tasks:
            current_app.logger.info("%s:%s finished, returning results" %(t.utility, t.uuid))
        return results            

    async def execute_and_store_tasks(self, tasks):
        ''' this function ensures parallelization task execution'''
        await asyncio.gather(*[self.execute_and_store(task) for task in tasks])

    async def execute_and_store(self, task):
        '''this function executes one task and its prerequisites'''
        
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
            investigator = Investigator(self, task)
            await investigator.investigate()
            task.task_status = 'finished'
            db.session.commit()

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
        utility = UTILITY_MAP[task.utility] 
        if input_task_uuid:
            input_task = TaskInstance.query.filter_by(uuid=input_task_uuid).first()
            current_app.logger.debug("input_task_uuid %s" %input_task_uuid)
            if input_task is None:
                raise ValueError('Invalid source_uuid')
            task.search_query=input_task.search_query
            db.session.commit()

            # return only if it has a correct type
            # e.g. search might be an input source but it doesn't have the right type and used only to cash search result
            if utility.input_type == input_task.output_type:           
                return input_task


        search_parameters = task.search_query
        if search_parameters is None:
            return None

        if utility.input_type == 'search_query':
            return None

        task_parameters = {'utility': self.get_source_utility(utility),
                           'utility_parameters': {},
                           'search_query': search_parameters,
                           'force_refresh' : task.force_refresh,
                           'source_uuid' : input_task_uuid
                         }
        _, input_task = verify_analysis_parameters(('analysis', task_parameters))
        input_task = generate_tasks(user=task.user, queries=('analysis', task_parameters), parent_id=task.uuid,
                                            return_tasks=True)
        # Generate tasks outputs a list, here with a length of one, so we only take the contents, and not the list
        return input_task[0]

from app import db
from app.main.db_utils import generate_tasks, store_results
from app.models import Task
from app.search.search_utils import search_database
from app.analysis import UTILITY_MAP
from datetime import datetime
from flask import current_app
import asyncio


# TODO: planner plans the task according to the task dependencies tree
#  Later on this will become an investigator
class Planner(object):
    """
    This class is not used for anything. Here as a remainder of the planned structure for the planner, to be removed
    when TaskPlanner is ready
    """
    async def plan(self, task):
        results = []
        while not self.satisfied(results):
            research_plan = self.plan_the_research(task, results)
            results = self.async_analysis(research_plan)
        return results

    @staticmethod
    def satisfied(task, results):
        return True

    @staticmethod
    def plan_the_research(task, results):
        return []  # return a list of new tasks


class TaskPlanner(object):
    def __init__(self, loop, user):
        self.planned_tasks = {}
        self.user = user
        self.loop = loop

    async def execute_user_task(self, task_uuids=None):

        tasks = Task.query.filter(Task.uuid.in_(task_uuids)).all()
        await self.execute_and_store(tasks)

    async def async_analysis(self, tasks):
        """ Generate asyncio tasks and run them, returning when all tasks are done"""

        # generates coroutines out of task objects
        async_tasks = [UTILITY_MAP[task.task_parameters.get('utility')](task) for task in tasks]

        # here tasks are actually executed asynchronously
        # returns list of results *or* exceptions if a task fail
        results = await asyncio.gather(*async_tasks, return_exceptions=True)
        current_app.logger.info("Tasks finished, returning results")
        return results

    async def execute_and_store(self, tasks):

        if not isinstance(tasks, list):
            tasks = [tasks]

        # Todo: delay estimates: based on old runtime history for similar tasks?
        # ToDo: Add timeouts for the results: timestamps are already stored, simply rerun the query if the timestamp
        #  is too old.

        for task in tasks:
            task.task_started = datetime.utcnow()
            # to update data obtained in previous searches
            if task.task_result and not task.task_parameters.get('force_refresh'):
                task.task_status = 'finished'
                task.task_finished = datetime.utcnow()
            else:
                task.task_status = 'running'
            # Remove the 'force_refresh parameter, if it is set. (We don't really want to store that, especially to the
            # result object. Note that we need to use a dict comprehension to generate a new dict instead of modifing
            # an existing one for the changes to be committed properly to database.
            task.task_parameters = {key: value for key, value in task.task_parameters.items() if key != 'force_refresh'}

        db.session.commit()

        tasks_to_perform = [task for task in tasks if task.task_status != 'finished']

        # A quick and dirty fix. In the future we should be able to perform the tasks in parallel.
        for task in tasks_to_perform:
            required_task = await self.get_prerequisite_tasks(task)
            if required_task:
                new_parameters = {key: value for key, value in task.task_parameters.items()}
                new_parameters['target_uuid'] = str(required_task.uuid)
                task.task_parameters = new_parameters
                db.session.commit()
                await self.execute_and_store(required_task)

            if task.task_type == 'search':
                # runs searches on the external database
                search_results = await search_database([task.task_parameters], database='solr')
                # stores results in the internal database
                store_results([task], search_results)

            if task.task_type == 'analysis':
                # waiting for tasks to be done
                # calls main processing function
                analysis_results = await self.async_analysis([task])
                # store in the database
                store_results([task], analysis_results)

    async def get_prerequisite_tasks(self, task):
        # TODO: Fix the task history to work in the new way (original task is the parent and everything generated
        #  by the planner are under it)
        input_task_uuid = task.task_parameters.get('target_uuid')
        if input_task_uuid:
            input_task = Task.query.filter_by(uuid=input_task_uuid).first()
            if input_task is None:
                raise ValueError('Invalid target_uuid')
            return input_task
        else:
            search_parameters = task.task_parameters.get('target_search')
            if search_parameters is None:
                return None
            utility = UTILITY_MAP[task.task_parameters.get('utility')]
            if utility.input_type == 'search_query':
                return None
            source_utilities = [key for key, value in UTILITY_MAP.items() if key != utility.utility_name
                                and value.output_type == utility.input_type]
            if not source_utilities:
                input_task = generate_tasks(queries=('search', search_parameters), user=self.user, parent_id=task.uuid,
                                            return_tasks=True)
            else:
                task_parameters = {'utility': source_utilities[0],
                                   'target_search': search_parameters}

                input_task = generate_tasks(user=task.user, queries=('analysis', task_parameters), parent_id=task.uuid,
                                            return_tasks=True)
        # Generate tasks outputs a list, here with a length of one, so we only take the contents, and not the list
        return input_task[0]

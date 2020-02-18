from app import db
from app.utils.db_utils import generate_task, store_results, verify_analysis_parameters
from app.models import Task
from app.utils.search_utils import search_database

# from app.analysis import UTILITY_MAP, INPUT_TYPE_MAP
from datetime import datetime
from flask import current_app
import asyncio
from app.investigator.investigator import Investigator
import warnings


class TaskPlanner(object):
    def __init__(self, user):
        self.user = user

    async def execute_user_task(self, task_uuid=None):
        task = Task.query.filter(Task.uuid == task_uuid).all()
        # .all() returns a list
        await self.execute_and_store_tasks(task)

    async def async_analysis(self, tasks):
        """ Generate asyncio tasks and run them, returning when all tasks are done"""
        # generates coroutines out of task objects

        async_tasks = []
        for task in tasks:
            # importing processor using its name and import path stored in the database and linked to the task
            # currently all processors are from this package so it would be possible to import them directly
            # in the future it is possible that we use another processing package,
            # which would need to register its processors in the database and then they will be imported
            # so, planner doesn't nede to know import path beforehand and imports it during runtime
            Processor = getattr(
                __import__(task.processor.import_path, fromlist=[task.processor.name]),
                task.processor.name,
            )
            processor = Processor()

            async_tasks.append(processor(task))

        #current_app.logger.debug("ASYNC_TASKS: %s" %async_tasks)
        # here tasks are actually executed asynchronously
        # returns list of results *or* exceptions if a task fail
        results = await asyncio.gather(*async_tasks, return_exceptions=(not current_app.debug))

#        current_app.logger.debug("RESULTS:%s" %results)

        
        for t in tasks:
            current_app.logger.info("%s:%s finished, returning results" % (t.processor, t.uuid))
        return results

    async def execute_and_store_tasks(self, tasks):
        """ this function ensures parallelization task execution"""

        await asyncio.gather(*[self.execute_and_store(task) for task in tasks])

    async def result_exists(self, task):
        # ToDo: Add timeouts for the results: timestamps are already stored, simply rerun the query if the timestamp
        ##  is too old.
        related_tasks = Task.query.filter(
            Task.processor_id == task.processor_id,
            Task.dataset_id == task.dataset_id,
            Task.solr_query_id == task.solr_query_id,
            Task.task_status == "finished",
            Task.task_results is not None,
        ).all()
        if not related_tasks:
            return

        related_task = sorted(related_tasks, key=lambda t: t.task_finished)[-1]
        result = related_task.task_result

        task.task_results.append(result)
        return True

    async def execute_and_store(self, task):
        """this function executes one task and its prerequisites"""

        # Todo: delay estimates: based on old runtime history for similar tasks?

        task.task_started = datetime.utcnow()
        # to update data obtained in previous searches

        #current_app.logger.debug("FORCE_REFRESH %s" % task.force_refresh)

        if not task.force_refresh:
            # search for similar tasks, reuse results
            if await self.result_exists(task):
                current_app.logger.debug("NOT RUNNING %s, result exists" % task.processor.name)
                task.task_status = "finished"
                task.task_finished = datetime.utcnow()
        else:
            task.task_status = "running"

        db.session.commit()

        if task.task_status == "finished":
            return task

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
        return task

    @staticmethod
    def get_source_utility(utility):

        source_utility = INPUT_TYPE_MAP.get(utility.input_type, None)
        if not source_utility:
            source_utilities = [
                key
                for key, value in UTILITY_MAP.items()
                if key != utility.utility_name and value.output_type == utility.input_type
            ]
            if source_utilities:
                if len(source_utilities) > 1:
                    # TODO: output more than one source task
                    current_app.logger.debug(
                        "More than one source utility for %s : %s, taking the first one"
                        % (utility.utility_name, source_utilities)
                    )
                source_utility = source_utilities[0]
        return source_utility

    async def get_prerequisite_tasks(self, task):
        # TODO: get prerequisite tasks")
        return None

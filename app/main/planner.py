from app import db, analysis
from app.utils.db_utils import generate_task, store_results
from app.models import Task, Processor
from app.utils.search_utils import search_database

# from app.analysis import UTILITY_MAP, INPUT_TYPE_MAP
from datetime import datetime
from flask import current_app
import asyncio
from app.investigator.investigator import Investigator
import warnings
from config import Config

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
            # so, planner doesn't need to know import path beforehand and imports it during runtime
            Processor = getattr(
                __import__(task.processor.import_path, fromlist=[task.processor.name]),
                task.processor.name,
            )
            processor = Processor()

            async_tasks.append(processor(task))

        # current_app.logger.debug("ASYNC_TASKS: %s" %async_tasks)
        # here tasks are actually executed asynchronously
        # returns list of results *or* exceptions if a task fail
        results = await asyncio.gather(
            *async_tasks, return_exceptions=(not current_app.debug)
        )

        #        current_app.logger.debug("RESULTS:%s" %results)

        for t in tasks:
            current_app.logger.info(
                "%s:%s finished, returning results" % (t.processor, t.uuid)
            )
        return results

    async def execute_and_store_tasks(self, tasks):
        """ this function ensures parallelization task execution"""

        await asyncio.gather(*[self.execute_and_store(task) for task in tasks])

    async def result_exists(self, task):
        # ToDo: Add timeouts for the results: timestamps are already stored, simply rerun the query if the timestamp
        ##  is too old.
        related_tasks = Task.query.filter(
            Task.processor_id == task.processor_id,
            Task.parameters == task.parameters,
            Task.dataset_id == task.dataset_id,
            Task.solr_query_id == task.solr_query_id,
            Task.task_status == "finished",
            Task.task_results is not None,
        ).all()
        related_tasks = [
            rt
            for rt in sorted(related_tasks, key=lambda t: t.task_finished)
            if rt.task_result is not None
        ]
        if not related_tasks:
            return
        result = related_tasks[-1].task_result
        task.task_results.append(result)
        return True

    async def execute_and_store(self, task):
        """this function executes one task and its prerequisites"""

        # TODO: delay estimates: based on old runtime history for similar tasks?

        task.task_started = datetime.utcnow()
        # to update data obtained in previous searches

        # current_app.logger.debug("FORCE_REFRESH %s" % task.force_refresh)

        current_app.logger.debug(
            "TASK %s FORCE_REFRESH: %s" % (task, task.force_refresh)
        )
        if not task.force_refresh:
            # search for similar tasks, reuse results
            if await self.result_exists(task):
                current_app.logger.debug(
                    "NOT RUNNING %s, result exists" % task.processor.name
                )
                task.task_status = "finished"
                task.task_finished = datetime.utcnow()
        else:
            task.task_status = "running"

        db.session.commit()

        if task.task_status == "finished":
            return task

        required_tasks = await self.get_prerequisite_tasks(task)

        current_app.logger.debug("REQUIRED_TASKS: %s" % required_tasks)

        if required_tasks:
            tasks_to_execute = [t for t in required_tasks if not t.task_result]
            await self.execute_and_store_tasks(tasks_to_execute)
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
                if key != utility.utility_name
                and value.output_type == utility.input_type
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
        current_app.logger.debug(
            "task.processor.input_type: %s" % task.processor.input_type
        )
        if (
            task.processor.input_type == "dataset"
            or task.processor.name in Config.PROCESSOR_EXCEPTION_LIST
        ):
            return

        parent_uuids = task.parent_uuid
        input_tasks = []

        current_app.logger.debug("parent.uuids: %s" % parent_uuids)
        if parent_uuids:
            for parent_uuid in parent_uuids:

                input_task = Task.query.filter_by(uuid=parent_uuid).first()

                current_app.logger.debug("input_task_uuid %s" % parent_uuid)
                if input_task is None:
                    raise ValueError("Invalid parent_uuid")

                input_tasks.append(input_task)

        else:
            task_parameters = {
                "processor": self.get_source_processor(task),
                "parameters": {},
                "search_query": task.solr_query.search_query
                if task.solr_query
                else None,
                "dataset": task.dataset,
                "force_refresh": task.force_refresh,
            }

            input_task = generate_task(
                query=task_parameters, user=task.user, return_task=True,
            )

            task.parents.append(input_task)
            input_tasks.append(input_task)
            db.session.commit()
        return input_tasks

    @staticmethod
    def get_source_processor(task):
        related_processors = [
            p.name
            for p in Processor.query.all()
            if p.output_type == task.processor.input_type
        ]

        if not related_processors:
            raise ValueError(
                "Cannot find processor with output_type %s" % task.processor.input_type
            )
        elif len(related_processors) > 1:
            raise NotImplementedError(
                "Don't know how to get a prerequisite task for %s, too many options: %s"
                % (task.processor.input_type, " ".join(related_processors))
            )
        else:
            return related_processors[0]

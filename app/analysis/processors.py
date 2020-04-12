from app import db
from app.models import Processor, Task
from app.utils.search_utils import search_database
from app.analysis import assessment
import asyncio

from flask import current_app


class AnalysisUtility(Processor):
    @classmethod
    def make_processor(cls):
        if not Processor.query.filter_by(
            name=cls.__name__, import_path=cls.__module__
        ).one_or_none():
            db.session.add(cls._make_processor())
            db.session.commit()

    @classmethod
    def _make_processor(cls):
        return Processor(
            name=cls.__name__, import_path=cls.__module__, parameter_info=[]
        )

    def __init__(self, initialize=False):
        if initialize:
            pass
        else:
            processor = Processor.query.filter_by(name=self.__class__.__name__).all()
            if len(processor) > 1:
                raise NotImplementedError(
                    "More than one processor with the same name %s"
                    % self.__class__.__name__
                )
            processor = processor[0]
            self.processor = processor

    async def __call__(self, task):
        self.task = task
        self.input_data = await self._get_input_data()
        self.result = await self.make_result()
        self.interestingness = await self._estimate_interestingness()
        return {"result": self.result, "interestingness": self.interestingness}

    @staticmethod
    def get_input_task(task):
        input_task_uuid = task.source_uuid
        if input_task_uuid:
            input_task = Task.query.filter_by(uuid=input_task_uuid).first()
        else:
            input_task = None
        return input_task

    async def _get_input_data(self):
        current_app.logger.debug("PARENT UUID: %s" % self.task.parent_uuid)

        if self.task.parent_uuid:
            input_task = Task.query.filter_by(uuid=self.task.parent_uuid).first()
            if input_task:
                wait_time = 0
                while input_task.task_status != "finished" and wait_time < 100:
                    # what happens after that? should we raise some
                    # exception? cancel all tasks?
                    asyncio.sleep(wait_time)
                    wait_time += 1
                    if input_task.task_status == "failed":
                        raise BadRequest(
                            "Task used as source_uuid (%s) failed" % input_task.uuid
                        )

                return await self.get_input_data(input_task.task_result.result)
        return await self.get_input_data()

    async def get_input_data(self):
        return await search_database(self.task.search_query, retrieve="all")

    def get_description(self):
        return self.processor.dict()

    async def make_result(self, task):
        return {"error": "This utility has not yet been implemented"}

    async def _estimate_interestingness(self):
        """
        Computes overall interestingness of the result as a single number.
        Currently: maximum of all numbers found in interestingness dictionary.
        """
        interestingness = await self.estimate_interestingness()
        # maximum of all numbers
        # a temporal solution
        interestingness.update({"overall": assessment.recoursive_max(interestingness)})
        return interestingness

    async def estimate_interestingness(self):
        # convert all numerical lists and dict values into distributions (0-1)
        return assessment.recoursive_distribution(self.result)
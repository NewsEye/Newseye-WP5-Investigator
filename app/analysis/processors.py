from app import db
from app.models import Processor
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
        self.input_data = await self.get_input_data()
        self.result = await self.make_result()
        self.interestingness = await self._estimate_interestingness()
        return {"result": self.result, "interestingness": self.interestingness}

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
        interestingness.update({"overall": assessment.recoursive_max(interestingness)})
        return interestingness

    async def estimate_interestingness(self):
        # convert all numerical lists and dict values into distributions (0-1)
        return assessment.recoursive_distribution(self.result)

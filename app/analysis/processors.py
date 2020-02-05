from app import db
from app.models import Processor
from app.utils.search_utils import search_database
from app.analysis import assessment  # , timeseries
import asyncio
from app.utils.db_utils import make_query_from_dataset




class AnalysisUtility(Processor):
    @classmethod
    def make_processor(cls):
        if not Processor.query.filter_by(name=cls.__name__).one_or_none():
            db.session.add(cls._make_processor())
            db.session.commit()

    @classmethod
    def _make_processor(cls):
        return Processor(name=cls.__name__, import_path=cls.__module__, parameter_info=[])

    def __init__(self, initialize=False):
        if initialize:
            pass
        else:
            processor = Processor.query.filter_by(name=self.__class__.__name__).all()
            if len(processor) > 1:
                raise NotImplementedError(
                    "More than one processor with the same name %s" % self.__class__.__name__
                )
            processor = processor[0]
            self.processor = processor

    async def __call__(self, task):
        self.task = task
        self.input_data = await self._get_input_data()
        self.result = await self.make_result()
        self.interestingness = await self.estimate_interestingness()
        return {"result": self.result, "interestingness": self.interestingness}

    async def _get_input_data(self):
        # TODO: check input type; in many cases we just need to get result form the internal db
        if self.task.dataset:
            return await self.get_input_data(make_query_from_dataset(self.task.dataset))
        elif self.task.solr_query:
            return await self.get_input_data(self.task.solr_query.search_query)
        else:
            # source_uuid
            raise NotImplementedError("cannot get data for task %s" % task)

    async def get_input_data(self, solr_query):
        return await search_database(solr_query, retrieve="all")

    def get_description(self):
        return self.processor.dict()

    async def make_result(self, task):
        return {"error": "This utility has not yet been implemented"}

    async def estimate_interestingness(self):
        # convert all numerical lists and dict values into distributions (0-1)
        return assessment.recoursive_distribution(self.result)






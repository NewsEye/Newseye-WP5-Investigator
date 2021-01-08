from app import db
from app.models import Processor, Task
from app.utils.search_utils import DatabaseSearch
from app.analysis import assessment
import asyncio
from werkzeug.exceptions import BadRequest
from flask import current_app


class AnalysisUtility(Processor):
    @classmethod
    def make_processor(cls):
        processors = Processor.query.filter_by(
            name=cls.__name__, import_path=cls.__module__
        ).all()

        processors = [p for p in processors if not p.deprecated]

        assert len(processors) <= 1

        if not processors:
            processor = cls._make_processor()
            current_app.logger.debug("NEW PROCESSOR: %s" % processor)
            db.session.add(processor)
            db.session.commit()

    @classmethod
    def _make_processor(cls):
        return Processor(
            name=cls.__name__, import_path=cls.__module__, parameter_info=[]
        )

    def __init__(self, initialize=False, solr_controller=None):
        if initialize:
            pass
        else:
            assert solr_controller
            self.solr_controller = solr_controller
            processor = [
                p
                for p in Processor.query.filter_by(name=self.__class__.__name__).all()
                if not p.deprecated
            ]
            if len(processor) > 1:
                raise NotImplementedError(
                    "More than one processor with the same name %s"
                    % self.__class__.__name__
                )
            processor = processor[0]
            self.processor = processor

    async def search_database(self, queries, **kwargs):

        current_app.logger.debug(
            "PROCESSOR %s STARTS SOLR SEARCH" % self.__class__.__name__
        )
        # current_app.logger.debug("SEARCH: %s" %queries)
        database_search = DatabaseSearch(self.solr_controller)
        res = await database_search.search(queries, **kwargs)
        current_app.logger.debug(
            "PROCESSOR %s DONE SOLR SEARCH" % self.__class__.__name__
        )
        return res

    async def __call__(self, task):
        self.task = task
        self.input_data = await self._get_input_data()
        self.result = await self.make_result()
        self.images = await self.make_images()
        self.interestingness = await self._estimate_interestingness()
        return {
            "result": self.result,
            "interestingness": self.interestingness,
            "images": self.images,
        }

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
        parent_uuids = self.task.parent_uuid
        if parent_uuids:
            for parent_uuid in parent_uuids:
                self.input_task = Task.query.filter_by(uuid=parent_uuid).first()
                if self.input_task:
                    wait_time = 0
                    while self.input_task.task_status != "finished" and wait_time < 100:
                        asyncio.sleep(wait_time)
                        wait_time += 1
                        if self.input_task.task_status == "failed":
                            raise BadRequest(
                                "Task used as source_uuid (%s) failed"
                                % self.input_task.uuid
                            )
                if len(parent_uuids) == 1:
                    try:
                        return await self.get_input_data(self.input_task.task_result)
                    except Exception as e:
                        # raise e
                        current_app.logger.debug(
                            "!!!!!!!!Don't know how to use previous_task_result for %s Result: %s Exception: %s"
                            % (self.processor.name, self.input_task.task_result, e)
                        )
                        ## TODO: get rid of this 'pass', this is counter-intuitive behaviour
                        pass  # try to call get_input_data in a standard way, without parameters

        return await self.get_input_data()

    async def get_input_data(self, previous_task_result=None):
        if previous_task_result:
            return previous_task_result.result
        else:
            return await self.search_database(self.task.search_query, retrieve="all")

    def get_description(self):
        return self.processor.dict()

    async def make_result(self, task):
        return {"error": "This utility has not yet been implemented"}

    async def make_images(self):
        pass

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

    async def get_languages(self):
        # not optimal: extract facets already does this
        # don't know how to use to results...
        facets = await self.search_database(self.task.search_query, retrieve="facets")
        for facet in facets["facets"]:
            if facet["name"] == "language_ssi":
                return {
                    i["label"]: i["hits"]
                    for i in facet["items"]
                    if i["label"]
                    in ["fi", "de", "fr"]  # no support for swedish at the moment
                }

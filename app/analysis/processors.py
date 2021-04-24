from app import db
from app.models import Processor, Task
from app.utils.search_utils import DatabaseSearch
from app.analysis import assessment
import asyncio
from werkzeug.exceptions import BadRequest, NotFound
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
        self.updated_parameters = {}

        try:
            self.input_data = await self._get_input_data()
            #current_app.logger.debug("SELF.INPUT_DATA: %s" %self.input_data)
        except BadRequest as e:
            current_app.logger.info("BadRequest: {0}".format(e))
            self.input_data = None
        except Exception as e:
            raise e
        
        if self.input_data:
            try:
                self.result = await self.make_result()
            except NotFound as e:
                current_app.logger.info("NotFound: {0}".format(e))
                self.result = {}
                self.interestingness = {"overall": 0.0}
                self.images = None
            else:
                self.interestingness = await self._estimate_interestingness()
                self.images = await self.make_images()
        else:
            current_app.logger.info("DATA UNAVAILABLE FOR TASK %s" % task)
            self.result = {}
            self.interestingness = {"overall": 0.0}
            self.images = None

        return {
            "result": self.result,
            "interestingness": self.interestingness,
            "images": self.images,
            "updated_parameters": self.updated_parameters,
        }

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
                    current_app.logger.debug("PARENT_UUIDS: %s" % parent_uuids)
                    try:
                        return await self.get_input_data(self.input_task.task_result)
                    except Exception as e:
                        current_app.logger.debug(
                            "!!!!!!!!Don't know how to use previous_task_result for %s Task %s Result: %s Exception: %s"
                            % (
                                self.processor.name,
                                self.input_task.uuid,
                                self.input_task.task_result,
                                e,
                            )
                        )
                        #raise e
                        ## TODO: get rid of this 'pass', this is counter-intuitive behaviour
                        pass  # try to call get_input_data in a standard way, without parameters

        return await self.get_input_data()

    async def get_input_data(self, previous_task_result=None):
        if previous_task_result and not (task.processor.input_type == dataset):
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
        if isinstance(interestingness, float):
            interestingness = {"overall": interestingness}
        elif isinstance(interestingness, dict):
            # maximum of all numbers
            # a temporal solution
            interestingness.update(
                {"overall": assessment.recoursive_max(interestingness)}
            )
        else:
            raise TypeError(
                "Unexpected interestingness type: %s" % type(interestingness)
            )
        return interestingness

    async def estimate_interestingness(self):
        # convert all numerical lists and dict values into distributions (0-1)
        return assessment.recoursive_distribution(self.result)

    async def get_languages(self):
        # not optimal: extract facets already does this

        facets = await self.search_database(self.task.search_query, retrieve="facets")
        for facet in facets["facets"]:
            if facet["name"] == "language_ssi":
                return {i["label"]: i["hits"] for i in facet["items"]}

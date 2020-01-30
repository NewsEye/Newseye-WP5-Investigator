from app import db
from app.models import Task, Processor
from app.search.search_utils import search_database
from config import Config
from flask import current_app
import numpy as np
import pandas as pd
from app.analysis import assessment  # , timeseries
from operator import itemgetter
from werkzeug.exceptions import BadRequest
import asyncio
from app.main.db_utils import make_query_from_dataset


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
        return { "result" : self.result,
                 "interestingness" : self.interestingness}

    async def _get_input_data(self):
        # TODO: check input type; in many cases we just need to get result form the internal db
        if self.task.dataset:
            return await self.get_input_data(make_query_from_dataset(self.task.dataset))
        elif self.task.solr_query:
            return await self.get_input_data(self.task.solr_query)
        else:
            # source_uuid
            raise NotImplementedError("cannot get data for task %s" % task)

    async def get_input_data(self, solr_query, retrieve="facets"):
        return await search_database(solr_query, retrieve=retrieve)

    def get_description(self):
        return self.processor.dict()

    async def make_result(self, task):
        return {"error": "This utility has not yet been implemented"}

    async def estimate_interestingness(self):
        # convert all numerical lists and dict values into distributions (0-1)
        return assessment.recoursive_distribution(self.result)
        
    

class ExtractFacets(AnalysisUtility):
    @classmethod
    def _make_processor(cls):
        return Processor(
            name=cls.__name__,
            import_path=cls.__module__,
            description="Examines the document set given as input, and finds all the different facets for which values have been set in at least some of the documents.",
            parameter_info=[],
            input_type="dataset",
            output_type="facet_list",
        )

    async def make_result(self):
        """ Extract all facet values found in the input data and the number of occurrences for each."""
        # too complicated
        # seems nobody is using these Config.constants
        # except for this processor --- get read of them???
        facets = {}
        for feature in self.input_data[Config.FACETS_KEY]:
            values = {}
            for item in feature[Config.FACET_ITEMS_KEY]:
                values[item[Config.FACET_VALUE_LABEL_KEY]] = item[Config.FACET_VALUE_HITS_KEY]

            if feature[Config.FACET_ID_KEY] in Config.AVAILABLE_FACETS.values():
                facets[feature[Config.FACET_ID_KEY]] = values

        return facets

 
        

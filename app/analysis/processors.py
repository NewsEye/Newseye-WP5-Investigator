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
        self.input_data = await self._get_input_data(task)
        return await self.call(task)

    async def call(self, task):
        return {"error": "This utility has not yet been implemented"}

    async def _get_input_data(self, task):
        # TODO: check input type; in many cases we just need to get result form the internal db
        if task.dataset:
            return await self.get_input_data(make_query_from_dataset(task.dataset))
        elif task.solr_query:
            return await self.get_input_data(task.solr_query)
        else:
            # source_uuid
            raise NotImplementedError("cannot get data for task %s" % task)

    async def get_input_data(self, solr_query, retrieve="facets"):
        return await search_database(solr_query, retrieve=retrieve)

    def get_description(self):
        return {
            "name": self.processor.name,
            "description": self.processor.description,
            "parameters": self.processor.parameter_info,
            "input_type": self.processor.input_type,
            "output_type": self.processor.output_type,
        }


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

    async def call(self, task):
        """ Extract all facet values found in the input data and the number of occurrences for each."""
        facets = {}
        for feature in self.input_data[Config.FACETS_KEY]:
            values = {}
            for item in feature[Config.FACET_ITEMS_KEY]:
                values[item[Config.FACET_VALUE_LABEL_KEY]] = item[Config.FACET_VALUE_HITS_KEY]
            facets[feature[Config.FACET_ID_KEY]] = values
        return {
            "result": facets,
            # interestingness as distribution
            # before it was just facet values, now it's normalized to [0,1]
            "interestingness": {
                f: {
                    k: v
                    for (k, v) in zip(
                        facets[f].keys(), list(assessment.Distribution(facets[f].values()).dist),
                    )
                }
                for f in facets
            },
        }

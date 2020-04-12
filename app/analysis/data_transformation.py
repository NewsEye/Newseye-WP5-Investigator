from app.models import Processor
from app.analysis.processors import AnalysisUtility
from app.utils.search_utils import search_database
from app.analysis import assessment
from flask import current_app
from app.analysis.facet_processors import AVAILABLE_FACETS
from copy import copy
import asyncio
import numpy as np


class SplitByFacet(AnalysisUtility):
    @classmethod
    def _make_processor(cls):
        return Processor(
            name=cls.__name__,
            import_path=cls.__module__,
            description="Split dataset by a given facet",
            parameter_info=[
                {
                    "name": "facet",
                    "description": "facet used to split data, could be LANGUAGE, NEWSPAPER_NAME or PUB_YEAR",
                    "type": "string",
                    "default": "LANGUAGE",
                    "required": False,
                }
            ],
            input_type="facet_list",
            output_type="dataset_list",
        )

    async def get_input_data(self, previous_task_result):
        return previous_task_result[self.task.parameters["facet"]]

    async def make_result(self):
        if len(self.input_data) == 1:
            return []
        facet_field = AVAILABLE_FACETS[self.task.parameters["facet"]]
        search_query = self.task.search_query
        fq = search_query.get("fq", [])
        if isinstance(fq, str):
            fq = [fq]
        queries = {}
        for f in self.input_data:
            q = copy(search_query)
            q["fq"] = [*fq, "{}:{}".format(facet_field, f)]
            queries[f] = q

        return queries

    async def estimate_interestingness(self):
        interestingness = {
            f: 1 - d if d > 0.5 else d
            for f, d in assessment.recoursive_distribution(self.input_data).items()
        }
        return {
            f: 2 * interestingness[f]
            for f in sorted(interestingness, key=interestingness.get, reverse=True)
        }

    async def _estimate_interestingness(self):
        interestingness = await self.estimate_interestingness()
        interestingness.update(
            {"overall": assessment.normalized_entropy(self.input_data.values())}
        )
        return interestingness

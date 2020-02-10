from app.analysis.processors import AnalysisUtility
from app.models import Processor
from app.utils.search_utils import search_database
from app.analysis import assessment
import pandas as pd
from flask import current_app

FACETS_KEY = "facets"
FACET_ID_KEY = "name"
FACET_ITEMS_KEY = "items"
FACET_VALUE_LABEL_KEY = "label"
FACET_VALUE_HITS_KEY = "hits"
AVAILABLE_FACETS = {
    "LANGUAGE": "language_ssi",
    "NEWSPAPER_NAME": "member_of_collection_ids_ssim",
    "PUB_YEAR": "year_isi",
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

    async def make_result(self):
        """ Extract all facet values found in the input data and the number of occurrences for each."""
        # too complicated
        # seems nobody is using these constants
        # except for this processor --- get read of them???
        facets = {}
        for feature in self.input_data[FACETS_KEY]:
            values = {}
            for item in feature[FACET_ITEMS_KEY]:
                values[item[FACET_VALUE_LABEL_KEY]] = item[FACET_VALUE_HITS_KEY]

            if feature[FACET_ID_KEY] in AVAILABLE_FACETS.values():
                facets[feature[FACET_ID_KEY]] = values

        return facets


class GenerateTimeSeries(AnalysisUtility):
    @classmethod
    def _make_processor(cls):
        return Processor(
            name=cls.__name__,
            import_path=cls.__module__,
            description="Generates timeseries for facets",
            parameter_info=[
                {
                    "name": "facet_name",
                    "description": "the facet to be analysed",
                    "type": "string",
                    "default": "NEWSPAPER_NAME",
                    "required": False,
                },
                ## TODO: Add a parameter for choosing what to do with missing data
            ],
            input_type="dataset",
            output_type="timeseries",
        )

    async def make_result(self):
        # TODO Add support for total document count

        facet_name = self.task.parameters["facet_name"]
        facet_string = AVAILABLE_FACETS.get(facet_name)
        if facet_string is None:
            raise TypeError(
                "Facet not specified or specified facet not available in current database"
            )

        year_facet = AVAILABLE_FACETS["PUB_YEAR"]
        for facet in self.input_data[FACETS_KEY]:
            if facet[FACET_ID_KEY] == year_facet:
                years_in_data = [item["value"] for item in facet["items"]]
                break
        else:
            raise TypeError("Search results don't contain required facet {}".format(year_facet))

        original_search = self.task.search_query

        queries = [{"fq": "{}:{}".format(year_facet, item)} for item in years_in_data]
        for query in queries:
            query.update(original_search)
        query_results = await search_database(queries, retrieve="facets")

        f_counts = []
        for query, result in zip(queries, query_results):
            if result is None:
                current_app.logger.error("Empty query result in generate_time_series")
                continue
            _, year = query["fq"].split(":")
            total_hits = result["numFound"]
            for facet in result[FACETS_KEY]:
                if facet[FACET_ID_KEY] == facet_string:
                    f_counts.extend(
                        [
                            [year, item["value"], item["hits"], item["hits"] / total_hits]
                            for item in facet["items"]
                        ]
                    )
                    break

        # TODO: count the number of items with no value defined for the desired facet
        # TODO: get all years available in the database

        df = pd.DataFrame(f_counts, columns=["year", facet_name, "count", "rel_count"])

        abs_counts = df.pivot(index=facet_name, columns="year", values="count").fillna(0)
        rel_counts = df.pivot(index=facet_name, columns="year", values="rel_count").fillna(0)
        analysis_results = {
            "absolute_counts": self.make_dict(abs_counts),
            "relative_counts": self.make_dict(rel_counts),
        }
        return analysis_results

    @staticmethod
    def make_dict(counts):
        count_dict = counts.to_dict(orient="index")
        info = pd.concat(
            [counts[counts > 0].min(axis=1), counts.max(axis=1), counts[counts > 0].mean(axis=1)],
            axis=1,
        )
        info.columns = ["min", "max", "avg"]
        info_dict = info.to_dict(orient="index")

        out_dict = {f: {**count_dict[f], **info_dict[f]} for f in count_dict}

        return out_dict

    async def estimate_interestingness(self):
        rel_counts = {
            k: v
            for k, v in self.result["relative_counts"].items()
            if k not in ["min", "max", "avg"]
        }
        interestingness = assessment.recoursive_distribution(rel_counts)
        return {
            k: (assessment.normalized_entropy(v.values()), v) for k, v in interestingness.items()
        }

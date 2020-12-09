from app.analysis.processors import AnalysisUtility
from app.models import Processor
from app.utils.search_utils import search_database
from app.analysis import assessment
import pandas as pd
from flask import current_app
from copy import copy
from collections import defaultdict

FACETS_KEY = "facets"
FACET_ID_KEY = "name"
FACET_ITEMS_KEY = "items"
FACET_VALUE_LABEL_KEY = "label"
FACET_VALUE_HITS_KEY = "hits"
AVAILABLE_FACETS = {
    "LANGUAGE": "language_ssi",
    "NEWSPAPER_NAME": "member_of_collection_ids_ssim",
    "PUB_YEAR": "year_isi",
    "DATE": "date_created_dtsi",
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
        facets = {}
        available_facets = {v: k for k, v in AVAILABLE_FACETS.items()}
        current_app.logger.debug("AVAILABLE_FACETS: %s" % available_facets)
        for feature in self.input_data[FACETS_KEY]:
            values = {}
            for item in feature[FACET_ITEMS_KEY]:
                values[item[FACET_VALUE_LABEL_KEY]] = item[FACET_VALUE_HITS_KEY]

            if feature[FACET_ID_KEY] in available_facets:
                facets[available_facets[feature[FACET_ID_KEY]]] = values

        if not "PUB_YEAR" in facets:
            facets["PUB_YEAR"] = defaultdict(int)
            # try to recover from dates
            for date, count in facets["DATE"].items():
                facets["PUB_YEAR"][date[:4]] += count
            facets["PUB_YEAR"] = dict(facets["PUB_YEAR"])

        if "DATE" in facets:
            del facets["DATE"]

        years = [int(y) for y in facets["PUB_YEAR"]]

        for y in range(min(years), max(years)):
            if y not in years:
                facets["PUB_YEAR"][str(y)] = 0

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
                    "values": ["LANGUAGE", "NEWSPAPER_NAME"],
                    "required": False,
                },
                ## TODO: Add a parameter for choosing what to do with missing data
            ],
            input_type="dataset",
            output_type="timeseries",
        )

    async def make_result(self):
        # This is example of the function, which would be trickier to adapt to another document structure

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
            raise TypeError(
                "Search results don't contain required facet {}".format(year_facet)
            )

        original_search = self.task.search_query

        fq = original_search.get("fq", [])
        if isinstance(fq, str):
            fq = [fq]

        queries = []
        years = []
        for year in years_in_data:
            q = copy(original_search)
            q["fq"] = [*fq, "{}:{}".format(year_facet, year)]
            queries.append(q)
            years.append(year)
        #
        #
        # if self.task.solr_query:
        #     queries = [{"fq": "{}:{}".format(year_facet, item)} for item in years_in_data]
        #     for q in queries:
        #         q.update(original_search)
        # elif self.task.dataset:
        #     queries = [{"q" : item, "qf" : year_facet, 'fq':original_search["fq"]} for item in years_in_data]
        # else:
        #     raise NotImplementedError

        query_results = await search_database(queries, retrieve="facets")

        f_counts = []
        for year, result in zip(years, query_results):
            if result is None:
                current_app.logger.error("Empty query result in generate_time_series")
                continue

            total_hits = result["numFound"]
            for facet in result[FACETS_KEY]:
                if facet[FACET_ID_KEY] == facet_string:
                    f_counts.extend(
                        [
                            [
                                year,
                                item["value"],
                                item["hits"],
                                item["hits"] / total_hits,
                            ]
                            for item in facet["items"]
                        ]
                    )
                    break

        df = pd.DataFrame(f_counts, columns=["year", facet_name, "count", "rel_count"])
        abs_counts = df.pivot(index=facet_name, columns="year", values="count").fillna(
            0
        )
        rel_counts = df.pivot(
            index=facet_name, columns="year", values="rel_count"
        ).fillna(0)

        analysis_results = {
            "absolute_counts": self.make_dict(abs_counts),
            "relative_counts": self.make_dict(rel_counts),
        }
        return analysis_results

    @staticmethod
    def make_dict(counts):
        count_dict = counts.to_dict(orient="index")
        years = [int(y) for c_dict in count_dict.values() for y in c_dict]
        for d in count_dict:
            for y in range(min(years), max(years)):
                if str(y) not in count_dict[d]:
                    count_dict[d][str(y)] = 0

        info = pd.concat(
            [
                counts[counts > 0].min(axis=1),
                counts.max(axis=1),
                counts[counts > 0].mean(axis=1),
            ],
            axis=1,
        )
        info.columns = ["min", "max", "avg"]
        info_dict = info.to_dict(orient="index")

        out_dict = {f: {**count_dict[f], **info_dict[f]} for f in count_dict}

        return out_dict

    async def estimate_interestingness(self):
        counts = {
            k: v
            for k, v in self.result["absolute_counts"].items()
            if k not in ["min", "max", "avg"]
        }
        interestingness = assessment.recoursive_distribution(counts)
        return {
            k: (
                1 - assessment.normalized_entropy(v.values()),
                v,
            )  # we want minimal entropy, which means sharpest peak
            for k, v in interestingness.items()
        }

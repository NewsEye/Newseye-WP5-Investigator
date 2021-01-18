from app.models import Processor, Task
from app.analysis.processors import AnalysisUtility
from app.analysis import assessment
from flask import current_app
from app.analysis.facet_processors import AVAILABLE_FACETS
from copy import copy
import asyncio
import numpy as np
from werkzeug.exceptions import BadRequest
import datetime
from app.utils.dataset_utils import make_dataset


class SplitProcessor(AnalysisUtility):
    async def make_dataset_from_query(self, f, query):
        search = await self.search_database(query, retrieve="docids")
        docids = [
            {"id": d["id"], "type": "article", "relevancy": 1} for d in search["docs"]
        ]
        if docids:
            dataset_name = "_".join(
                [self.task.dataset.dataset_name, str(self.task.uuid), f]
            )
            make_dataset(dataset_name, "PRA", docids)
            return f, dataset_name

    async def make_datasets_from_queries(self, queries):
        subsets = {}
        for subset in asyncio.as_completed(
            [self.make_dataset_from_query(f, q) for f, q in queries.items()]
        ):
            subset = await subset
            if subset:
                subsets[subset[0]] = subset[1]
        return subsets


class SplitByFacet(SplitProcessor):
    @classmethod
    def _make_processor(cls):
        return Processor(
            name=cls.__name__,
            import_path=cls.__module__,
            description="Split dataset by a given facet",
            parameter_info=[
                {
                    "name": "facet",
                    "description": "facet used to split data, default LANGUAGE",
                    "type": "string",
                    "default": "LANGUAGE",
                    "required": False,
                    "values": ["LANGUAGE", "NEWSPAPER_NAME", "PUB_YEAR"],
                }
            ],
            input_type="facet_list",
            output_type="dataset_list",
        )

    async def get_input_data(self, previous_task_result):
        return previous_task_result.result[self.task.parameters["facet"]]

    async def make_result(self):
        facet_field = AVAILABLE_FACETS[self.task.parameters["facet"]]
        current_app.logger.debug("******FACET_FIELD: %s" % facet_field)

        search_query = self.task.search_query
        fq = search_query.get("fq", [])
        if isinstance(fq, str):
            fq = [fq]
        queries = {}
        for f in self.input_data:
            q = copy(search_query)
            q["fq"] = [*fq, "{}:{}".format(facet_field, f)]
            queries[f] = q

        if self.task.dataset:
            return await self.make_datasets_from_queries(queries)
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


class FindBestSplitFromTimeseries(SplitProcessor):
    @classmethod
    def _make_processor(cls):
        return Processor(
            name=cls.__name__,
            import_path=cls.__module__,
            description="Find the best time timeseries from result and the best (single) split within timeseries",
            parameter_info=[],
            input_type="timeseries",
            output_type="dataset_list",
        )

    async def get_input_data(self, previous_task_result):
        # choose most interesting timeseries
        max_interestingness = (None, 0)

        for k, v in previous_task_result.interestingness.items():
            if k == "overall":
                continue
            if v[0] > max_interestingness[1]:
                max_interestingness = (k, v[0])

        return {"data": previous_task_result.result, "key": max_interestingness[0]}

    async def make_result(self):
        # get a single, the most interesting timeseries
        data = self.input_data["data"].get("relative_counts") or self.input_data["data"]

        timeseries = {
            int(k): v
            for k, v in data[
                self.input_data["key"]  # key to choose the most interesting timeseries
            ].items()
            if k.isdigit()  # only choose years
        }
        # strip zeros in the beginning and end:
        for k, v in sorted(timeseries.items()):
            if v != 0:
                start = k
                break
        for k, v in sorted(timeseries.items(), reverse=True):
            if v != 0:
                end = k
                break

        timeseries = {
            k: v for k, v in sorted(timeseries.items()) if k >= start and k <= end
        }

        if len(timeseries) == 1:
            self.split_point = list(timeseries.keys())[0]
        else:
            # compute distances
            self.diffs = {
                k1: abs(timeseries[k1] - timeseries[k2])
                for k1, k2 in zip(
                    list(timeseries.keys())[:-1], list(timeseries.keys())[1:]
                )
            }
            # choose split point at max distance
            self.split_point = max(self.diffs, key=self.diffs.get)

        # now make new queries
        # current_app.logger.debug("INPUT_TASK: %s" %self.input_task)
        # current_app.logger.debug('self.input_task.parameters["facet_name"] %s' %self.input_task.parameters["facet_name"])

        search_query = self.task.search_query
        fq = search_query.get("fq", [])
        if isinstance(fq, str):
            fq = [fq]

        query1 = copy(search_query)
        query1["fq"] = [
            *fq,
            "date_created_dtsi:%s" % self.format_period(start, self.split_point),
        ]

        query2 = copy(search_query)
        query2["fq"] = [
            *fq,
            "date_created_dtsi:%s" % self.format_period(self.split_point + 1, end),
        ]

        #        if self.input_task.parameters.get("facet_name"):
        #            facet_field = AVAILABLE_FACETS[self.input_task.parameters["facet_name"]]
        #            facet_q = "{}:{}".format(facet_field, self.input_data["key"])
        #            query1["fq"].append(facet_q)
        #            query2["fq"].append(facet_q)

        result = {"query1": query1, "query2": query2}

        if self.task.dataset:
            return await self.make_datasets_from_queries(result)
        return result

    async def estimate_interestingness(self):
        if len(self.result) < 2:
            return {"overall": 0.0}
        data = self.input_data["data"].get("absolute_counts") or self.input_data["data"]
        total1 = sum(
            [
                v
                for k, v in data[self.input_data["key"]].items()
                if k.isdigit() and int(k) <= self.split_point
            ]
        )
        total2 = sum(
            [
                v
                for k, v in data[self.input_data["key"]].items()
                if k.isdigit() and int(k) > self.split_point
            ]
        )

        return {
            "query1": total1 / (total1 + total2),
            "query2": total2 / (total1 + total2),
        }

    async def _estimate_interestingness(self):
        interestingness = await self.estimate_interestingness()
        if "overall" in interestingness:
            return interestingness
        try:
            interestingness.update({"overall": max(self.diffs.values())})
        except AttributeError:
            interestingness.update({"overall": 0.0})
        return interestingness

    @staticmethod
    def format_period(start_year, end_year):
        # should it go to db utils?
        date_format = "%Y-%m-%dT%H:%M:%SZ"
        return "[%s TO %s]" % (
            datetime.datetime(start_year, 1, 1).strftime(date_format),
            datetime.datetime(end_year, 12, 31, 23, 59, 59).strftime(date_format),
        )


class Comparison(AnalysisUtility):
    @classmethod
    def _make_processor(cls):
        return Processor(
            name=cls.__name__,
            import_path=cls.__module__,
            description="Special type of the utility which takes as an input a list of tasks with the same input type and finds the difference.",
            parameter_info=[
                {
                    "name": "facet",
                    "description": "If compare by facet, can specify the facet",
                    "default": "PUB_YEAR",
                    "required": False,
                    "values": ["LANGUAGE", "NEWSPAPER_NAME", "PUB_YEAR"],
                }
            ],
            input_type="task_id_list",
            output_type="comparison",
        )

    async def get_input_data(self):
        tasks = Task.query.filter(
            Task.uuid.in_([t.uuid for t in self.task.parents])
        ).all()

        wait_time = 0
        while (
            any([task.task_status != "finished" for task in tasks]) and wait_time < 100
        ):
            asyncio.sleep(wait_time)
            wait_time += 1

        input_data_type = [task.processor.output_type for task in tasks]
        try:
            assert len(set(input_data_type)) == 1
        except AssertionError:
            raise BadRequest("All input tasks must have the same output types")
        self.data_type = input_data_type[0]

        return [task.task_result.result for task in tasks]

    async def make_result(self):
        dicts = [self.make_dict(data) for data in self.input_data]
        if len(dicts) > 2:
            raise NotImplementedError(
                "At the moment comparison of more than two results is not supported"
            )

        if isinstance(list(dicts[0].values())[0], dict):
            return {k:self.compare_dicts([dicts[0].get(k, {}), dicts[1].get(k, {})])
                    for k in set(list(dicts[0].keys())+list(dicts[1].keys()))}


        else:
            return self.compare_dicts(dicts)

    @staticmethod
    def compare_dicts(dicts):
        assessment.align_dicts(dicts[0], dicts[1], default_value=assessment.EPSILON)     
        return {
            "jensen_shannon_divergence": assessment.dict_js_divergence(
                dicts[0], dicts[1]
            ),
            "abs_diff": assessment.abs_diff(dicts[0], dicts[1]),
        }

    @staticmethod
    def estimate_interestingness(result):
        return {
            "abs_diff": assessment.recoursive_distribution(result["abs_diff"]),
            "jensen_shannon_divergence": result["jensen_shannon_divergence"],
        }

    async def _estimate_interestingness(self):
        if "jensen_shannon_divergence" in self.result:
            interestingness = self.estimate_interestingness(self.result)
            interestingness.update({"overall": self.result["jensen_shannon_divergence"]})
        else:
            #nested dict

            interestingness = {k:self.estimate_interestingness(v) for k,v in self.result.items()}
            interestingness.update({"overall":
                                    np.mean(
                                        [v["jensen_shannon_divergence"]
                                         for v in interestingness.values()])})
        return interestingness

    def make_dict(self, data):
        if self.data_type == "word_list":
            return self.make_ipm_dict(data)
        elif self.data_type == "bigram_list":
            return self.make_bigram_ipm_list(data)
        elif self.data_type == "facet_list":
            return self.make_facet_dict(data, self.task.parameters.get("facet"))
        elif self.data_type == "topic_analysis":
            return self.make_topic_dict(data)
        elif self.data_type == "timeseries":
            return self.make_timeseries_dict(data)
        else:
            current_app.logger.debug("DATA: %s" %data)
            raise NotImplementedError("Unknown data_type: %s" % self.data_type)

    @staticmethod
    def make_ipm_dict(word_list):
        return {k: v[1] for k, v in word_list["vocabulary"].items()}

    @staticmethod
    def make_bigram_ipm_list(bigram_list):
        return {k: v[1] for k, v in bigram_list.items()}

    @staticmethod
    def make_facet_dict(facet_list_output, facet):
        return facet_list_output[facet]

    @staticmethod
    def make_topic_dict(topic_analysis_output):
        return dict(enumerate(topic_analysis_output["topic_weights"]))

    @staticmethod
    def make_timeseries_dict(generate_timeseries_output):
        return generate_timeseries_output["absolute_counts"]
        

import asyncio
import requests
import json
from config import Config
from app.models import Processor
from app.analysis.processors import AnalysisUtility
from app.analysis import assessment
from werkzeug.exceptions import BadRequest, RequestTimeout
from flask import current_app
import random
from app.utils.dataset_utils import get_dataset
import numpy as np
from scipy.stats import entropy
import itertools


class TopicProcessor(AnalysisUtility):
    async def get_input_data(self):
        self.language = self.task.parameters.get("language")
        if not self.language:
            languages = await self.get_languages()
            self.language = max(languages, key=languages.get)
        else:
            self.language = self.language.lower()
        return await self.get_doc_topic_vectors(self.task.search_query, self.language)

    async def get_doc_topic_vectors(self, query, language):
        query["fl"] = "id, topics_fsim, language_ssi"
        res = await self.search_database(query)
        doc_ids = []
        topics = []
        
        for doc in res:
            if "topics_fsim" in doc and doc["language_ssi"] == language:
                doc_ids.append(doc["id"])
                topics.append(doc["topics_fsim"])
        
        return {
            "doc_ids": doc_ids,
            "topic_weights": list(np.mean(topics, axis=0)),
            "doc_weights": topics,
        }


class TopicModelDocumentLinking(TopicProcessor):
    @classmethod
    def _make_processor(cls):
        return Processor(
            name=cls.__name__,
            import_path=cls.__module__,
            description="Find similar documents using topic models",
            parameter_info=[
                {
                    "name": "num_docs",
                    "description": "Number of document IDs to return",
                    "type": "integer",
                    "default": 3,
                    "required": False,
                },
                {
                    "name": "language",
                    "description": "Language for which the model is needed. One topic model is build for one language; models are not aligned across languages. If language is not specified the majority language in the collection will be used. Documents in other languages will be ignored.",
                    "type": "string",
                    "values": ["FR", "DE", "FI"],
                    "default": None,
                    "required": False,
                },
                #                {
                #                    "name": "model_name",
                #                    "description": "The name of the topic model to use.",
                #                    "values": [
                #                        "LDA-FR",
                #                        "LDA-FI",
                #                        "LDA-DE",
                #                        "DTM-FR",
                #                        "DTM-FI",
                #                        "DTM-DE",
                #                    ],
                #                    "type": "string",
                #                    "default": None,
                #                    "required": True,
                #                },
            ],
            input_type="dataset",
            output_type="dataset",
        )

    async def make_result(self):
        payload = {
            "model_type": "lda",
            "lang": self.language,
            "num_docs": self.task.parameters.get("num_docs"),
            "topics_distrib": self.input_data["topic_weights"],
        }
        response = await self.request_result_from_tm(
            payload,
            "{}/doc-linking-by-distribution".format(Config.TOPIC_MODEL_URI),
            "{}/doc-linking-results".format(Config.TOPIC_MODEL_URI),
        )
        return response

    async def request_result_from_tm(
        self, payload, request_uri, result_uri, parameters={}, max_delay=False
    ):
        response = requests.post(request_uri, json=payload)
        uuid = response.json().get("task_uuid")
        if not uuid:
            raise ValueError(
                "Invalid response from the Topic Model API: {}".format(response)
            )

        response = await self.request_result_with_retry(
            result_uri, uuid, max_delay=max_delay, parameters=parameters
        )
        response["documents"] = response.pop("similar_docs")
        return response

    @staticmethod
    async def request_result_with_retry(
        uri, task_uuid, delay=4, max_delay=False, parameters={}
    ):
        delay = 4
        total_delay = 0
        while True:
            await asyncio.sleep(delay)
            total_delay += delay
            delay *= 1.2
            parameters["task_uuid"] = task_uuid
            response = requests.post(uri, json=parameters)
            if response.status_code == 200:
                return response.json()
            elif response.status_code != 202:
                return response
            current_app.logger.debug(
                "TM_TASK: %s DELAY: %s STATUS: %s"
                % (task_uuid, delay, response.status_code)
            )
            if max_delay and delay >= max_delay:
                raise RequestTimeout(
                    "Task {} cannot finish in {} seconds".format(task_uuid, total_delay)
                )

    async def estimate_interestingness(self):
        return {"documents": [1 - dist for dist in self.result["distance"]]}


class QueryTopicModel(TopicProcessor):
    @classmethod
    def _make_processor(cls):
        return Processor(
            name=cls.__name__,
            import_path=cls.__module__,
            description="Queries the selected topic model.",
            parameter_info=[
                {
                    "name": "language",
                    "description": "Language for which the model is needed. One topic model is build for one language; models are not aligned across languages. If language is not specified the majority language in the collection will be used. Documents in other languages will be ignored.",
                    "type": "string",
                    "values": ["FR", "DE", "FI"],
                    "default": None,
                    "required": False,
                },
            ],
            input_type="dataset",
            output_type="topic_analysis",
        )

    async def make_result(self):
        #current_app.logger.debug("INPUT_DATA: %s" % self.input_data)
        return self.input_data

    async def estimate_interestingness(self):
        # coefficients might change when we have more examples
        return {
            "topic_weights": assessment.find_large_numbers_from_lists(
                self.result["topic_weights"], coefficient=1.8
            ),
            "doc_weights": assessment.find_large_numbers_from_lists(
                self.result["doc_weights"], coefficient=2.5
            ),
        }


class TopicModelDocsetComparison(TopicProcessor):
    @classmethod
    def _make_processor(cls):
        return Processor(
            name=cls.__name__,
            import_path=cls.__module__,
            description="Compare datasets using topic_models. Takes two collection and comparison type",
            parameter_info=[
                {
                    "name": "collection1",
                    "description": "First collection to compare. Could be search or dataset",
                    "type": "dictionary",
                    "default": None,
                    "required": True,
                },
                {
                    "name": "collection2",
                    "description": "Second collection to compare. Could be search or dataset",
                    "type": "dictionary",
                    "default": None,
                    "required": True,
                },
                {
                    "name": "language",
                    "description": "Language of the documents. Only documents in this language will be used. Both collections should have at least one document in this language. Comparisons across languages is not supported",
                    "values": ["FR", "DE", "FI"],
                    "type": "string",
                    "default": None,
                    "required": True,
                },
                {
                    "name": "num_topics",
                    "description": "How many topics to return for comparison types 'shared_topics' and 'distinct_topics'. Default 3",
                    "type": "integer",
                    "default": 3,
                    "required": False,
                },
                # {
                #    "name": "model_type",
                #    "values": ["LDA", "DTM"],
                #    "description": "The type of the topic model to use",
                #    "type": "string",
                #    "default": "lda",
                #    "required": False,
                # },
            ],
            input_type="collection_pair",
            output_type="comparison",
        )

    async def get_input_data(self):
        collection1 = self.task.parameters.get("collection1")
        collection2 = self.task.parameters.get("collection2")
        language = self.task.parameters.get("language").lower()
        collections = await asyncio.gather(
            self.get_collection(collection1, language),
            self.get_collection(collection2, language),
        )

        for i in [0, 1]:
            if not collections[i]:
                raise BadRequest(
                    "Documents in language {} not found for collection {}".format(
                        language, i + 1
                    )
                )

        return collections

    async def get_collection(self, collection, language):
        # takes input collection and return a list of documents

        if "dataset" in collection:
            dataset = get_dataset(collection["dataset"])
            search_query = dataset.make_query()
        elif "search_query" in collection:
            search_query = collection["search_query"]
        else:
            raise BadRequest(
                "Collection could be a 'dataset' or a 'search_query'. \n{} is unsupported".format(
                    collection.keys()
                )
            )

        collection = await self.get_doc_topic_vectors(search_query, language)
        return collection

    async def make_result(self):
        return {
            "mean_jsd": np.round(
                self.compute_jsd(
                    self.input_data[0]["topic_weights"],
                    self.input_data[1]["topic_weights"],
                ),
                3,
            ),
            "internal_jsd1": np.round(
                self.compute_internal_jsd(self.input_data[0]["doc_weights"]), 3
            ),
            "internal_jsd2": np.round(
                self.compute_internal_jsd(self.input_data[1]["doc_weights"]), 3
            ),
            "cross_jsd": np.round(
                self.compute_cross_jsd(
                    self.input_data[0]["doc_weights"], self.input_data[1]["doc_weights"]
                ),
                3,
            ),
            "shared_topics": self.get_shared_topics(
                self.input_data[0]["topic_weights"], self.input_data[1]["topic_weights"]
            ),
            "distinct_topics1": self.get_distinct_topics(
                self.input_data[0]["topic_weights"], self.input_data[1]["topic_weights"]
            ),
            "distinct_topics2": self.get_distinct_topics(
                self.input_data[1]["topic_weights"], self.input_data[0]["topic_weights"]
            ),
        }

    def compute_jsd(self, list1, list2):
        p = np.array(list1)
        q = np.array(list2)
        m = (p + q) / 2
        return (entropy(p, m) + entropy(q, m)) / 2

    def compute_internal_jsd(self, vecs):
        vecs = np.array(vecs)
        divs = [
            self.compute_jsd(vecs[topic_pair[0]], vecs[topic_pair[1]])
            for topic_pair in itertools.combinations(range(vecs.shape[0]), 2)
        ]
        return np.mean(divs)

    def compute_cross_jsd(self, vecs1, vecs2):
        divs = [self.compute_jsd(v1, v2) for v1 in vecs1 for v2 in vecs2]
        return np.mean(divs)

    def get_shared_topics(self, vec1, vec2):
        mult_vec = np.multiply(np.array(vec1), np.array(vec2))
        top_shared = (-mult_vec).argsort()
        top_shared = top_shared[: self.task.parameters["num_topics"]]
        return [int(t) + 1 for t in top_shared]

    def get_distinct_topics(self, vec1, vec2):
        corpus0_topic_ranks = [t for t in np.argsort(-np.array(vec1))]
        corpus1_topic_ranks = [t for t in np.argsort(-np.array(vec2))]

        rank_difference = [
            corpus1_topic_ranks.index(corpus0_topic_ranks[i]) - i
            for i in range(len(corpus0_topic_ranks))
        ]
        num_topics = self.task.parameters["num_topics"]
        return [
            int(t) + 1
            for _, t in sorted(
                zip(
                    np.array(rank_difference[:num_topics]),
                    corpus0_topic_ranks[:num_topics],
                ),
                reverse=True,
            )
        ]

    async def estimate_interestingness(self):
        # jsd is already normalized between 0 and 1
        interestingness = {k: v for k, v in self.result.items() if isinstance(v, float)}

        # shared and distinct topics should not overlap. if they fo this means not enough information
        sh = self.result["shared_topics"]
        d1 = self.result["distinct_topics1"]
        d2 = self.result["distinct_topics2"]
        interestingness["shared_topics"] = (
            len(sh) - len(set.intersection(set(sh), set(d1 + d2)))
        ) / len(sh)
        interestingness["distinct_topics1"] = (
            len(d1) - len(set.intersection(set(sh), set(sh + d2)))
        ) / len(d1)
        interestingness["distinct_topics2"] = (
            len(d2) - len(set.intersection(set(sh), set(sh + d1)))
        ) / len(d2)

        return interestingness

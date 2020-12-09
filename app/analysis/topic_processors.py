import asyncio
import requests
import json
from config import Config
from app.models import Processor
from app.analysis.processors import AnalysisUtility
from app.utils.search_utils import search_database
from app.analysis import assessment
from werkzeug.exceptions import BadRequest, RequestTimeout
from flask import current_app
import random
from app.utils.dataset_utils import get_dataset


async def get_search_documents(search_query):
    search = await search_database(
        search_query, retrieve="docids", max_return_value=10000
    )  # Hack to avoid "payload too big" problem in TM API
    return search


class TopicProcessor(AnalysisUtility):
    async def get_input_data(self):
        if self.task.dataset:
            return [d.document.solr_id for d in self.task.dataset.documents]
        search = await get_search_documents(self.task.search_query)
        return [d["id"] for d in search["docs"]]

    async def request_result_from_tm(
        self, payload, request_uri, result_uri, parameters={}, max_delay=False
    ):
        response = requests.post(request_uri, json=payload)
        uuid = response.json().get("task_uuid")
        current_app.logger.debug(
            "PAYLOAD: %s RESPONSE: %s RESULT_UUID: %s" % (payload, response, uuid)
        )
        if not uuid:
            raise ValueError(
                "Invalid response from the Topic Model API: {}".format(response)
            )

        response = await self.request_result_with_retry(
            result_uri, uuid, max_delay=max_delay, parameters=parameters
        )
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
                    "name": "model_name",
                    "description": "The name of the topic model to use.",
                    "values":["LDA-FR", "LDA-FI", "LDA-DE", "DTM-FR", "DTM-FI", "DTM-DE"],
                    "type": "string",
                    "default": None,
                    "required": True,
                },
            ],
            input_type="dataset",
            output_type="dataset",
        )

    async def make_result(self):
        payload = {
            "model_name": self.task.parameters.get("model_name"),
            "num_docs": self.task.parameters.get("num_docs"),
            "documents": self.input_data,
        }
        response = await self.request_result_from_tm(
            payload,
            "{}/doc-linking".format(Config.TOPIC_MODEL_URI),
            "{}/doc-linking-results".format(Config.TOPIC_MODEL_URI),
        )

        return response

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
                    "name": "model_name",
                    "description": "The name of the topic model to use. If topic_name is specified, 'model_type' and 'language' are not used.",
                    "values":["LDA-FR", "LDA-FI", "LDA-DE", "DTM-FR", "DTM-FI", "DTM-DE"],
                    "type": "string",
                    "default": None,
                    "required": False,
                },
                {
                    "name": "model_type",
                    "description": "The type of the topic model to use",
                    "type": "string",
                    "values": ["LDA", "DTM"],
                    "default": None,
                    "required": False,
                },
                {
                    "name": "language",
                    "description": "Language for which the model is needed",
                    "type": "string",
                    "values": ["FR", "DE", "FI"],
                    "default": None,
                    "required": False,
                },
            ],
            input_type="dataset",
            output_type="topic_analysis",
        )

    # NOT USED: TM has only one lda and one dtm model per language
    async def find_model(self, language):
        # this is not relevant anymore
        # we will have only 1 model for each language/pair type
        available_models = []
        for model_type in Config.TOPIC_MODEL_TYPES:
            response = requests.get(
                "{}/{}/list-models".format(Config.TOPIC_MODEL_URI, model_type)
            )
            available_models += [
                (model_type, model["name"])
                for model in response.json()
                if model["lang"] == language
            ]
        # for now: random choice
        # later on: something more clever
        return random.choice(available_models)

    async def make_result(self):
        model_name = self.task.parameters.get("model_name")
        model_type = self.task.parameters.get("model_type")
        language = self.task.parameters.get("language")

        if model_name is None:
            if self.task.parameters.get("language") is None:
                raise KeyError
            elif self.task.parameters.get("model_type") is None:
                # TODO: random selection between lda and dtm
                model_type = "lda"
            else:
                model_name = (model_type + "_" + language).upper()

        payload = {
            "model_name": model_name,
            "documents": self.input_data,
        }

        response_data = await self.request_result_from_tm(
            payload,
            "{}/query-tm".format(Config.TOPIC_MODEL_URI),
            "{}/query-results".format(Config.TOPIC_MODEL_URI),
        )
        # If the lists are stored as strings, fix them into proper lists
        if isinstance(response_data["topic_weights"], str):
            response_data = {
                key: (json.loads(value) if isinstance(value, str) else value)
                for key, value in response_data.items()
            }
        response_data["model_name"] = self.task.parameters["model_name"]
        return response_data

    async def estimate_interestingness(self):
        """
        Example:
               {
               "topic_coherence": 0.0,
               "topic_weights": [0.06,0.1,0.09,0.02,0.1,0.11,0.01,0.11,0.11,0.29],
               "doc_weights": [[0.06,0.13,0.08,0.02,0.11,0.05,0.02,0.12,0.14,0.26],[0.07,0.09,0.08,0.01,0.07,0.19,0.01,0.08,0.09,0.31],[0.05,0.09,0.1,0.02,0.11,0.1,0.01,0.14,0.09,0.3]]
               }
        """
        # coefficients might change when we have more examples
        return {
            "topic_coherence": 0.0,
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
                    "description": "Language of the documents. Only documents in this language will be used.",
                    "values": ["FR", "DE", "FI"],
                    "type": "string",
                    "default": None,
                    "required": True,
                },
                {
                    "name": "num_topics",
                    "description": "How many topics to return for comparison types 'shared_topics' and 'distinct_topics'",
                    "type": "integer",
                    "default": None,
                    "required": False,
                },
                {
                    "name": "model_type",
                    "values": ["LDA", "DTM"],
                    "description": "The type of the topic model to use",
                    "type": "string",
                    "default": "lda",
                    "required": False,
                },
            ],
            input_type="collection_pair",
            output_type="comparison",
        )

    async def get_input_data(self):
        collection1 = self.task.parameters.get("collection1")
        collection2 = self.task.parameters.get("collection2")
        language = self.task.parameters.get("language")
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

    @staticmethod
    async def get_collection(collection, language):
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

        search = await get_search_documents(search_query)
        collection = [d["id"] for d in search["docs"] if d["language_ssi"] == language]

        return collection

    async def make_result(self):
        result = {}
        for attempt in range(3):

            comparisons = [
                self.query_tm_comparison(i[0], i[1], n, max_delay=150)
                for n, i in enumerate(Config.TOPIC_MODEL_COMPARISON_TYPE.items())
                if i[0] not in result and i[0] + "1" not in result
            ]

            current_app.logger.debug("COMPARISONS: %s" % comparisons)
            if not comparisons:
                # all done
                break
            # else try once again with (hopefully) less parallel tasks
            comparison_results = await asyncio.gather(
                *comparisons, return_exceptions=True
            )
            for res in comparison_results:
                if not isinstance(res, RequestTimeout):
                    result.update(res)

                    current_app.logger.debug(
                        "ATTEMPT: %d RESULT: %s" % (attempt, result)
                    )

        if not result:
            raise RequestTimeout("No results could be obtained in reasonable time")

        return result

    async def query_tm_comparison(
        self, comparison_type, accept_num_topics=False, postpone=0, max_delay=False
    ):
        await asyncio.sleep(2 * postpone)  # HACK to avoid task uuid collision

        model_type = self.task.parameters.get("model_type")
        payload = {
            "model_name": "-".join(
                [model_type, self.task.parameters.get("language")]
            ).upper(),
            "docs1": self.input_data[0],
            "docs2": self.input_data[1],
            "comparison_type": comparison_type,
        }
        if accept_num_topics and self.task.parameters.get("num_topics"):
            payload["num_topics"] = self.task.parameters.get("num_topics")

        response = await self.request_result_from_tm(
            payload,
            "{}/docset-comparison".format(Config.TOPIC_MODEL_URI),
            "{}/docset-results".format(Config.TOPIC_MODEL_URI),
            parameters={"compare_type": comparison_type},
            max_delay=max_delay,
        )

        return response

    async def _estimate_interestingness(self):
        # function with underscore computes 'overall' interestingness as well
        # jsd is already normalized between 0 and 1
        interestingness = {}
        numerical_results = []

        for k, v in self.result.items():
            if isinstance(v, float):
                interestingness[k] = v
                numerical_results.append(v)
            else:
                interestingness[k] = 1

        if "mean_jsd" in interestingness:
            interestingness["overall"] = interestingness["mean_jsd"]
        elif numerical_results:
            interestingness["overall"] = max(numerical_results)
        else:
            interestingness["overall"] = 0.5

        current_app.logger.debug("INTERESTINGNESS: %s" % interestingness)
        return interestingness

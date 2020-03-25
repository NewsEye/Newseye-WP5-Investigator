import asyncio
import requests
import json
from config import Config
from app.models import Processor
from app.analysis.processors import AnalysisUtility
from app.utils.search_utils import search_database
from app.analysis import assessment
from werkzeug.exceptions import NotFound
from flask import current_app


class TopicProcessor(AnalysisUtility):
    async def get_input_data(self):
        if self.task.dataset:
            return [d.document.solr_id for d in self.task.dataset.documents]
        search = await search_database(self.task.search_query, retrieve="docids")
        return [d['id'] for d in search['docs']]


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
                    "description": "The name of the topic model to use. If this is not specified, the system will use the first model offered by the topic modelling API.",
                    "type": "string",
                    "default": None,
                    "required": True,
                },

            ],
            input_type="dataset",
            output_type="dataset",
        )

    async def make_result(self):

        # current_app.logger.debug("PARAMETERS: %s" %self.task.parameters)
        
        payload = {"model_name": self.task.parameters.get("model_name"),
                   "num_docs": self.task.parameters.get("num_docs"),
                   "documents": self.input_data}
        # current_app.logger.debug("!!!PAYLOAD: %s" %payload)

        
        # ??? will we have any other way to link documents?
        response = requests.post(
            "{}/lda-doc-linking".format(Config.TOPIC_MODEL_URI), json=payload
        )
        uuid = response.json().get("task_uuid")

        if not uuid:
            raise ValueError("Invalid response from the Topic Model API")
        delay = 4
        while True:
            await asyncio.sleep(delay)
            delay *= 1.5
            response = requests.post(
                "{}/doc-linking-results".format(Config.TOPIC_MODEL_URI),
                json={"task_uuid": uuid},
            )
            current_app.logger.debug("DELAY: %s STATUS: %s" %(delay, response.status_code))
            if response.status_code == 200:
                break
        response = response.json()
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
                    "name": "model_type",
                    "description": "The type of the topic model to use",
                    "type": "string",
                    "default": None,
                    "is_required": True,
                },
                {
                    "name": "model_name",
                    "description": "The name of the topic model to use. If this is not specified, the system will use the first model offered by the topic modelling API.",
                    "type": "string",
                    "default": None,
                    "required": True,
                },
            ],
            input_type="dataset",
            output_type="topic_analysis",
        )

    async def make_result(self):
        model_type = self.task.parameters.get("model_type")
        if model_type is None:
            raise KeyError
        payload = {"model_name": self.task.parameters.get("model_name"),
                   "documents": self.input_data}
        # current_app.logger.debug("!!!PAYLOAD: %s" %payload)
        response = requests.post(
            "{}/{}/query".format(Config.TOPIC_MODEL_URI, model_type), json=payload
        )

        # current_app.logger.debug("RESPONSE: %s %s" %(response, response.__dict__))
        uuid = response.json().get("task_uuid")
        if not uuid:
            raise ValueError("Invalid response from the Topic Model API")
        delay = 4
        while True:
            await asyncio.sleep(delay)
            delay *= 1.5
            response = requests.post(
                "{}/query-results".format(Config.TOPIC_MODEL_URI),
                json={"task_uuid": uuid},
            )
            current_app.logger.debug("TASK_UUID: %s DELAY: %s STATUS: %s" %(uuid, delay, response.status_code))            
            if response.status_code == 200:
                break

        # current_app.logger.debug("RESPONSE: %s" %response)    
        response_data = response.json()
        #current_app.logger.debug("JSON: %s" %response_data)    
        # If the lists are stored as strings, fix them into proper lists
        if isinstance(response_data["topic_weights"], str):
            response_data = {
                key: (json.loads(value) if isinstance(value, str) else value)
                for key, value in response_data.items()
            }
        response_data["model_name"] = model_name
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

import asyncio
import requests
import json
from config import Config
from app.analysis.analysis_utils import AnalysisUtility
from app.analysis import assessment
from werkzeug.exceptions import NotFound


class QueryTopicModel(AnalysisUtility):
    def __init__(self):
        self.utility_name = 'query_topic_model'
        self.utility_description = 'Queries the selected topic model.'
        self.utility_parameters = [
            {
                'parameter_name': 'model_type',
                'parameter_description': 'The type of the topic model to use',
                'parameter_type': 'string',
                'parameter_default': None,
                'parameter_is_required': True,
            },
            {
                'parameter_name': 'model_name',
                'parameter_description': 'The name of the topic model to use. If this is not specified, the system will use the first model offered by the topic modelling API.',
                'parameter_type': 'string',
                'parameter_default': None,
                'parameter_is_required': False,
            },
        ]
        self.input_type = 'id_list'
        self.output_type = 'topic_analysis'
        super(QueryTopicModel, self).__init__()

    async def __call__(self, task):
        parameters = task.task_parameters.get('utility_parameters', {})
        model_type = parameters.get('model_type')
        if model_type is None:
            raise KeyError
        model_name = parameters.get('model_name')
        if model_name is None:
            available_models = self.request_topic_models(model_type)
            if available_models:
                model_name = available_models[0]['name']
            else:
                raise NotFound('No trained topic models exist for the selected model type.')
        input_task = self.get_input_task(task)
        payload = {
            'model': model_name,
            'documents': input_task.task_result.result['result']
        }
        response = requests.post('{}/{}/query'.format(Config.TOPIC_MODEL_URI, model_type), json=payload)
        uuid = response.json().get('task_uuid')
        if not uuid:
            raise ValueError('Invalid response from the Topic Model API')
        delay = 4
        while delay < 300:
            await asyncio.sleep(delay)
            delay *= 1.5
            response = requests.post('{}/query-results'.format(Config.TOPIC_MODEL_URI), json={'task_uuid': uuid})
            if response.status_code == 200:
                break
        response_data = response.json()
        # If the lists are stored as strings, fix them into proper lists
        if isinstance(response_data['topic_weights'], str):
            response_data = {key: (json.loads(value) if isinstance(value, str) else value) for key, value in response_data.items()}
        return {'result': response_data,
                'interestingness': self.estimate_interestingness(response_data),
                'model_name': model_name}

    @staticmethod
    def request_topic_models(model_type):
        response = requests.get('{}/{}/list-models'.format(Config.TOPIC_MODEL_URI, model_type))
        return response.json()

    @staticmethod
    def estimate_interestingness(response_json):
        """
        Example:
               {
               "topic_coherence": 0.0,
               "topic_weights": [0.06,0.1,0.09,0.02,0.1,0.11,0.01,0.11,0.11,0.29],
               "doc_weights": [[0.06,0.13,0.08,0.02,0.11,0.05,0.02,0.12,0.14,0.26],[0.07,0.09,0.08,0.01,0.07,0.19,0.01,0.08,0.09,0.31],[0.05,0.09,0.1,0.02,0.11,0.1,0.01,0.14,0.09,0.3]]
               }
        """
        # coefficients might change when we have more examples
        return {"topic_coherence": 0.0,
                "topic_weights":
                    assessment.find_large_numbers_from_lists(response_json["topic_weights"], coefficient=1.8),
                "doc_weights":
                    assessment.find_large_numbers_from_lists(response_json["doc_weights"], coefficient=2.5)}
import asyncio

import requests
from config import Config

from app.analysis.analysis_utils import AnalysisUtility

from app.analysis import assessment

import json

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
                'parameter_description': 'The name of the topic model to use',
                'parameter_type': 'string',
                'parameter_default': None,
                'parameter_is_required': False,
            },
        ]
        self.input_type = 'id_list'
        self.output_type = 'topic_analysis'
        super(QueryTopicModel, self).__init__()

    async def __call__(self, task):
        parameters = task.task_parameters['utility_parameters']
        model_type = parameters.get('model_type')
        if model_type is None:
            raise KeyError
        model_name = parameters.get('model_name')
        if model_name is None:
            available_models = self.request_topic_models(model_type)
            model_name = available_models[0]['name']
        input_task = self.get_input_task(task)
        payload = {
            'model': model_name,
            'documents': input_task.task_result.result['result']
        }
        response = requests.post('{}/{}/query'.format(Config.TOPIC_MODEL_URI, model_type), json=payload)
        uuid = response.json().get('task_uuid')
        if not uuid:
            raise ValueError('Invalid response from the Topic Model API')
        delay = 60
        while delay < 300:
            await asyncio.sleep(delay)
            delay *= 1.5
            response = requests.post('{}/query-results'.format(Config.TOPIC_MODEL_URI), json={'task_uuid': uuid})
            if response.status_code == 200:
                break
        return {'result': response.json(),
                'interestingness': estimate_interestness(response.json()),
                'model_name' : model_name}

    @staticmethod
    def request_topic_models(model_type):
        response = requests.get('{}/{}/list-models'.format(Config.TOPIC_MODEL_URI, model_type))
        return response.json()

    @staticmethod
    def estimate_interestness(response_json):
        """
        Example:
               {
               "topic_coherence": 0.0,
               "topic_weights": "[0.06,0.1,0.09,0.02,0.1,0.11,0.01,0.11,0.11,0.29]",
               "doc_weights": "[[0.06,0.13,0.08,0.02,0.11,0.05,0.02,0.12,0.14,0.26],[0.07,0.09,0.08,0.01,0.07,0.19,0.01,0.08,0.09,0.31],[0.05,0.09,0.1,0.02,0.11,0.1,0.01,0.14,0.09,0.3]]"
               }
        """
        # coefficients might change when we have more examples
        return {"topic_coherence": 0.0,
                "topic_weights" :
                assessment.find_large_numbers_from_lists(response_json["topic_weights"], coefficient=1.8),
                "doc_weights" :
                assessment.find_large_numbers_from_lists(response_json["doc_weights"], coefficient=2.5)}
                                                                                                  
        

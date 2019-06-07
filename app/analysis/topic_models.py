import asyncio

import requests
from config import Config

from app.analysis.analysis_utils import AnalysisUtility


class QueryTopicModel(AnalysisUtility):
    def __init__(self):
        super(QueryTopicModel, self).__init__()
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
                'interestingness': 0}

    @staticmethod
    def request_topic_models(model_type):
        response = requests.get('{}/{}/list-models'.format(Config.TOPIC_MODEL_URI, model_type))
        return response.json()

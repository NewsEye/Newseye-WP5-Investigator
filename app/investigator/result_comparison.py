from app.analysis.analysis_utils import AnalysisUtility
from flask import current_app
from app.models import Task, TaskInstance
import asyncio
from app.analysis import assessment

class ComparisonUtility(AnalysisUtility):

    def __init__(self):
        # TODO: input: result_id

        self.utility_name = 'comparison'
        self.utility_description = 'Special type of the utility which takes as an input a list of tasks with the same input type and finds difference. Assuming that the first result is comming from the corpus of the main interest and all the rest are omparison tasks. '
        self.utility_parameters = [
            {
                'parameter_name': 'task_ids',
                'parameter_description': 'The list of tasks with the same output type',
                'parameter_type': 'uuid_list',
                'parameter_default': [],
                'parameter_is_required': False
            },
            {
                'parameter_name': 'task_uuids',
                'parameter_description': 'The list of tasks with the same output type',
                'parameter_type': 'uuid_list',
                'parameter_default': [],
                'parameter_is_required': False
            }
        ]
        self.input_type = 'task_id_list'
        self.output_type = 'comparison'

        super(ComparisonUtility, self).__init__()
        
    async def get_input_data(self, task):
        if task.utility_parameters['task_ids']:
            # for calling from investigator
            # currently investigator checks that tasks are finnished
            # may cause problems in future?
            tasks = Task.query.filter(Task.id.in_(task.utility_parameters['task_ids'])).all()
        elif task.utility_parameters['task_uuids']:
            # calling directly from api
            tasks = TaskInstance.query.filter(TaskInstance.uuid.in_(task.utility_parameters['task_uuids'])).all()
            
            wait_time = 0
            while any([task.task_status != 'finished' for task in tasks]) and wait_time < 100:
                asyncio.sleep(wait_time)
                wait_time += 1

            tasks = [task.task for task in tasks]
        else:
            raise BadRequest('Request missing valid task_uuids or task_ids!')

        input_data_type = [task.output_type for task in tasks]
        assert(len(set(input_data_type))==1)
        input_data = [task.task_result.result for task in tasks]
        return input_data, input_data_type[0]
   
    async def __call__(self, task):
        self.input_data, self.data_type = await self.get_input_data(task)
        dicts = [self.make_dict(data) for data in self.input_data]
        if len(dicts) > 2:
            raise NotImplementedError("At the moment comparison of more than two results is not supported")
        assessment.align_dicts(dicts[0], dicts[1], default_value = assessment.EPSILON)
        fr = assessment.frequency_ratio(dicts[0], dicts[1])
        fr = {k:fr[k] for k in sorted(fr, key=fr.get, reverse=True)} 
        js_divergence = assessment.dict_js_divergence(dicts[0], dicts[1])
        return {'result': {'frequency_ratio':fr,
                           'jensen_shannon_divergence':js_divergence},
                'interestingness' : {'fr':{k:(lambda x: 1.0 if x > 1 else 0)(v) for k,v in fr.items()},
                                     'jensen_shannon_divergence':js_divergence}}        
        
        
    def make_dict(self, data):
        if self.data_type == 'tf_idf':
            return self.make_ipm_dict(data)
        elif self.data_type == 'facet_list':
            return self.make_facet_dict(data)
        elif self.data_type == 'topic_analysis':
            return self.make_topic_dict(data)
        elif self.data_type == 'step_list':
            return self.make_step_list(data)
        else:
            raise NotImplementedError("Unknown data_type: %s" %self.data_type)

    @staticmethod
    def make_ipm_dict(tf_idf_output):
        return {k:v['ipm'] for k,v in tf_idf_output.items()}

    @staticmethod
    def make_facet_dict(facet_list_output):
        facet_dict = {f['facet_value']:f['document_count'] for f in facet_list_output}
        total = float(sum(facet_dict.values()))
        return {k:v/total for k,v in facet_dict.items()}

    @staticmethod
    def make_topic_dict(topic_analysis_output):
        return dict(enumerate(topic_analysis_output['topic_weights']))

    @staticmethod
    def make_step_list(step_detection_output):
        # assuming only one column
        # assuming column is the same        
        return {s['step_time']:s['step_error'] for s in step_detection_output[0]['steps']}  # ???

    
    
    
def estimate_interestingness(subtask):
    return subtask.task_result.interestingness['jensen_shannon_divergence']

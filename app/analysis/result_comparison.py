from app.analysis.analysis_utils import AnalysisUtility
from flask import current_app
from app.models import Task
import asyncio

class ComparisonUtility(AnalysisUtility):

    def __init__(self):
        # TODO: input: result_id

        self.utility_name = 'comparison'
        self.utility_description = 'Special type of the utility which taks as an input a list of tasks with the same input type and finds difference'
        self.utility_parameters = [
            {
                'parameter_name': 'task_ids',
                'parameter_description': 'The list of tasks with the same output type',
                'parameter_type': 'uuid_list',
                'parameter_default': [],
                'parameter_is_required': True
            }
        ]
        self.input_type = 'task_id_list'
        self.output_type = 'comparison'

        super(ComparisonUtility, self).__init__()
        
    async def get_input_data(self, task):
        tasks = Task.query.filter(Task.id.in_(task.utility_parameters['task_ids'])).all()
        input_data_type = [task.output_type for task in tasks]
        assert(len(set(input_data_type))==1)
        input_data = [task.task_result.result for task in tasks]
        return input_data, input_data_type[0]
   
    async def __call__(self, task):
        self.input_data, self.data_type = await self.get_input_data(task)
        current_app.logger.debug("in call DATA_TYPE: %s" %self.data_type)        
        current_app.logger.debug(self.input_data)        
    

def estimate_interestingness(interestingness):
    return 0.0

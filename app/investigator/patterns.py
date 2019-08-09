from app.analysis.topic_models import QueryTopicModel
import asyncio
from flask import current_app
import numpy as np

    
def mask_times_data(mask, data):
    if isinstance(data, dict):
        ret = {}
        return {k:mask_times_data(mask[k], data[k]) for k in mask}
    elif isinstance(data, float) or isinstance(data, int):
        return mask*data
    elif isinstance(data, list):
        return [mask_times_data(m,d) for m,d in zip(mask, data)]
        
                
            
    
    
class InvestigationPattern(object):
    '''
    Ensures pattern execution together with all its prerequisites.
    Estimates interestingness of subtasks (based on the interestingness estimated by utilities).
    Stores result as a separate task record and as a global result for the main task.
   '''  
 

    def __init__(self, runner, main_task, search_query=None):
        # global variables, shared across patterns
        # all patterns update result and interestingness 

        self.main_task = main_task
        # document dependent
        self.search_query = search_query if search_query else self.main_task.search_query
        # pattern dependent
        self.prerequisite_utilities = []
        self.parameters = {}
        self.utility_name = None
        self.runner = runner
    
    async def __call__(self):
        # run 1-2 prerequisits, needed to define core pattern tasks
        # e.g. document language is essential to call topic models
        await self.run_prerequisite_utilities()
        # update parameters
        # e.g. topic model name depending on the document language(s)
        await self.update_parameters()
        # generate subtasks  --- generate core pattern tasks
        subtasks = await self.generate_subtasks()
        # run subtasks in parallel, estimate interestingness, store
        await self.runner.run_subtasks_and_update_results(subtasks, self.estimate_interestingness,
                                                          reference={"pattern":type(self).__name__})

        return subtasks
    
    async def run_prerequisite_utilities(self):
        self.prerequisite_tasks = self.runner.generate_investigation_tasks(self.prerequisite_utilities, search_query=self.search_query)
        await self.runner.run_subtasks_and_update_results(self.prerequisite_tasks, self.estimate_interestingness,
                                                          reference={"pattern":type(self).__name__})
                                                          
        
    # TODO: need to think out what is interesting and what is not
    def estimate_interestingness(self, subtask):
        # pattern dependent
        return subtask.task_result.interestingness

    async def update_parameters(self):
        # pattern dependent
        pass

    async def generate_subtasks(self):
        return self.runner.generate_investigation_tasks([(self.utility_name, self.parameters)], search_query=self.search_query)   
    
class BasicStats(InvestigationPattern):

    def __init__(self, *args, **kwargs):
        super(BasicStats, self).__init__(*args, **kwargs)
        self.utility_name = 'compute_tf_idf'
        self.prerequisite_utilities = [('common_facet_values', {'facet_name':'LANGUAGE'})]

class Facets(InvestigationPattern):
    def __init__(self, *args, **kwargs):
        super(Facets, self).__init__(*args, **kwargs)
        self.prerequisite_utilities = [('extract_facets', {})]
        self.utility_name = 'common_facet_values'
        self.parameters['n'] = 0

    async def update_parameters(self):
        prerequisite_task = self.prerequisite_tasks[0]
        self.parameters['facet_names'] = prerequisite_task.task_result.result.keys()
    
    async def generate_subtasks(self):
        target_utilities = [('common_facet_values', {'facet_name':facet_name, 'n':self.parameters['n']}) 
                            for facet_name in self.parameters['facet_names']]
        # TODO: more than one prerequisite_task?
        return self.runner.generate_investigation_tasks(target_utilities, source_uuid=self.prerequisite_tasks[0].uuid, search_query=self.search_query)

    def estimate_interestingness(self, subtask):
        if subtask.utility == 'extract_facets':
            # preliminary task, we are not really intertersted in result
            return 0.0
        # else default
        return super(Facets, self).estimate_interestingness(subtask)
        
    
class Topics(InvestigationPattern):
    def __init__(self, *args, **kwargs):
        super(Topics, self).__init__(*args, **kwargs)
        self.prerequisite_utilities = [('common_facet_values', {'facet_name':'LANGUAGE'})]
        self.utility_name = 'query_topic_model'

    async def update_parameters(self):
        prerequisite_task = self.prerequisite_tasks[0]

        language_list = [d['facet_value'] for d in prerequisite_task.task_result.result]

        if len(language_list) > 1:
            current_app.logger.error("More than one language in a corpus %s" %prerequisite_task.search_query)
            raise NotImplementedError("More than one language in a corpus")
        lang = language_list[0]
        
        # TODO: model_type selection (currently only one is available)
        # TODO: default model_type in parameters, similar to analysis utils
        model_type = 'lda'

        available_models = QueryTopicModel.request_topic_models(model_type)
        available_names = []
        
        for model in available_models:
            if model['lang'] == lang:
                available_names.append(model['name'])

        if len(available_names) == 1:
            model_name = available_names[0]
        elif len(available_names) > 1:
            current_app.logger.error("More than one model for language %s: %s" %(lang, available_models))
            raise NotImplementedError("More than one model")
        else:
            current_app.logger.error("Cannot find model for language %s: %s" %(lang, available_models))
            raise NotFound('No trained topic models exist for the selected model type and language.')

        self.parameters = {'model_type':model_type, 'model_name':model_name}


    def estimate_interestingness(self, subtask):
        if subtask.utility == 'common_facet_values':
            # preliminary task, we are not really intertersted in result
            return 0.0

        outcome = subtask.result_with_interestingness
        result, interestingness = outcome['result'], outcome['interestingness']
        # here interestingness is a [0-1] mask
        interestingness = mask_times_data(interestingness, result)
        return interestingness
        
        
class DocumentLinkingTM(InvestigationPattern):
    def __init__(self, *args, **kwargs):
        super(DocumentLinkingTM, self).__init__(*args)
        self.utility_name = 'tm_document_linking'
        self.parameters = {'num_docs':10}



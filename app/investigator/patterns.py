from app.analysis.topic_models import QueryTopicModel
from app.main.db_utils import generate_tasks,  store_results
import asyncio
from flask import current_app
import numpy as np

def max_interestingness(interestingness):
    if not interestingness:
        return 0.0
    if isinstance(interestingness, float):
        return interestingness
    elif isinstance(interestingness, list):
        return max([max_interestingness(i) for i in interestingness])
    elif isinstance(interestingness, dict):
        return max_interestingness([max_interestingness(i) for i in interestingness.values()])
    else:
        return interestingness


    
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
 

    def __init__(self, planner, main_task, task_result, interestingness, search_query=None):
        # global variables, shared across patterns
        # all patterns update result and interestingness 
        self.planner = planner
        self.user = planner.user
        self.main_task = main_task
        self.task_result = task_result
        self.interestingness = interestingness
        # document dependent
        self.search_query = search_query if search_query else self.main_task.search_query
        # pattern dependent
        self.prerequisite_utilities = []
        self.parameters = {}
        self.utility_name = None
    
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
        await self.run_subtasks_and_update_results(subtasks)
        return subtasks
    
    async def run_prerequisite_utilities(self):
        self.prerequisite_tasks = self.generate_investigation_tasks(self.prerequisite_utilities)
        await self.run_subtasks_and_update_results(self.prerequisite_tasks)


    async def run_subtasks_and_update_results(self, subtasks):
        """ 
        Generates and runs may tasks in parallel, assesses results and generate new tasks if needed.
        Stores data in the database as soon as they ready

               1. gets list of subtasks
               2. runs in parallel, store in db as soon as ready
               3. result of the main task is a list of task uuid (children tasks) + interestness

         """
        
        for subtask in asyncio.as_completed([self.execute_and_store(s) for s in subtasks]):
            done_subtask = await subtask
            subtask_interestingness = max_interestingness(self.estimate_interestingness(done_subtask))
            if subtask_interestingness > self.interestingness:
                self.interestingness = subtask_interestingness
            if not str(done_subtask.uuid) in self.task_result:
                self.task_result[str(done_subtask.uuid)] = {"utility_name" : done_subtask.utility,
                                                            "utility_parameters" : done_subtask.utility_parameters,
                                                            "search_query" : done_subtask.search_query,
                                                            "interestingness" : subtask_interestingness}
            
            store_results([self.main_task], [self.task_result],
                          set_to_finished=False, interestingness=self.interestingness)

        

    def generate_investigation_tasks(self, utilities, source_uuid=None):
        return generate_tasks(user=self.user,
                                  queries = [('analysis',
                                              {'source_uuid' : source_uuid,
                                               'search_query' : self.search_query,
                                               'utility' : u,
                                               'utility_parameters' : params,
                                               'force_refresh' : self.main_task.force_refresh})
                                             for u,params in utilities],
                                  parent_id=self.main_task.uuid,
                                  return_tasks=True)


    
    # TODO: need to think out what is interesting and what is not
    def estimate_interestingness(self, subtask):
        # pattern dependent
        return subtask.task_result.interestingness

    async def update_parameters(self):
        # pattern dependent
        pass

    async def generate_subtasks(self):
        return self.generate_investigation_tasks([(self.utility_name, self.parameters)])          
    
    async def execute_and_store(self, subtask):
        if subtask.force_refresh:
            for uuid, done_task in self.task_result.items():
                if (done_task['utility_name'] == subtask.utility and
                    done_task['utility_parameters'] == subtask.utility_parameters and
                    done_task['search_query'] == subtask.search_query
                    ):
                    # don't repeat task even if force_refresh (inherited from the main task) is True
                    # ---they are refreshed already in this investigation loop, nothing should change in between
                    # this way we can define patterns independently, without
                    # worrying if some utils are repeated many times across patterns
                    # NOTE: this does not work if patterns are different but have the same meaning,
                    # e.g. LANGUAGE = language_ssi for common_facet_values
                    subtask.force_refresh = False
                    break
        return await self.planner.execute_and_store(subtask)

 
    
    
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

    async def update_parameters(self):
        prerequisite_task = self.prerequisite_tasks[0]
        self.parameters['facet_names'] = prerequisite_task.task_result.result.keys()
    
    async def generate_subtasks(self):
        target_utilities = [('common_facet_values', {'facet_name':facet_name}) 
                            for facet_name in self.parameters['facet_names']]
        # TODO: more than one prerequisite_task?
        return self.generate_investigation_tasks(target_utilities, source_uuid=self.prerequisite_tasks[0].uuid)

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
        
    



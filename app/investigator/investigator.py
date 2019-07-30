from app.main.db_utils import generate_tasks, store_results
import asyncio
from flask import current_app
from app.analysis.topic_models import QueryTopicModel 
from config import Config

def max_interestingness(interestingness):
    if isinstance(interestingness, float):        
        return interestingness
    elif isinstance(interestingness, list):
        return max([max_interestingness(i) for i in interestingness])
    elif isinstance(interestingness, dict):
        return max([max_interestingness(i) for i in interestingness.values()])
    else:
        return interestingness


class Investigator(object):
    
    def __init__(self, planner, task):
       self.planner = planner
       self.main_task = task
       self.force_refresh = task.force_refresh
       self.task_result = {}
       self.interestingness = 0.0

    async def investigate(self):
        '''
           1st pattern: extract facets, run in parallel
           2nd pattern: extract languages, run topics
        '''

        subtasks = await asyncio.gather(self.generate_facet_subtasks(),
                                        self.generate_basic_stats(),
                                        self.generate_topic_tasks())
                                        
        subtasks = [task for tasklist in subtasks for task in tasklist]
        
        await self.run_subtasks_and_update_results(subtasks)


    async def generate_topic_tasks(self):
        prerequisite_utility = [('common_facet_values', {'facet_name':'LANGUAGE'})]
        prerequisite_task = self.generate_investigation_tasks(prerequisite_utility)
        prerequisite_task = prerequisite_task[0]

        language_list = [d['facet_value'] for d in prerequisite_task.task_result.result]

        if len(language_list) > 1:
            current_app.logger.error("More than one language in a corpus %s" %prerequisite_task.search_query)
            raise NotImplementedError("More than one language in a corpus")
        lang = language_list[0]
        
        # TODO: model_type selection (currently only one is available)
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
            

        return self.generate_investigation_tasks([('query_topic_model', {'model_type' : model_type,
                                                                         'model_name' :model_name})])                                                                      
        
        
        
    async def generate_basic_stats(self):
        return self.generate_investigation_tasks([('compute_tf_idf', {})])

    async def generate_facet_subtasks(self):
        prerequisite_utility = [('extract_facets', {})]
        prerequisite_task = self.generate_investigation_tasks(prerequisite_utility)
        await self.run_subtasks_and_update_results(prerequisite_task)
        prerequisite_task = prerequisite_task[0]

        facet_names = prerequisite_task.task_result.result.keys()
    
        target_utilities = [('common_facet_values', {'facet_name':facet_name}) 
                            for facet_name in facet_names]
        return self.generate_investigation_tasks(target_utilities, source_uuid=prerequisite_task.uuid)
        
    def generate_investigation_tasks(self, utilities, source_uuid=None):
        return generate_tasks(user=self.planner.user,
                                  queries = [('analysis',
                                              {'source_uuid' : source_uuid,
                                               'search_query' : self.main_task.search_query,
                                               'utility' : u,
                                               'utility_parameters' : params,
                                               'force_refresh' : self.force_refresh})
                                             for u,params in utilities],
                                  parent_id=self.main_task.uuid,
                                  return_tasks=True)
        
    async def run_subtasks_and_update_results(self, subtasks):
        """ Generate and runs may tasks in parallel, assesses results and generate new tasks if needed.
               Stores data in the database as soon as they ready

               1. gets list of subtasks
               2. runs in parallel, store in db as soon as ready
               3. result of the main task is a list of task uuid (children tasks) + interestness

         """
        
        for subtask in asyncio.as_completed([self.execute_and_store(s) for s in subtasks]):
            done_subtask = await subtask
            # the subtask result is already stored, now we have to add subtask into list of task results
            subtask_interestingness = max_interestingness(done_subtask.task_result.interestingness)
            if subtask_interestingness > self.interestingness:
                self.interestingness = subtask_interestingness
            if not str(done_subtask.uuid) in self.task_result:
                self.task_result[str(done_subtask.uuid)] = {"utility_name" : done_subtask.utility,
                                                            "utility_parameters" : done_subtask.utility_parameters,
                                                            "interestingness" : subtask_interestingness}
            current_app.logger.debug("TASK_RESULT: %s" %self.task_result)
            store_results([self.main_task], [self.task_result],
                          set_to_finished=False, interestingness=self.interestingness)
            


    async def execute_and_store(self, subtask):
        if subtask.force_refresh:
            for uuid, done_task in self.task_result.items():
                if (done_task['utility_name'] == subtask.utility and
                    done_task['utility_parameters'] == subtask.utility_parameters):
                    # don't repeat task even if force_refresh
                    # (inherited from the main task) is True---they
                    # are refreshed already in this investigation
                    # loop, nothing should change in between this way
                    # we can define patterns independently, without
                    # working if some utils are repeated and refreshed
                    subtask.force_refresh = False
                    break
        # if task not found, run it
        return await self.planner.execute_and_store(subtask)
        
        

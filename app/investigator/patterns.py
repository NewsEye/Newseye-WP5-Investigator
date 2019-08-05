from app.analysis.topic_models import QueryTopicModel
from app.main.db_utils import generate_tasks

class InvestigationPattern(object):

    def __init__(self, user, main_task, investigator):
        self.user = user
        self.main_task = main_task
        self.investigator = investigator
        self.prerequisite_utilities = []
        self.parameters = {}
        self.utility_name = None
    
    async def __call__(self):
        await self.run_prerequisite_utilities()
        await self.update_parameters()
        return await self.generate_subtasks()

        
    async def run_prerequisite_utilities(self):
        self.prerequisite_tasks = self.generate_investigation_tasks(self.prerequisite_utilities)
        await self.investigator.run_subtasks_and_update_results(self.prerequisite_tasks)
        

    def generate_investigation_tasks(self, utilities, source_uuid=None):
        return generate_tasks(user=self.user,
                                  queries = [('analysis',
                                              {'source_uuid' : source_uuid,
                                               'search_query' : self.main_task.search_query,
                                               'utility' : u,
                                               'utility_parameters' : params,
                                               'force_refresh' : self.main_task.force_refresh})
                                             for u,params in utilities],
                                  parent_id=self.main_task.uuid,
                                  return_tasks=True)

    async def update_parameters(self):
        pass

    async def generate_subtasks(self):
        return self.generate_investigation_tasks([(self.utility_name, self.parameters)])       


    
class BasicStats(InvestigationPattern):
    def __init__(self, *args):
        super(BasicStats, self).__init__(*args)
        self.utility_name = 'compute_tf_idf'
    


class Facets(InvestigationPattern):
    def __init__(self, *args):
        super(Facets, self).__init__(*args)
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


class Topics(InvestigationPattern):
    def __init__(self, *args):
        super(Topics, self).__init__(*args)
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


        
        



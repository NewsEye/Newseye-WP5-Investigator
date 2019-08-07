from app.main.db_utils import store_results
import asyncio
from flask import current_app
from config import Config
from app.investigator import ANALYSING_PATTERNS, LINKING_PATTERNS
from app.analysis import UTILITY_MAP


class Investigator(object):
    
    def __init__(self, planner, task):
       self.planner = planner
       self.main_task = task
       self.force_refresh = task.force_refresh
       self.task_result = {}
       self.interestingness = 0.0


    # TODO: make recoursive function for infinite investigation loop
    async def investigate(self):
        linked_docs_analysing_tasks = []
        
        for pattern_set in asyncio.as_completed([self.run_pattern_set(ps)
                                                 for ps in [ANALYSING_PATTERNS, LINKING_PATTERNS]]):
            subtasks = await pattern_set
            for subtask in subtasks:
                if UTILITY_MAP[subtask.utility].output_type == 'id_list_with_dist':
                    # TODO: start new investigator here
                    # TODO for now: make search_query and run analysing_patterns
                    
                    query = self.make_search_query_from_linked_documents(subtask)
                    current_app.logger.debug("QUERY: %s" %query)
                    if query:
                        linked_docs_analysing_tasks.append(asyncio.create_task(
                            self.run_pattern_set(ANALYSING_PATTERNS, search_query=query)))

        pending_tasks = [task for task in linked_docs_analysing_tasks if not task.done()]
        await asyncio.gather(*pending_tasks)                        

    async def run_pattern_set(self, pattern_set, search_query=None):

        patterns = [Pattern(self.planner, self.main_task,
                            self.task_result, self.interestingness,
                            search_query = search_query)
                    for Pattern in pattern_set]

        current_app.logger.debug("PATTERNS %s SEARCH_QUERY %s" %(patterns, search_query))
        # each pattern returns a list of subtasks hence patternset returns list of lists
        subtasks = await asyncio.gather(*[pattern() for pattern in patterns], return_exceptions=False)
        return [s for sl in subtasks for s in sl]
        

    def make_search_query_from_linked_documents(self, task):
        document_list = task.task_result.result.get('similar_docs', None)
        if document_list:
            current_app.logger.debug("main_task.search_query %s" %self.main_task.search_query)
            return {'q' : ' '.join([docid for docid in document_list]),
                    'mm':1,
                    # qf (query field) preserves language fo the original search
                    # TODO: general solution to manage languages across utils
                    'qf':'id ' + self.main_task.search_query['qf']}


        
        

import heapq
import itertools
from app import db
from app.utils.db_utils import generate_task, generate_investigator_node
from app.models import Task, Processor, InvestigatorRun, InvestigatorAction, InvestigatorResult
from copy import copy
from app.investigator import processorsets
from flask import current_app
import asyncio


class Investigator:
    def __init__(self, run_uuid, planner):
        # planner, which executes tasks
        self.planner = planner
        self.user = self.planner.user
        # database record which should be updated in all operations
        self.run = InvestigatorRun.query.filter_by(uuid=run_uuid).one()

        current_app.logger.debug("RUN: %s" %self.run)

        self.root_documentset = Documentset(self.run, self.user)
        self.action_id = 0
        self.node_id = 0
        self.to_stop = False
        self.task_queue = TaskQueue()
        self.done_tasks = []

    @property
    def queue_state(self):
        return self.task_queue.queue_state()

    async def initialize_run(self, continue_from_node=None):
        """
        run initialization:
        """
        if continue_from_node:
            # continue investigations from a given node
            raise NotImplementedError
        else:
            self.run.root_action_id = self.action_id
        
        # for now: always start with description
        # later on processorset could be infered from user parameters
        await self.action(self.initialize, processorset = "DESCRIPTION")
        
    async def act(self):
        """
        main function 
        running investigator actions
        """

        self.update_status("running")
        
        while not self.to_stop:

            # variables needed to pass information between actions within this step
            self.selected_tasks = None
            self.executed_tasks = None
            self.start_action = self.action_id
            self.nodes = self.run.nodes

            # investigator actions
            await self.action(self.select)
            await self.action(self.execute)
            await self.action(self.report)
            await self.action(self.update)           

            self.check_for_stop()
            
        await self.action(self.stop)
        await self.action(self.report, final=True)
        

    
    # ACTIONS
    # recorded in DB for Explainer    
    async def action(self, action_func, **action_parameters):
        input_q = self.queue_state
        
        why, action = await action_func(**action_parameters)

        db_action = InvestigatorAction(run_id=self.run.id,
                                       action_id=self.action_id,
                                       action_type=action_func.__name__,
                                       why=why,
                                       action=action,
                                       input_queue=input_q,
                                       output_queue=self.queue_state
                                   )

        current_app.logger.debug("DB_ACTION: %s" %db_action)
        current_app.logger.debug("NODES: %s" %self.run.nodes)
        
        db.session.add(db_action)
        db.session.commit()  # this also stores changes made inside actions (e.g. execute, report)
            
        self.action_id += 1
    
    async def initialize(self, processorset):
        """
        task queue initialization
        """
        tasks = self.make_tasks(processorsets[processorset], self.root_documentset)
        self.task_queue.add_tasks(tasks)
        why = {"processorset" : processorset}
        action = {"tasks_added_to_q" : self.task_list(tasks)}
        return why, action

    async def select(self):
        """
        task selection from queue
        """
   
        tasks = self.task_queue.pop_tasks_with_lowest_priority()
        self.selected_tasks = tasks
        why = {"priority":"lowest"}
        action = {"selected_tasks":self.task_list(tasks)}
        return why, action

    async def execute(self):
        """
        task execution
        """
        tasks = self.selected_tasks
        await self.planner.execute_and_store_tasks(tasks)
        current_app.logger.debug("TASKS %s" %tasks)

        # append to previously done tasks
        self.done_tasks += self.task_list(tasks)
        self.run.done_tasks = self.done_tasks
        
        self.executed_tasks = [t for t in tasks if t.task_status=="finished"] # maybe "failed"
        why = {"status":"finished"}
        action = {"execute_tasks":self.task_list(self.executed_tasks)}
        
        return why, action

    async def report(self, final=False):
        """
        collects tasks that should be reported so far
        reports should be available at every stage
        """
        if final:
            # results are already combined in the main loop, 
            # (when this function is called with final=False)
            # nothing to do for now
            # in the future: final decision on what is the most interesting for the user
            why = {'dev_note':'not implemented'}
            action = {}
        
        else:
            previous_results = self.run.result
            new_results = self.task_list(self.executed_tasks)
            interestingness = self.estimate_node_interestingness(new_results)
            
            why, combined_results = self.combine_results(previous_results, new_results)
            action = combined_results

            # replace previous results
            self.run.result = combined_results

            # save results for a single "node" --- a set of actions that could be shown to a user via demonstrator
            node=generate_investigator_node(self.run, self.start_action, self.action_id,
                                            self.sort_by_interestingness(new_results), interestingness,
                                            self.user)
            self.nodes += [{"uuid":str(node.uuid),
                            "interestingness":interestingness}]
            self.run.nodes = self.nodes
            self.node_id += 1

            
        return why, action
        
    
    async def update(self):
        """
        update task queue
        """
        # dev_note means temporal placeholder, which should not be used by explainer:
        why = {"dev_note":"not implemented"}
        action = {}
        return why, action

    async def stop(self):
        """
        stop investigations
        """
        self.update_status("finished")
        why = self.to_stop
        action = {}
        return why, action

    # HELPERS

    def check_for_stop(self):
        if self.run.user_parameters["describe"]:
            self.to_stop = {"user_parameters":"describe"}
        elif self.task_queue.taskq == []:
            self.to_stop = {"taskq":"empty"}
        return self.to_stop

    def update_status(self, status):
        self.run.run_status=status
        db.session.commit()

    def make_tasks(self, processorset, documentset):
        # TODO: processor parameters
        return [documentset.make_task(processor_name) for processor_name in processorset]

    def estimate_node_interestingness(self, results):
        # self is currently not used but might be useful to estimate interestingness
        # for now: maximum of existing results
        return max([result["interestingness"] for result in results])
                                            
    def combine_results(self, *results):
        # for now: just add everything
        # self is currently not used 
        # later on: some selective process based on result interestingness      
        why = {"dev_note":"not implemented; all results combined unselectevely"}
        combined_result = self.sort_by_interestingness(sum(results, []))
        return why, combined_result  

    @staticmethod
    def sort_by_interestingness(results):
        return sorted(results, key=lambda r: r["interestingness"], reverse=True)                                    
                                            
    @staticmethod
    def task_list(tasks):       
        return [task.dict(style="investigator") for task in tasks]
    
class TaskQueue:
    def __init__(self):
        self.taskq = []                      # list of entries arranged in a heap
        self.entry_finder = {}               # mapping of tasks to entries
        self.REMOVED = '<removed-task>'      # placeholder for a removed task
        self.counter = itertools.count()     # unique sequence count

    def add_tasks(self, tasks, priority=0):
        for t in tasks:
            self.add_task(t, priority=priority)
        
    def add_task(self, task, priority=0):
        'Add a new task or update the priority of an existing task'
        if task in self.entry_finder:
            self.remove_task(task)
        count = next(self.counter)
        entry = [priority, count, task]
        self.entry_finder[task] = entry
        heapq.heappush(self.taskq, entry)

    def remove_task(self, task):
        'Mark an existing task as REMOVED.  Raise KeyError if not found.'
        entry = self.entry_finder.pop(task)
        entry[-1] = self.REMOVED

    def pop_task(self):
        'Remove and return the lowest priority task. Raise KeyError if empty.'
        while self.taskq:
            priority, count, task = heapq.heappop(self.taskq)
            if task is not self.REMOVED:
                del self.entry_finder[task]
            return task
        raise KeyError('pop from an empty priority queue')

    def pop_tasks_with_lowest_priority(self):
        current_app.logger.debug("self.taskq: %s" %self.taskq)
        if not self.taskq:
            return None
        tasks = []
        lowest_priority = self.taskq[0][0]
        while self.taskq:
            if self.taskq[0][0] == lowest_priority:
                tasks.append(self.pop_task())
            else:
                break
        
        return tasks

    def queue_state(self):
        return [t[2].id for t in self.taskq]

   
class Documentset:
    def __init__(self, run, user):
        self.user = user
        if run.root_dataset_id is not None:
            raise NotImplementedError
        elif run.root_solr_query_id is not None:
            self.data_type = 'search_query'
            self.data = run.root_solr_query.search_query
        else:
            raise Exception("Unknown documentset for run %s" %run)
        
    def make_task(self, processor_name, task_parameters={}):
        return generate_task ({'processor':processor_name,
                               self.data_type : self.data,
                               'parameters':task_parameters},
                              user=self.user,
                              return_task=True)

        
    


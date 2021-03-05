import heapq
import itertools
from app import db
from app.utils.db_utils import generate_task, generate_investigator_node
from app.utils.dataset_utils import get_dataset
from app.models import Task, Processor, InvestigatorRun, InvestigatorAction, InvestigatorResult

from app.utils.search_utils import DatabaseSearch
from copy import copy, deepcopy
from app.models import (
    Task,
    Processor,
    InvestigatorRun,
    InvestigatorAction,
    InvestigatorResult,
    Collection,
    SolrQuery,
    Dataset,
)

from app.investigator import processorsets
from flask import current_app
import asyncio


class Investigator:
    def __init__(self, run_uuid, planner, strategy="elaboration"):
        # planner, which executes tasks
        self.planner = planner
        self.user = self.planner.user
        # database record which should be updated in all operations
        self.run = InvestigatorRun.query.filter_by(uuid=run_uuid).one()

        self.root_collection = RunCollection(
            self.user, self.run.id, "root", self.planner.solr_controller
        )
        self.root_collection.make_root_collection(self.run)
        # to use in this run
        self.collections = {self.root_collection.collection_no : self.root_collection}
        # to store in db
        self.run.collections = [self.root_collection.dict()]

        self.loop_no = 0

        self.action_id = 0
        self.node_id = 0
        self.to_stop = False
        self.task_queue = TaskQueue()
        self.done_tasks = []

        self.strategy = strategy
        self.paths = []

    @property
    def queue_state(self):
        return self.task_queue.queue_state()

    async def initialize_run(self, user_args):
        """
        run initialization:
        """
        self.run.root_action_id = self.action_id

        # for now: always start with description
        # later on processorset could be infered from user parameters
        await self.action(self.initialize, processorset="DESCRIPTION")

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

        db_action = InvestigatorAction(
            run_id=self.run.id,
            action_id=self.action_id,
            action_type=action_func.__name__,
            why=why,
            action=action,
            input_queue=input_q,
            output_queue=self.queue_state,
        )

        current_app.logger.debug("DB_ACTION: %s" % db_action)
        current_app.logger.debug("NODES: %s" % self.run.nodes)

        db.session.add(db_action)
        db.session.commit()  # this also stores changes made inside actions (e.g. execute, report)

        self.action_id += 1

    async def initialize(self, processorset):
        """
        task queue initialization
        """
        init_path = Path(self.strategy)

        ip = deepcopy(init_path)
        current_app.logger.debug("I INIT_PATH COPIED")

        self.paths.append(init_path)

        why, action = self.add_processorset_into_q(
            processorset, self.root_collection, "initialization"
        )
        init_path.append_action(self.root_collection, why, action)


        ip = deepcopy(init_path)
        current_app.logger.debug("II INIT_PATH COPIED")
        
        return why, action

    async def select(self):
        """
        task selection from queue
        """

        tasks = self.task_queue.pop_tasks_with_lowest_priority()
        self.selected_tasks = tasks
        why = {"priority": "lowest"}
        action = {"selected_tasks": self.task_list(tasks)}
        return why, action

    async def execute(self):
        """
        task execution
        """
        tasks = self.selected_tasks
        await self.planner.execute_and_store_tasks(tasks)
        current_app.logger.debug("TASKS %s" % tasks)

        # append to previously done tasks
        self.done_tasks += self.task_list(tasks)
        self.run.done_tasks = self.done_tasks

        self.executed_tasks = [
            t for t in tasks if t.task_status == "finished"
        ]  # may be "failed"

        current_app.logger.debug("SELF.EXECUTED_TASKS: %s" % self.executed_tasks)

        why = {"status": "finished"}
        action = {"execute_tasks": self.task_list(self.executed_tasks)}

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
            why = {"dev_note": "not implemented"}
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
            node = generate_investigator_node(
                self.run,
                self.start_action,
                self.action_id,
                self.sort_by_interestingness(new_results),
                interestingness,
                self.user,
            )
            self.nodes += [{"uuid": str(node.uuid), "interestingness": interestingness}]
            self.run.nodes = self.nodes
            self.node_id += 1

        return why, action

    async def update(self):
        """
        update task queue
        """

        #current_app.logger.debug("PATHS:%s" % self.paths)
        updates = await asyncio.gather(
            *[self.update_path(path) for path in self.paths if not path.finished],
            return_exceptions=(not current_app.debug)
        )
        whys = []
        actions = []
        for u in updates:
            if isinstance(u[0], list):
                whys += u[0]
                actions += u[1]
            else:
                whys.append(u[0])
                actions.append(u[1])

        current_app.logger.debug("In update: WHYS %s" % whys)
        current_app.logger.debug("In update: ACTIONS %s" % actions)

        return whys, actions

    def find_collection(self, collection_no):
        return self.collections[collection_no]
        
    async def update_path(self, path):
        last_action = path.actions[-1]
        collections = [self.find_collection(c_no) for c_no in last_action["collections"]]
        why = last_action["why"]
        if isinstance(why, list):
            processorsets = [w["processorset"] for w in why]
            if len(set(processorsets)) > 1:
                raise NotImplementedError("Variety of processorsets in a single action: %s" %processorsets)
            else:
                last_processorset = processorsets[0]

        else:
            last_processorset = why["processorset"]


        current_app.logger.debug("~~~~~~~~~~~LAST_PROCESSORSET %s" %last_processorset)
            
        if path.strategy == "expansion":
            if last_processorset == "DESCRIPTION":
                why, action = self.start_expansion(
                    collection, "%s path strategy" % path.strategy
                )
            elif last_processorset == "EXPAND_QUERY":
                # check GLOBAL strategy, i.e. initial goal of investigation

                if self.strategy == "expansion":
                    # if initial goal was to find more documents then
                    # we could continue investigate this new dataset with more tasks
                    path.strategy = "elaboration"
                    collection = self.make_collection_from_expanded_query(last_action)
                    why, action = self.add_processorset_into_q(
                        "DESCRIPTION", collection, "new collection"
                    )
                    path.append_action(collection, why, action)
                elif self.strategy == "elaboration":
                    # initial goal was to investigate a particular collection
                    # we should not dig too deep into this new collection
                    path.finished = True
                    action = {}
                    why = {"reason": "%s initial goal" % self.strategy}
                    path.append_action(collection, why, action)
                else:
                    raise NotImplementedError("Unknown strategy %s" % self.strategy)
            else:
                raise NotImplementedError(
                    "Unexpected last processorset %s for %s strategy"
                    % (last_processorset, path.strategy)
                )

            path.append_action(collection, why, action)
            return why, action

        elif path.strategy == "elaboration":
            if last_processorset == "DESCRIPTION":
                if len(collections) == 1:
                    collection = collections[0]
                    why, action = await self.proceed_after_description(collection, path)
                    path.append_action(collection, why, action)                   
                    return why, action
                else:

                    current_app.logger.debug("=======COLLECTIONS: %s" %collections)
                    
                    whys = []
                    actions = []
                    # make new path for each collection, with language specific tasks
                    for collection in collections:
                        new_path = deepcopy(path)
                        why, action = await self.proceed_after_description(collection, new_path)
                        whys.append(why)
                        actions.append(action)
                        self.paths.append(new_path)


                    # make name comparison
                    why, action = self.add_processorset_into_q(
                        "COMPARE_NAMES", collections, "crosslingual comparison"
                        )
                    path.append_action(collection, why, action)
                    whys.append(why)
                    actions.append(action)
                    
                    return whys, actions
                    
            elif last_processorset == "SUMMARIZATION":
                path.finished = True
                why = {"reason": "non-significant"}
                action = {}
                return why, action
                
            elif last_processorset == "MONOLINGUAL_BIG":
                if len(collections) > 1:
                    raise NotImplementedError("More than one collection after MONOLINGUAL_BIG %s" %collections)
                else:
                    collection = collections[0]
            
                source_split, split_uuid = self.find_split_by_facet(
                    collection, "NEWSPAPER_NAME"
                    )
                new_collections = self.make_collections_from_split(
                    source_split, split_uuid, data_type=collection.data_type, outliers=True
                )
                
                if len(new_collections) == 1:
                    why, action = self.add_processorset_into_q(
                        "FIND_BEST_SPLIT", collection, "brute_force"
                    )
                    path.append_action(collection, why, action)
                    return why, action
                else:
                    # make new path
                    # add description for all collections
                    new_path = deepcopy(path) 
                    whys, actions = self.add_processorset_into_q(
                        "DESCRIPTION", new_collections, "needed to continue"
                    )
                    new_path.append_action(new_collections, whys, actions)
                    self.paths.append[new_path]
                    
                    # in the old path: tm comparison
                    why, action = self.make_tm_comparison_task(
                        language=list(collection.languages().keys())[0],
                        collections=new_collections
                        )
                    path.append_action(new_collections, why, action)

                    whys.append(why)
                    actions.append(action)

                    return why, actions
                    
                    
            elif last_processorset == "SPLIT_BY_LANGUAGE":
                collection = collections[0]
                lang_split, split_uuid  = self.find_split_by_facet(
                    collection, "LANGUAGE"
                )


                current_app.logger.debug("IN SPLIT_BY_LANGUAGE")
                current_app.logger.debug("COLLECTION: %s" %collection)
                
                new_collections = self.make_collections_from_split(
                    lang_split,
                    split_uuid,
                    data_type=collection.data_type,
                    outliers=0.1
                )


                current_app.logger.debug("NEW_COLLECTIONS: %s" %new_collections)
                current_app.logger.debug("NEW_COLLECTIONS: %s" %[c.collection_no for c in new_collections])
                
                if len(new_collections) == 1:
                    why, action = await self.add_language_specific_tasks_to_collection(
                        collection,
                        source_uuid = collection.find_processor_uuid(
                            "ExtractFacets"
                        ))
                    path.append_action(collection, why, action)
                    return why, action
                else:
                    whys, actions = self.add_processorset_into_q(
                        "DESCRIPTION", new_collections, "needed to continue"
                    )
                    path.append_action(new_collections, whys, actions)

                    current_app.logger.debug("PATH LAST ACTION: %s" %path.actions[-1])
                    
                    return whys, actions
                    
                
                    
            else:
                raise NotImplementedError(
                    "Don't know what to do after processorset %s" % last_processorset
                )
        else:
            raise NotImplementedError("Unknown path strategy %s" % path.strategy)


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
        current_app.logger.debug("!!!!!!!!!!!!!!!!CHECK_FOR_STOP: self.paths %s" %self.paths)
        current_app.logger.debug("self.task_queue.taskq: %s" %self.task_queue.taskq)
        if self.run.user_parameters.get("describe"):
            self.to_stop = {"user_parameters": "describe"}
        elif not [p for p in self.paths if p.finished == False]:
            self.to_stop = {"paths": "all finished"}
        elif self.task_queue.taskq == []:
            self.to_stop = {"taskq": "empty"}
        return self.to_stop

    def update_status(self, status):
        self.run.run_status = status
        db.session.commit()

    def make_tasks(self, processorset_name, collection, source_uuid=None):
        collection.processors.append(processorset_name)
        return [
            collection.make_task(p["name"], p["parameters"], source_uuid)
            for p in processorsets[processorset_name]
        ]


    def make_comparison_tasks(self, processorset_name, collections):
        comparison_tasks = []
            
        for processor in processorsets[processorset_name]:
            tasks_to_compare = []
            source_processor = processor["source"]

            current_app.logger.debug("COLLECTIONS: %s" %collections)
            
            for run_collection in collections:
                

                
                for task in run_collection.tasks:
                    if task.processor.name == source_processor and task.task_status == "finished":
                        tasks_to_compare.append([task, run_collection])
            if len(tasks_to_compare) < 2:
                return "nothing-to-compare", {}

            current_app.logger.debug("TASKS_TO_COMPARE: %s" %tasks_to_compare)
            
            for task_pair in itertools.combinations(tasks_to_compare, 2):
                source_uuids = [str(task[0].uuid) for task in task_pair]
                current_app.logger.debug("***SOURCE_UUIDS %s" %source_uuids)
                comparison_task = generate_task(
                {
                    "processor": "Comparison",
                    "source_uuid": source_uuids
                },
                user = self.user,
                return_task=True
                )


                # these are db collections
                comparison_task.collections = task_pair[0][0].collections + task_pair[1][0].collections

                # these are RunCollections, for the current run
                for t in task_pair:
                    t[1].tasks.append(comparison_task)

                comparison_tasks.append(comparison_task)

        for run_collection in collections:
            run_collection.processors.append(processorset_name)

        return comparison_tasks
                   


    def estimate_node_interestingness(self, results):
        # self is currently not used but might be useful to estimate interestingness
        # for now: maximum of existing results
        return max([result["interestingness"] for result in results])

    def combine_results(self, *results):
        # for now: just add everything
        # self is currently not used
        # later on: some selective process based on result interestingness
        why = {"dev_note": "not implemented; all results combined unselectevely"}
        combined_result = self.sort_by_interestingness(sum(results, []))
        return why, combined_result

    async def proceed_after_description(self, collection, path):
        size = await collection.collection_size()
        if collection.data_type != "dataset" and size < 10:
            why, action = self.start_expansion(
                path, collection, "not enough data"
            )

        else:
            languages = collection.collection_languages()
            if len(languages) == 1:
                # start language-specific tasks
                why, action = await self.add_language_specific_tasks_to_collection(
                    collection,
                    source_uuid=collection.find_processor_uuid(
                        "ExtractFacets"
                    ),
                )
            elif len(languages) > 1:
                why, action = self.add_processorset_into_q(
                    "SPLIT_BY_LANGUAGE", collection, "brute_force"
                )
            else:
                raise NotImplementedError(
                    "Unexpected collection_languages: %s" % languages
                )
        return why, action
    
    def add_processorset_into_q(
        self, processorset, collections, reason, source_uuid=None
    ):
        current_app.logger.info("ADDING PROCESSORSET: %s" % processorset)
        current_app.logger.debug("COLLECTIONS %s" % collections)
        if isinstance(collections, list):
            processornames = [p["name"] for p in processorsets[processorset]]
            if "Comparison" in processornames:
                return self.add_comparisonset_into_q(processorset, collections, reason)
            else:
                whys = []
                actions = []
                for collection in collections:
                    w,a = self._add_processorset_into_q(
                        processorset, collection, reason, source_uuid=source_uuid)
                    whys.append(w)
                    actions.append(a)
                return whys, actions
        else:
            return self._add_processorset_into_q(
                    processorset, collections, reason, source_uuid=source_uuid)
        
    def _add_processorset_into_q(
                    self, processorset, collection, reason, source_uuid=None
    ):
        tasks = self.make_tasks(processorset, collection, source_uuid)
        self.task_queue.add_tasks(tasks)
        why = {"processorset": processorset, "reason": reason}
        action = {"tasks_added_to_q": self.task_list(tasks)}

        return why, action

    def add_comparisonset_into_q(self, processorset, collections, reason):
        if len(set([p["name"] for p in processorsets[processorset]])) > 1:
            raise NotImplementedError("Don't know how to combine comparisons with other tasks for processorset %s" %processorset)
        comparison_tasks = self.make_comparison_tasks(processorset, collections)
        self.task_queue.add_tasks(comparison_tasks)
        why = {"processorset": processorset, "reason": reason}
        action = {"tasks_added_to_q": self.task_list(comparison_tasks)}    
        return why, action
        
    
    def make_tm_comparison_task(self):
        raise NotImplementedError("MAKE_TM_COMPARISON_TASK is not ready. Should combine operations from add_processor_set and make_task")
    
    async def add_language_specific_tasks(self, collections, source_uuid):
        whys = []
        actions = []
        for collection in collections:
            why, action = await self.add_language_specific_tasks_to_collection(
                collections, source_uuid
            )
            whys.append(why)
            actions.append(action)

        return whys, actions

    async def add_language_specific_tasks_to_collection(self, collection, source_uuid):
        # collection for sure has document in only one language
        language = collection.collection_languages() 
        assert(len(language) == 1)
        language = list(language.keys())[0]
        
        collection_size = await collection.collection_size()

        current_app.logger.debug("****COLLECTION_SIZE: %s" % collection_size)

        if collection_size <= 30:
            why, action = self.add_processorset_into_q(
                "SUMMARIZATION", collection, "small_collection", source_uuid
            )
        elif collection_size < 100000:  # TODO: find most realistic number
            current_app.logger.debug("######LANGUAGE: %s" %language)
            if language=="se":
                # TM does not exist for Swedish
                # TODO: check automatically, that model exists for the language
                why, action = self.add_processorset_into_q(
                    "SPLIT_BY_SOURCE", collection, "language", source_uuid
                    )
                why["language"] = language
            else:
                why, action = self.add_processorset_into_q(
                    "MONOLINGUAL_BIG", collection, "big_collection", source_uuid
                )
        else:
            why = {"reason": "too_big_collection"}
            action = {}

        why["collection_size"] = collection_size
        return why, action

    def start_expansion(path, collection, reason):
        path.strategy = "expansion"
        source_uuid = collection.find_processor_uuid("ExtractWords")
        why, action = self.add_processorset_into_q(
            "EXPAND_QUERY", collection, reason, source_uuid=source_uuid
        )
        return why, action

    def find_split_by_facet(self, collection, facet):
        for task in collection.tasks:
            if (
                task.processor.name == "SplitByFacet"
                and task.parameters["facet"] == facet
                and task.task_status == "finished"
            ):
                return task.task_result, task.uuid

    def make_collection_from_expanded_query(self, last_action):
        raise NotImplementedError(
            "MAKE_COLLECTION_FROM_EXPANDED_QUERY: Don't know how to make collection from this action: %s"
            % last_action
        )

    def make_collections_from_split(self, split, origin, data_type, number=None, outliers=False):
        # split is a task result
        # returns list of collections
        # todo: some meaningful criteria
        if len(split.result) == 1 or not outliers:
            thr = 0.0
        elif isinstance(outliers, float):
            thr = outliers
        else:
            thr = 0.001

        if data_type == "search_query":
            collections = [
                RunCollection(
                    self.user,
                    self.run.id,
                    origin,
                    self.planner.solr_controller,
                    query=split.result[facet],
                )
                for facet in split.result
                if split.interestingness[facet] >= thr
            ]
        elif data_type == "dataset":
            collections = [
                RunCollection(
                    self.user,
                    self.run.id,
                    origin,
                    self.planner.solr_controller,
                    dataset_name = split.result[facet],
                )
                for facet in split.result
                if split.interestingness[facet] >= thr
            ]   
            
        else:
            raise NotImplementedError("Don't know how to make collections from data_type %s" %data_type)
   
        for c in collections:
            current_app.logger.debug("collection dict: %s" % c.dict())

        # for storing in db
        self.run.collections = self.run.collections + [
            collection.dict() for collection in collections
        ]

        # for using in this run
        self.collections.update({c.collection_no:c for c in collections})

        db.session.commit()

        return collections

    
    def apply_comparison(self, collections):

        tasks_to_compare = []

        for collection in collections:

            for task in collection.tasks:
                if (
                    task.processor.name == "ExtractFacets"
                    and task.task_status == "finished"
                ):
                    tasks_to_compare.append(task)
                if len(tasks_to_compare) == 2:
                    break

        if len(tasks_to_compare) < 2:
            return "nothing-to-compare", {}

        comparison_task = generate_task(
            {
                "processor": "Comparison",
                "source_uuid": [str(task.uuid) for task in tasks_to_compare],
            },
            user=self.user,
            return_task=True,
        )
        comparison_task.collections = []
        for task in tasks_to_compare:
            comparison_task.collections += task.collections

        self.task_queue.add_tasks(comparison_task)
        current_app.logger.debug("COMPARISON_TASK %s" % comparison_task)
        action = {"tasks_added_to_q": self.task_list(comparison_task)}
        return "ready_for_comparison", action


    @staticmethod
    def sort_by_interestingness(results):
        return sorted(results, key=lambda r: r["interestingness"], reverse=True)

    @staticmethod
    def task_list(tasks):
        return [task.dict(style="investigator") for task in tasks]


class TaskQueue:
    def __init__(self):
        self.taskq = []  # list of entries arranged in a heap
        self.entry_finder = {}  # mapping of tasks to entries
        self.REMOVED = "<removed-task>"  # placeholder for a removed task
        self.counter = itertools.count()  # unique sequence count

    def add_tasks(self, tasks, priority=0):
        for t in tasks:
            self.add_task(t, priority=priority)

    def add_task(self, task, priority=0):
        "Add a new task or update the priority of an existing task"
        if task in self.entry_finder:
            self.remove_task(task)
        count = next(self.counter)
        entry = [priority, count, task]
        self.entry_finder[task] = entry
        heapq.heappush(self.taskq, entry)

    def remove_task(self, task):
        "Mark an existing task as REMOVED.  Raise KeyError if not found."
        entry = self.entry_finder.pop(task)
        entry[-1] = self.REMOVED

    def pop_task(self):
        "Remove and return the lowest priority task. Raise KeyError if empty."
        while self.taskq:
            priority, count, task = heapq.heappop(self.taskq)
            if task is not self.REMOVED:
                del self.entry_finder[task]
            return task
        raise KeyError("pop from an empty priority queue")

    def pop_tasks_with_lowest_priority(self):
        current_app.logger.debug("self.taskq: %s" % self.taskq)
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


class RunCollection:
    collection_count = 0

    def __init__(self, user, run_id, origin, solr_controller, query=None, dataset_name=None):
        self.solr_controller = solr_controller
        RunCollection.collection_count += 1
        self.processors = []
        self.tasks = []
        self.user = user

        self.collection_no = copy(RunCollection.collection_count)
                                             
        if query:
            self.data_type = "search_query"
            self.data = query

            solr_query = SolrQuery(search_query=query)
            db.session.add(solr_query)
            db.session.commit()

            self.collection = Collection(
                run_id=run_id,
                collection_no=self.collection_no,
                data_type=self.data_type,
                data_id=solr_query.id,
            )

        elif dataset_name:
            self.data_type = "dataset"
            self.data = {
                "name": dataset_name,
                "user": "PRA"
            }

            self.collection = Collection(
                run_id = run_id,
                collection_no = self.collection_no,
                data_type=self.data_type,
                data_id=get_dataset(self.data).id,
            )

        else:
            self.collection = Collection(
                run_id=run_id,
                collection_no=self.collection_no
            )

        if not isinstance(origin, list):
            origin = [origin]
        self.collection.origin = [str(o) for o in origin]

        db.session.add(self.collection)
        db.session.commit()

        self.languages = None
        self.size = None

    def __repr__(self):
        return "Data %s processors %s" % (self.data, self.processors)

    def dict(self):
        return self.collection.dict()

    def make_root_collection(self, run):

        if run.root_dataset_id is not None:
            self.data_type = "dataset"
            self.data = {"name": run.root_dataset.dataset_name,
                         "user": run.root_dataset.user}
        elif run.root_solr_query_id is not None:
            self.data_type = "search_query"
            self.data = run.root_solr_query.search_query
        else:
            raise Exception("Unknown collection for run %s" % run)

    def make_task(self, processor_name, task_parameters={}, source_uuid=None):

        task_dict = {
            "processor": processor_name,
            self.data_type: self.data,
            "parameters": task_parameters,
        }

        current_app.logger.debug("!!!! INVESTIGATOR  task_dict %s" % task_dict)

        if source_uuid:
            task_dict["source_uuid"] = source_uuid

        task = generate_task(task_dict, user=self.user, return_task=True,)
        task.collections.append(self.collection)
        self.tasks.append(task)
        return task

    async def collection_size(self):
        if self.size:
            return self.size

        if self.data_type == "dataset":
            dataset = Dataset.query.filter_by(
                dataset_name=self.data["name"], user=self.data["user"]
            ).one_or_none()
            self.size = len(dataset.documents)
            return self.size
        else:
            database_search = DatabaseSearch(self.solr_controller)
            search_result = await database_search.search(
                {"rows": 0, **self.data}, retrieve="docids"
            )
            return search_result["numFound"]

    def collection_languages(self):
        if self.languages:
            return self.languages

        for task in self.collection.tasks:
            if (
                task.task_status == "finished"
                and task.processor.name == "ExtractFacets"
                and "LANGUAGE" in task.task_result.result
            ):
                self.languages = task.task_result.result["LANGUAGE"]
                return self.languages

    def find_processor_uuid(self, processor_name):
        for task in self.collection.tasks:
            if task.task_status == "finished" and task.processor.name == processor_name:
                return task.uuid


class Path:
    def __init__(self, strategy):
        self.actions = []
        self.strategy = strategy
        self.finished = False

    def append_action(self, collections, why, action):       
        if not isinstance(collections, list):
            collections = [collections]

        self.actions.append({"collections": [c.collection_no for c in collections],
                             "why": why,
                             "action": action})

    def __repr__(self):
        return "PATH: strategy: %s finished: %s actions: %s" % (self.strategy, self.finished, self.actions)

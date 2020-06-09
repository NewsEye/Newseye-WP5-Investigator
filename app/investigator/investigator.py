import heapq
import itertools
from app import db
from app.utils.db_utils import generate_task, generate_investigator_node
from app.utils.search_utils import search_database
from app.models import (
    Task,
    Processor,
    InvestigatorRun,
    InvestigatorAction,
    InvestigatorResult,
    Collection,
    SolrQuery,
)
from copy import copy
from app.investigator import processorsets
from flask import current_app
import asyncio
from datetime import datetime
from app.utils.db_utils import get_solr_query
from app.utils.dataset_utils import get_dataset


class Investigator:
    def __init__(self, run_uuid, planner):
        # planner, which executes tasks
        self.planner = planner
        self.user = self.planner.user
        # database record which should be updated in all operations
        self.run = InvestigatorRun.query.filter_by(uuid=run_uuid).one()

        current_app.logger.debug("RUN: %s" % self.run)

        self.root_documentset = Run_Collection(self.user, self.run.id)
        self.root_documentset.make_root_collection(self.run)
        self.run.collections = [self.root_documentset.dict()]

        self.current_collections = [self.root_documentset]
        self.loop_no = 0
        self.action_id = 0
        self.node_id = 0
        self.to_stop = False
        self.task_queue = TaskQueue()
        self.done_tasks = []

    @property
    def queue_state(self):
        return self.task_queue.queue_state()

    async def initialize_run(self, user_args, continue_from_node=None):
        """
        run initialization:
        """
        self.update_status("initializing")

        await self.get_data(user_args)

        if continue_from_node:
            # continue investigations from a given node
            raise NotImplementedError
        else:
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

        whys, actions = await action_func(**action_parameters)
        if not isinstance(whys, list):
            whys = [whys]
            actions = [actions]

        for why, action in zip(whys, actions):

            db_action = InvestigatorAction(
                run_id=self.run.id,
                action_id=self.action_id,
                action_type=action_func.__name__,
                why=why,
                action=action,
                input_queue=input_q,
                output_queue=self.queue_state,
                timestamp=datetime.utcnow(),
            )

            current_app.logger.debug("DB_ACTION: %s" % db_action)
            current_app.logger.debug("NODES: %s" % self.run.nodes)
            current_app.logger.debug("TASKQ: %s" % self.task_queue)
            db.session.add(db_action)
            self.action_id += 1

        db.session.commit()  # this also stores changes made inside actions (e.g. execute, report)

    async def initialize(self, processorset):
        """
        task queue initialization
        """
        why, action = self.add_processorset_into_q(
            processorset, self.root_documentset, "initialization"
        )
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
        ]  # maybe "failed"
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
            self.nodes += [
                {
                    "uuid": str(node.uuid),
                    "interestingness": interestingness,
                    "id": str(node.id),
                    "start_action_id": str(node.start_action_id),
                    "end_action_id": str(node.end_action_id),
                }
            ]
            self.run.nodes = self.nodes
            self.node_id += 1

        return why, action

    async def update(self):
        """
        update task queue
        """
        # Rule-based for now

        if "SPLIT" not in self.root_documentset.processors:
            # the first thing to do: split
            current_app.logger.debug("UPDATE: split not done ---> adding split")
            why, action = self.add_processorset_into_q(
                "SPLIT", self.root_documentset, "brute_force"
            )
            return why, action

        elif self.root_documentset in self.current_collections:
            current_app.logger.debug(
                "UPDATE: split is done but we are still in the root dataset"
            )
            # we are still in the root collection but splits are done
            lang_split, split_uuid = self.find_split_by_facet(
                self.root_documentset, "LANGUAGE"
            )
            self.current_collections = self.make_collections_from_split(
                lang_split, outliers=True
            )

            current_app.logger.debug(
                "###CURRENT COLLECTIONS: %s" % self.current_collections
            )

            whys, actions = await self.add_language_specific_tasks(
                self.current_collections, split_uuid
            )

            for collection in self.current_collections:
                w, a = self.add_processorset_into_q(
                    "DESCRIPTION", collection, "brute_force", source_uuid=split_uuid
                )
                whys.append(w)
                actions.append(a)

            return whys, actions

        elif len(self.current_collections) > 1:
            # we already away from the root and have several collections
            current_app.logger.debug("UPDATE: comparison of split parts")

            why, action = self.apply_comparison(self.current_collections)

            self.current_collections = []

        else:
            # dev_note means temporal placeholder, which should not be used by explainer:
            why = {"dev_note": "not implemented"}
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
    async def get_data(self, user_args):
        if user_args.get("dataset"):
            self.run.root_dataset = get_dataset(user_args["dataset"])
        elif user_args.get("search_query"):
            self.run.root_solr_query = get_solr_query(user_args["search_query"])
        else:
            raise NotImplementedError

    def check_for_stop(self):
        if self.run.user_parameters.get("describe"):
            self.to_stop = {"user_parameters": "describe"}
        elif self.task_queue.taskq == []:
            self.to_stop = {"taskq": "empty"}
        return self.to_stop

    def update_status(self, status):
        self.run.run_status = status
        if status == "finished":
            self.run_finished = datetime.utcnow()
        db.session.commit()

    def make_tasks(self, set_name, documentset, source_uuid=None):
        documentset.processors.append(set_name)
        return [
            documentset.make_task(p["name"], p["parameters"], source_uuid)
            for p in processorsets[set_name]
        ]

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

    def add_processorset_into_q(
        self, processorset, documentset, reason, source_uuid=None
    ):
        current_app.logger.info("ADDING PROCESSORSET: %s" % processorset)
        tasks = self.make_tasks(processorset, documentset, source_uuid)
        self.task_queue.add_tasks(tasks)
        why = {"processorset": processorset, "reason": reason}
        action = {"tasks_added_to_q": self.task_list(tasks)}
        return why, action

    async def add_language_specific_tasks(self, collections, source_uuid):
        whys = []
        actions = []
        for collection in collections:
            collection_size = await collection.collection_size()

            current_app.logger.debug("****COLLECTION_SIZE: %s" % collection_size)

            if collection_size <= 20:
                why, action = self.add_processorset_into_q(
                    "SUMMARIZATION", collection, "small_collection", source_uuid
                )
            elif collection_size < 1000:
                why, action = self.add_processorset_into_q(
                    "TOPIC_MODEL", collection, "big_collection", source_uuid
                )
            else:
                why = {"reason": "too_big_collection"}
                action = {}

            why["collection_size"] = collection_size
            whys.append(why)
            actions.append(action)

        return whys, actions

    def find_split_by_facet(self, collection, facet):
        for task in collection.tasks:
            if (
                task.processor.name == "SplitByFacet"
                and task.parameters["facet"] == facet
                and task.task_status == "finished"
            ):
                return task.task_result, task.uuid

    def make_collections_from_split(self, split, number=None, outliers=False):
        # split is a task result
        # returns list of collections
        # todo: some meaningful criteria
        if len(split.result) == 1 or not outliers:
            thr = 0.0
        else:
            thr = 0.001
        collections = [
            Run_Collection(self.user, self.run.id, query=split.result[lang], lang=lang)
            for lang in split.result
            if split.interestingness[lang] >= thr
        ]

        current_app.logger.debug("SELF.RUN.COLLECTIONS: %s" % self.run.collections)
        current_app.logger.debug("COLLECTIONS: %s" % collections)

        for c in collections:
            current_app.logger.debug("collection dict: %s" % c.dict())

        self.run.collections = self.run.collections + [
            collection.dict() for collection in collections
        ]

        db.session.commit()

        current_app.logger.debug("SELF.RUN.COLLECTIONS: %s" % self.run.collections)

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
        if not isinstance(tasks, list):
            tasks = [tasks]
        return [task.dict(style="investigator") for task in tasks]


class TaskQueue:
    def __init__(self):
        self.taskq = []  # list of entries arranged in a heap
        self.entry_finder = {}  # mapping of tasks to entries
        self.REMOVED = "<removed-task>"  # placeholder for a removed task
        self.counter = itertools.count()  # unique sequence count

    def add_tasks(self, tasks, priority=0):
        if not isinstance(tasks, list):
            tasks = [tasks]
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


class Run_Collection:
    collection_no = 0

    def __init__(self, user, run_id, query=None, lang=None):
        Run_Collection.collection_no += 1
        current_app.logger.debug("COLLECTION_NO: %s" % Run_Collection.collection_no)
        self.processors = []
        self.tasks = []
        self.user = user
        # we know language if the collection is a result of language-based split
        self.lang = lang
        if query:
            self.data_type = "search_query"
            self.data = query

            solr_query = SolrQuery(search_query=query)
            db.session.add(solr_query)
            db.session.commit()

            self.collection = Collection(
                run_id=run_id,
                collection_no=Run_Collection.collection_no,
                data_type=self.data_type,
                data_id=solr_query.id,
            )

        else:
            self.collection = Collection(
                run_id=run_id, collection_no=Run_Collection.collection_no
            )

        db.session.add(self.collection)
        db.session.commit()

    def __repr__(self):
        return "Data %s processors %s" % (self.data, self.processors)

    def dict(self):
        return self.collection.dict()

    def make_root_collection(self, run):
        if run.root_dataset_id is not None:
            self.data_type = "dataset"
            self.data = {
                "name": run.root_dataset.dataset_name,
                "user": run.root_dataset.user,
            }
            self.collection.data_type = self.data_type
            self.collection.data_id = run.root_dataset.id
        elif run.root_solr_query_id is not None:
            self.data_type = "search_query"
            self.data = run.root_solr_query.search_query
            self.collection.data_type = self.data_type
            self.collection.data_id = run.root_solr_query.id
        else:
            raise Exception("Unknown documentset for run %s" % run)

    def make_task(self, processor_name, task_parameters={}, source_uuid=None):

        if not isinstance(task_parameters, dict):
            # need to infer parameters dynamically
            if task_parameters == "LANG":
                task_parameters = {"language": self.lang}
            else:
                raise NotImplementedError(
                    "Don't know where to get parameters %s" % task_parameters
                )

        task_dict = {
            "processor": processor_name,
            self.data_type: self.data,
            "parameters": task_parameters,
        }

        current_app.logger.debug("!!!! InVESTIGATOR  task_dict %s" % task_dict)

        if source_uuid:
            task_dict["source_uuid"] = source_uuid

        task = generate_task(task_dict, user=self.user, return_task=True,)
        task.collections.append(self.collection)
        self.tasks.append(task)
        return task

    async def collection_size(self):
        if self.data_type == "dataset":
            raise NotImplementedError
        else:
            search_result = await search_database(
                {"rows": 0, **self.data}, retrieve="docids"
            )
        return search_result["numFound"]

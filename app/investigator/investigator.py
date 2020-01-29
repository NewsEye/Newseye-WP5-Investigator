from app.main.db_utils import store_results, generate_task
import asyncio
from flask import current_app
from config import Config
from app.investigator import ANALYSIS, LINKING, ANALYSIS_LINKED_DOCS
from app.analysis.assessment import max_interestingness
from app.investigator.result_comparison import estimate_interestingness
from app.models import Task


class Investigator(object):
    def __init__(self, planner, task):
        self.planner = planner
        self.main_task = task
        self.force_refresh = task.force_refresh
        self.task_result = {}
        self.interestingness = 0.0
        self.runner = SubtaskRunner(
            self.planner, self.main_task, self.interestingness, self.task_result
        )

    # TODO: make recoursive function for an infinite investigation loop
    async def investigate(self):
        linked_docs_analysing_tasks = []

        for pattern_set in asyncio.as_completed(
            [self.run_pattern_set(ps) for ps in [ANALYSIS, LINKING]]
        ):
            subtasks = await pattern_set
            for subtask in subtasks:
                if UTILITY_MAP[subtask.utility].output_type == "id_list_with_dist":
                    # TODO: call investigate function recoursively

                    query = self.make_search_query_from_linked_documents(subtask)
                    # current_app.logger.debug("QUERY: %s" %query)
                    if query:
                        linked_docs_analysing_tasks.append(
                            asyncio.create_task(
                                self.run_pattern_set(ANALYSIS_LINKED_DOCS, search_query=query)
                            )
                        )

        comparison_tasks = []
        for linked_analysis_task in asyncio.as_completed(linked_docs_analysing_tasks):
            subtasks = await linked_analysis_task
            for subtask in subtasks:
                comparison_tasks.append(self.make_comparison_task(subtask))

        await asyncio.gather(*comparison_tasks, return_exceptions=(not current_app.debug))

    def make_comparison_task(self, subtask):
        subtask_parameters = subtask.task_parameters
        comparable_task_uuids = [str(subtask.uuid)] + [
            uuid
            for uuid, res in self.task_result.items()
            # TODO: more flexible definition of comparable task
            if (
                res["utility_name"] == subtask_parameters["utility"]
                and res["search_query"] != subtask_parameters["search_query"]
                and res["utility_parameters"] == subtask_parameters["utility_parameters"]
            )
        ]
        task_ids = [
            Task.query.filter_by(uuid=uuid).first().task_id for uuid in comparable_task_uuids
        ]
        comparison_task = self.runner.generate_investigation_tasks(
            [("comparison", {"task_ids": task_ids})]
        )
        return asyncio.create_task(
            self.runner.run_subtasks_and_update_results(
                comparison_task,
                estimate_interestingness,
                reference={subtask.output_type: comparable_task_uuids},
            )
        )

    async def run_pattern_set(self, pattern_set, search_query=None):

        patterns = [
            Pattern(self.runner, self.main_task, search_query=search_query)
            for Pattern in pattern_set
        ]

        current_app.logger.debug("PATTERNS %s SEARCH_QUERY %s" % (patterns, search_query))
        # each pattern returns a list of subtasks hence patternset returns list of lists
        subtasks = await asyncio.gather(
            *[pattern() for pattern in patterns], return_exceptions=(not current_app.debug)
        )
        subtasks = [s for s in subtasks if not isinstance(s, Exception)]

        return [s for sl in subtasks for s in sl]

    def make_search_query_from_linked_documents(self, task):
        document_list = task.task_result.result.get("similar_docs", None)
        if document_list:
            current_app.logger.debug("main_task.search_query %s" % self.main_task.search_query)
            try:
                return {
                    "q": " ".join([docid for docid in document_list]),
                    "mm": 1,
                    # qf (query field) preserves language fo the original search
                    # TODO: general solution to manage languages across utils
                    "qf": "id " + self.main_task.search_query["qf"],
                }
            except KeyError:
                return {"q": " ".join([docid for docid in document_list]), "mm": 1}


class SubtaskRunner(object):
    def __init__(self, planner, main_task, interestingness, task_result):
        self.interestingness = interestingness
        self.task_result = task_result
        self.planner = planner
        self.main_task = main_task

    @staticmethod
    def estimate_interestingness(subtask):
        # default: do nothing
        return subtask.task_result.interestingness

    async def run_subtasks_and_update_results(
        self, subtasks, estimate_interestingness=None, reference=None
    ):
        """ 
        Generates and runs many tasks in parallel, assesses results
        Stores data in the database as soon as they ready

               1. gets list of subtasks
               2. runs in parallel, store in db as soon as ready
               3. result of the main task is a list of task uuid (children tasks) + interestingness

         """

        if not estimate_interestingness:
            estimate_interestingness = self.estimate_interestingness

        for subtask in asyncio.as_completed([self.execute_and_store(s) for s in subtasks]):
            # the subtask result is already stored, now we have to add subtask into list of task results
            done_subtask = await subtask

            subtask_interestingness = max_interestingness(estimate_interestingness(done_subtask))
            if subtask_interestingness > self.interestingness:
                self.interestingness = subtask_interestingness
            if not str(done_subtask.uuid) in self.task_result:
                self.task_result[str(done_subtask.uuid)] = {
                    "utility_name": done_subtask.utility,
                    "utility_parameters": done_subtask.utility_parameters,
                    "search_query": done_subtask.search_query,
                    "interestingness": subtask_interestingness,
                }
                if reference:
                    # a bit adhooc to make nicer output for investigator api
                    self.task_result[str(done_subtask.uuid)].update({"reference": reference})
            store_results(
                [self.main_task],
                [self.task_result],
                set_to_finished=False,
                interestingness=self.interestingness,
            )

    def generate_investigation_tasks(self, utilities, source_uuid=None, search_query=None):
        return generate_tasks(
            user=self.planner.user,
            queries=[
                (
                    "analysis",
                    {
                        "source_uuid": source_uuid,
                        "search_query": search_query,
                        "utility": u,
                        "utility_parameters": params,
                        "force_refresh": self.main_task.force_refresh,
                    },
                )
                for u, params in utilities
            ],
            parent_id=self.main_task.uuid,
            return_tasks=True,
        )

    async def execute_and_store(self, subtask):
        if subtask.force_refresh:
            for uuid, done_task in self.task_result.items():
                if (
                    done_task["utility_name"] == subtask.utility
                    and done_task["utility_parameters"] == subtask.utility_parameters
                    and done_task["search_query"] == subtask.search_query
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

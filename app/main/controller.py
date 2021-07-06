from flask import current_app
from flask_login import current_user
from app.main.planner import TaskPlanner
from app.investigator.investigator import Investigator
from app.utils.db_utils import generate_task, generate_investigator_run
from app.models import Task, User, InvestigatorRun
import threading
import time
import asyncio
from app.main.solr_controller import SolrController
from config import Config
import multiprocessing


solr_controller = SolrController()
semaphore = multiprocessing.Semaphore(Config.SOLR_MAX_SESSIONS)

def execute_task(args):
    """
    Generate tasks from queries and execute them.
    :param queries: a single query or a list of queries
    :return: A list of task_objects or task_ids corresponding to the queries.
    """

    task_uuid = generate_task(args)

    # TODO: allow user to cancel task
    # currently each user query starts  a new thred (why?) and it's impossible to call tasks that are already running
    # t = threading.Thread(
    #     target=worker,
    #     args=[
    #         current_app._get_current_object(),
    #         current_user.id,
    #         task_uuid,
    #         solr_controller,
    #     ],
    # )
    # t.setDaemon(False)
    # t.start()

    
    
    process = multiprocessing.Process(target=task_worker,
                                name=str(task_uuid),
                                args = (current_app._get_current_object(),
                                        current_user.id,
                                        task_uuid,
                                        solr_controller))
    process.start()
    
    #Wait until the thread has started the tasks before responding to the user
    # while (
    #     Task.query.filter(Task.uuid == task_uuid, Task.task_status == "created").count()
    #     > 0
    # ):
    #     time.sleep(0.1)

    return Task.query.filter(Task.uuid == task_uuid).one_or_none()


def task_worker(app, user_id, task_uuid, solr_controller):
    with semaphore:
        with app.app_context():
            planner = TaskPlanner(User.query.get(user_id), solr_controller)
            asyncio.run(planner.execute_user_task(task_uuid))


def investigator_run(args):
    """
    Currently works exactly the same as the previous function (for the single task).
    Starts a new thread and initializes an investigator run within this thread
    """

    run_uuid = generate_investigator_run(args)

    process = multiprocessing.Process(target=run_worker,
                                name = str(run_uuid),
                                args = (current_app._get_current_object(),
                                        current_user.id,
                                        run_uuid,
                                        solr_controller,
                                        args))
    process.start()
#     t = threading.Thread(
#         target=run_thread,
#         args=[
#             current_app._get_current_object(),
#             current_user.id,
#             run_uuid,
#             solr_controller,
#             args,
#         ],
#     )
#     t.setDaemon(False)
#     t.start()

#    while (
#        InvestigatorRun.query.filter(
#            InvestigatorRun.uuid == run_uuid, InvestigatorRun.run_status == "created"
#        ).count()
#        > 0
#    ):
#        time.sleep(0.1)

    return InvestigatorRun.query.filter(InvestigatorRun.uuid == run_uuid).one_or_none()


def run_worker(app, user_id, run_uuid, solr_controller, user_args):
    with semaphore:
        with app.app_context():
            planner = TaskPlanner(User.query.get(user_id), solr_controller)
            current_app.logger.debug("USER_ARGS: %s" % user_args)
        
            if user_args.get("dataset"):
                
                investigator = Investigator(
                    run_uuid,
                    planner,
                    user_args["parameters"].get("strategy", "elaboration"),
                    dataset=user_args.get("dataset")
                )
        
            elif user_args.get("search_query"):
        
                investigator = Investigator(
                    run_uuid,
                    planner,
                    user_args["parameters"].get("strategy", "elaboration"),
                    search_query=user_args.get("search_query")
                )
        
            else:
                raise NotImplementedError
        
                
            asyncio.run(investigator.initialize_run(user_args))
            asyncio.run(investigator.act())

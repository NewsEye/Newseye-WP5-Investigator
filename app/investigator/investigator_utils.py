from app.main.db_utils import generate_tasks, store_results
import numpy as np
import asyncio


DEFAULT_UTILITIES = ['common_facet_values',
#                     'query_topic_model',  # TODO: parameters
                     'compute_tf_idf']


def sum_up_interestingness(interestingness):
    if isinstance(interestingness, float):        
        return interestingness
    elif isinstance(interestingness, list):
        return np.mean(interestingness)
    elif isinstance(interestingness, dict):
        return np.mean(list(interestingness.values()))
    else:
        return interestingness


async def investigate(planner, task, utilities=DEFAULT_UTILITIES):
    """ Generate and runs may tasks in parallel, assesses results and generate new tasks if needed.
        Stores data in the database as soon as they ready

    1. gets list of utilities
    2. runs in parallel, store in db as soon as ready
    3. result of the main task is a list of task uuid (children tasks) + interestness

    """
   
    subtasks = generate_tasks(user=task.user,
                           queries = [('analysis',
                                       {'search_query' : task.search_query,
                                        'utility' : u, 'force_refresh' : task.force_refresh}) for u in utilities],
                           parent_id=task.uuid,
                           return_tasks=True)

    task_result = {}
    interestingness = 0.0
    
    for subtask in asyncio.as_completed([planner.execute_and_store(s) for s in subtasks]):
        done_subtask = await subtask
        # the subtask result is already stored, now we have to add subtask into list of task results
        subtask_interestingness = sum_up_interestingness(done_subtask.task_result.interestingness)
        if subtask_interestingness > interestingness:
            interestingness = subtask_interestingness
        task_result[str(done_subtask.uuid)] = subtask_interestingness
        store_results([task], [task_result], set_to_finished=False, interestingness=interestingness)

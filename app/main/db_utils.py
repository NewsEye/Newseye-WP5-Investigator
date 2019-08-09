import uuid
from flask import current_app
from sqlalchemy.exc import IntegrityError
from flask_login import current_user
from app import db
from app.models import Task, TaskInstance, Result
from datetime import datetime
from werkzeug.exceptions import BadRequest
from app.analysis import UTILITY_MAP


def verify_analysis_parameters(query):
    ''' 
    checks the correctness 
    and updates missed parameters with defaults
    '''
    if query[0] != 'analysis':
        return query
    args = query[1]
    if args['utility'] is None:
        raise BadRequest("Required parameter 'utility' missing for query:\n{}".format(query))
    if args['utility'] not in UTILITY_MAP.keys():
        raise BadRequest("Utility '{}' is currently not supported.".format(args['utility']))
    utility_info = UTILITY_MAP[args['utility']].get_description()

    query_parameters = args.get('utility_parameters', {})
    
    new_parameters = {}
    for parameter in utility_info['utility_parameters']:
        parameter_name = parameter['parameter_name']
        if parameter_name in query_parameters.keys():
            new_parameters[parameter_name] = query_parameters[parameter_name]
        else:
            if parameter['parameter_is_required']:
                raise BadRequest(
                    "Required utility parameter '{}' is not defined in the query:\n{}".format(parameter['parameter_name'], query))
            else:
                new_parameters[parameter_name] = parameter['parameter_default']
    new_args = {key: value for key, value in args.items() if key != 'utility_parameters'}
    new_args['utility_parameters'] = new_parameters
    return 'analysis', new_args


def generate_tasks(queries, user=current_user, parent_id=None, return_tasks=False):
    '''
    turns queries into Task objects
    stores them in the database
    returns task objects or task ids
    '''

    if not isinstance(queries, list):
        queries = [queries]

    if not queries:
        return []

    instances = []

    for query in queries:
        if not isinstance(query, tuple):
            raise ValueError('query should be of the type tuple')
        
        task_type, task_parameters = verify_analysis_parameters(query)                      
        utility_name = task_parameters.get('utility', None)
        search_query = task_parameters.get('search_query', task_parameters)
        utility_parameters = task_parameters.get('utility_parameters', {})
        task = Task.query.filter_by(task_type = task_type,
                                    utility_name = utility_name,
                                    search_query = search_query,
                                    utility_parameters = utility_parameters).one_or_none() \
                                    if utility_name != 'comparison' else \
                                       Task.query.filter_by(task_type = task_type,
                                                            utility_name = utility_name,
                                                            utility_parameters = utility_parameters).one_or_none()
                                                                                                              
        input_type = UTILITY_MAP[utility_name].input_type if utility_name else None
        output_type = UTILITY_MAP[utility_name].output_type if utility_name else None
        if not task:
            
            task = Task(task_type = task_type,
                        utility_name = utility_name,
                        search_query = search_query,
                        utility_parameters = utility_parameters,
                        input_type = input_type,
                        output_type = output_type)
            
            # crucial to commit immediately, otherwise task won't have an id 
            db.session.add(task)
            db.session.commit()
            current_app.logger.debug("Created a new task: %s" %task)
            
        task_instance = TaskInstance(task_id =  task.id,
                                     force_refresh   = bool(task_parameters.get('force_refresh', False)),
                                     source_uuid     = task_parameters.get('source_uuid', None),
                                     hist_parent_id=parent_id,
                                     user_id=user.id,
                                     task_status='created')
        

        instances.append(task_instance)

    while True:
        try:
            db.session.add_all(instances)
            db.session.commit()
            break
        except IntegrityError as e:
            current_app.logger.error("Got a UUID collision? Trying with different UUIDs. Exception: {}".format(e))
            db.session.rollback()
            for instance in instances:
                instance.uuid = uuid.uuid4()

    if return_tasks:
        return instances
    else:
        return [instance.uuid for instance in instances]


def store_results(tasks, task_results, set_to_finished=True, interestingness=0.0):
    # Store the new results to the database after everything has been finished
    # TODO: Should we offer the option to store results as soon as they are ready? Or do that by default?
    # Speedier results vs. more sql calls. If different tasks in the same query take wildly different amounts of
    # time, it would make sense to store the finished ones immediately instead of waiting for the last one, but I
    # doubt this would be the case here.
    
    for task, result in zip(tasks, task_results):
        if set_to_finished:
            task.task_finished = datetime.utcnow()
        if isinstance(result, ValueError):
            current_app.logger.error("ValueError: {}".format(result))
            task.task_status = 'failed: {}'.format(result)[:255]
        elif isinstance(result, Exception):
            current_app.logger.error("Unexpected exception: {}".format(result))
            task.task_status = 'failed: Unexpected exception: {}'.format(result)[:255]
        else:
            if set_to_finished:
                task.task_status = 'finished'
            # else update result but keep task running (for investigator)
            
            res = Result.query.filter_by(id=task.result_id).one_or_none()
            
            if not res:
                db.session.commit()
                try:
                    res = Result(id=task.result_id)
                    db.session.add(res)
                    db.session.commit()
                    
                # If another thread created the query in the meanwhile, this should recover from that, and simply overwrite the result with the newest one.
                # If the filter still returns None after IntegrityError, we log the event, ignore the result and continue
                except IntegrityError:
                    db.session.rollback()
                    res = Result.query.filter_by(id=task.result_id).one_or_none()
                    if not res:
                        current_app.logger.error("Unable to create or retrieve Result for {}. Store results failed!".format(task))
                        continue
                    
            # analysis utilities return {'result': ..., 'interestingness': ...}
            # search return just result            
            res.result = result.get('result', result)
            res.interestingness = result.get('interestingness', interestingness)
            res.last_updated = datetime.utcnow()
            res.task_id = task.task_id
            task.result_id = res.id
            task.task.result_id = res.id

    current_app.logger.info("Storing results into database %s" %[str(task.uuid) for task in tasks])
    db.session.commit()



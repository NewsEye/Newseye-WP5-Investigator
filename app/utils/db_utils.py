import uuid
from flask import current_app
from sqlalchemy.exc import IntegrityError
from flask_login import current_user
from app import db
from app.models import Task, Result, Dataset, Processor, SolrQuery
from datetime import datetime
from werkzeug.exceptions import BadRequest


def verify_analysis_parameters(args):
    """ 
    checks the correctness 
    and updates missed parameters with defaults
    """

    if args["processor"] is None:
        raise BadRequest("Required parameter 'processor' missing for query:\n{}".format(query))

    if args["dataset"] is None and args["search_query"] is None and args["source_uuid"] is None:
        raise BadRequest("A 'dataset' or 'source_uuid' is missing for query:\n{}".format(query))

    if args["dataset"] and args["search_query"]:
        raise BadREquest("You cannot specify 'dataset' and 'search query' in the same time")

    if args["dataset"]:
        if not Dataset.query.filter_by(dataset_name=args["dataset"]).one_or_none():
            # TODO: check dataset from Axel's api and insert it into the local db
            raise NotImplementedError("Dataset {} does not exist".format(args["dataset"]))

    processor = Processor.query.filter_by(name=args["processor"]).one_or_none()

    current_app.logger.debug("PROCESSOR: %s" % processor)

    # more than one should have raised an exception already in init
    parameter_info = processor.parameter_info
    query_parameters = args["parameters"]

    new_parameters = {}
    for parameter in parameter_info:
        parameter_name = parameter["name"]
        if parameter_name in query_parameters.keys():
            new_parameters[parameter_name] = query_parameters[parameter_name]
        else:
            if parameter["required"]:
                raise BadRequest(
                    "Required utility parameter '{}' is not defined in the query:\n{}".format(
                        parameter["parameter_name"], query
                    )
                )
            else:
                new_parameters[parameter_name] = parameter["default"]

    new_args = {key: value for key, value in args.items() if key != "parameters"}
    new_args["parameters"] = new_parameters

    return new_args, processor


def get_solr_query(search_query):
    current_app.logger.debug("SEARCH_QUERY %s" % search_query)
    solr_query = SolrQuery.query.filter_by(search_query=search_query).one_or_none()
    current_app.logger.debug("SOLR_QUERY %s" % solr_query)
    if not solr_query:
        solr_query = SolrQuery(search_query=search_query)
        db.session.add(solr_query)
        db.session.commit()
    return solr_query


def commit_task(task, max_try=5):
    for i in range(max_try):
        try:
            db.session.add(task)
            db.session.commit()
            break
        except IntegrityError as e:
            error = e
            current_app.logger.error(
                "Got a UUID collision? Trying with different UUIDs. Exception: {}".format(e)
            )
            db.session.rollback()
            task.uuid = uuid.uuid4()
    else:
        current_app.logger.error("Cannot store to the database Task %s" % Task)
        raise error


def generate_task(query, user=current_user, parent_id=None, return_task=False):
    """
    turns queries into Task objects
    stores them in the database
    returns task objects or task ids
    """

    task_parameters, processor = verify_analysis_parameters(query)

    if task_parameters["dataset"]:
        dataset = Dataset.query.filter_by(dataset_name=task_parameters["dataset"]).first()

        task = Task(
            processor_id=processor.id,
            force_refresh=bool(task_parameters.get("force_refresh", False)),
            user_id=user.id,
            input_data="dataset",
            task_status="created",
            parameters=task_parameters.get("parameters", {}),
            dataset_id=dataset.id,
        )

    elif task_parameters["search_query"]:

        task = Task(
            processor_id=processor.id,
            force_refresh=bool(task_parameters.get("force_refresh", False)),
            user_id=user.id,
            input_data="solr_query",
            task_status="created",
            parameters=task_parameters.get("parameters", {}),
            solr_query=get_solr_query(task_parameters["search_query"]),
        )

    else:
        raise NotImplementedError("Taking a source_uuid as an input is not ready yet")
    # if source_uuid:
    #     source_instance = Task.query.filter_by(uuid=source_uuid).one_or_none()
    #     search_query = source_instance.search_query

    commit_task(task)
    current_app.logger.debug("TASK %s" % task)

    if return_task:
        return task
    else:
        return task.uuid


def store_results(tasks, task_results, set_to_finished=True, interestingness=0.0):
    # Store the new results to the database after everything has been finished
    # TODO: Should we offer the option to store results as soon as they are ready? Or do that by default?
    # Speedier results vs. more sql calls. If different tasks in the same query take wildly different amounts of
    # time, it would make sense to store the finished ones immediately instead of waiting for the last one, but I
    # doubt this would be the case here.

    for task, result in zip(tasks, task_results):

        # current_app.logger.debug("IN STORE_RESULTS: task: %s result: %s" % (task, result))
        if set_to_finished:
            task.task_finished = datetime.utcnow()

        if isinstance(result, ValueError):
            current_app.logger.error("ValueError: {}".format(result))
            task.task_status = "failed"
            task.status_message = "{}".format(result)[:255]
        elif isinstance(result, Exception):
            current_app.logger.error("Unexpected exception: {}".format(result))
            task.task_status = "failed"
            task.status_message = "Unexpected exception: {}".format(result)[:255]
        else:
            if set_to_finished:
                task.task_status = "finished"
            # else update result but keep task running (for investigator)  --- TODO: check if we still need that

            res = Result(
                result=result["result"],
                interestingness=result["interestingness"],
                last_updated=datetime.utcnow(),
                tasks=[task],
            )
            db.session.add(res)
            db.session.commit()

    current_app.logger.info("Storing results into database %s" % [str(task.uuid) for task in tasks])
    db.session.commit()


def make_query_from_dataset(dataset):
    query = {
        "q": "*:*",
        "fq": "{!terms f=id}" + ",".join([doc.solr_id for doc in dataset.documents]),
    }
    return query

import requests
from app import db
from app.models import Report, Task, InvestigatorRun, InvestigatorResult
from config import Config
from flask_login import current_user
import json
from flask import current_app
from pprint import pprint
from uuid import UUID
from werkzeug.exceptions import NotFound, BadRequest


def make_report(args):

    report_language = args["language"]
    report_format = args["format"]
    if args.get("task"):
        uuid = args.get("task")
        Table = Task
    elif args.get("node"):
        uuid = args.get("node")
        Table = InvestigatorResult
    elif args.get("run"):
        uuid = args.get("run")
        Table = InvestigatorRun
    else:
        raise BadRequest("A 'run', 'node' or 'task' must be in a query")

    try:
        uuid = UUID(uuid)
    except ValueError:
        raise NotFound

    record = Table.query.filter_by(uuid=uuid, user_id=current_user.id).first()
    if record is None:
        raise NotFound(
            "{} {} not found for user {}".format(Table.__name__, uuid, current_user.username)
        )
    report = record.report(report_language, report_format)
    if report:
        current_app.logger.info("Report exists, not generating")
    else:
        report = generate_report(record, report_language, report_format)
        current_app.logger.debug("GENERATE: report_content: %s" % report.report_content)
    return report.report_content


def generate_report(record, report_language, report_format):

    if isinstance(record, Task):
        tasks = [record]
    else:
        task_uuids = [task["uuid"] for task in record.result]
        tasks = Task.query.filter(Task.uuid.in_(task_uuids)).all()

    data = [t.dict("reporter") for t in tasks]

    payload = {
        "language": report_language,
        "format": report_format,
        "data": json.dumps(data),
    }

    response = requests.post(Config.REPORTER_URI + "/report", data=payload)

    #    current_app.logger.debug("RESPONSE %s" % response.text)

    report_content = response.json()

    if isinstance(record, Task):
        report = Report(
            report_language=report_language,
            report_format=report_format,
            report_content=report_content,
            result_id=record.task_result.id,
        )
    elif isinstance(record, InvestigatorRun):
        report = Report(
            report_language=report_language,
            report_format=report_format,
            report_content=report_content,
            run_id=record.id,
        )
    elif isinstance(record, InvestigatorResult):
        report = Report(
            report_language=report_language,
            report_format=report_format,
            report_content=report_content,
            node_id=record.id,
        )

    db.session.add(report)
    db.session.commit()

    current_app.logger.debug("Report_Content: %s" % report.report_content)

    return report


def get_languages():
    return requests.get(Config.REPORTER_URI + "/languages").json()


def get_formats():
    return requests.get(Config.REPORTER_URI + "/formats").json()


def get_history(make_tree=True):
    tasks = Task.query.filter_by(user_id=current_user.id)
    user_history = dict(
        zip([task.uuid for task in tasks], [task.dict(style="full") for task in tasks])
    )
    if not make_tree:
        return user_history
    tree = {"root": []}
    if not user_history:
        return tree
    for task in user_history.values():
        parent = task["hist_parent_id"]
        if parent:
            if "children" not in user_history[parent].keys():
                user_history[parent]["children"] = []
            user_history[parent]["children"].append(task)
        else:
            tree["root"].append(task)
    return tree


def get_parents(tasks):
    raise NotImplementedError("Need to update get_parents function for new data structures")

    if not isinstance(tasks, list):
        tasks = [tasks]
    required_tasks = set(tasks)
    for task in tasks:
        current_task = task
        while current_task.source_uuid:
            current_app.logger.debug("SOURCE_UUID: %s" % current_task.source_uuid)
            current_task = Task.query.filter_by(uuid=current_task.source_uuid).first()
            if current_task.task_type == "analysis":
                required_tasks.add(current_task)
    return required_tasks

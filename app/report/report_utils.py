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

# TODO: async reports


def make_report(args):

    report_language = args["language"]
    report_format = args["format"]
    need_links = False if args["nolinks"] else True
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
            "{} {} not found for user {}".format(
                Table.__name__, uuid, current_user.username
            )
        )
    report = record.report(report_language, report_format, need_links)
    if report:
        current_app.logger.info("Report exists, not generating")
        current_app.logger.debug("REPORT: %s" % report.report_content)
    else:
        report = generate_report(record, report_language, report_format, need_links)
        current_app.logger.debug("GENERATE: %s" % report)

    try:
        return report.report_content
    except:
        current_app.logger.debug("REPORT: %s type %s" % (report, type(report)))
        return report


def generate_report(record, report_language, report_format, need_links=True):

    if isinstance(record, Task):
        tasks = [record]
    else:
        task_uuids = [task["uuid"] for task in record.result]
        tasks = Task.query.filter(Task.uuid.in_(task_uuids)).all()

    data = [t.dict("reporter") for t in tasks]

    # current_app.logger.debug("DATA: %s" %json.dumps(data))
    # current_app.logger.debug("NEED LINKS: %s" % need_links)

    payload = {
        "language": report_language,
        "format": report_format,
        "data": json.dumps(data),
        "links": json.dumps(need_links),
    }

    #current_app.logger.debug("PAYLOAD: %s" % json.dumps(payload))
    #json.dump(payload, open("reporter_payload.json", "w"))
        

    
    headers = {"content-type": "application/json"}
    response = requests.post(Config.REPORTER_URI + "/report", payload)

    try:
        report = response.json()
    except Exception as e:
        return {"error": "%s: %s" % (response.status_code, response.reason)}

    report = Report(
        report_language=report.get("language", report_language),
        report_format=report_format,
        report_content={"header": report.get("header"), "body": report.get("body")},
        head_generation_error=report.get("head_generation_error"),
        body_generation_error=report.get("body_generation_error"),
    )

    if isinstance(record, Task):
        report.result_id = record.task_result.id
    elif isinstance(record, InvestigatorRun):
        report.run_id = record.id
    elif isinstance(record, InvestigatorResult):
        report.node_id = record.id

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
    raise NotImplementedError(
        "Need to update get_parents function for new data structures"
    )

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

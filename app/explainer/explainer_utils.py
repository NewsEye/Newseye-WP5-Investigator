import requests, json
from config import Config
from flask_login import current_user
from app import db
from app.models import (
    InvestigatorRun,
    InvestigatorAction,
    Explanation,
    Task,
    TaskExplanation,
)
from uuid import UUID
from werkzeug.exceptions import NotFound, BadRequest, InternalServerError
from flask import current_app


def get_languages():
    return requests.get(Config.EXPLAINER_URI + "/languages").json()


def get_formats():
    return requests.get(Config.EXPLAINER_URI + "/formats").json()


def make_reason(why):
    current_app.logger.debug("WHY: %s" % why)
    try:
        why["name"] = why.pop("reason")
    except KeyError:
        why["name"] = "unknown"
    return why


def make_task(task):
    if task["parameters"]:
        return {
            "parameters": task["parameters"],
            "name": task["processor"],
            "uuid": task["uuid"],
        }
    else:
        return {"name": task["processor"], "uuid": task["uuid"]}


def make_explanation(args):
    if "task" in args:
        return make_task_explanation(args)
    else:
        return make_run_explanation(args)


def find_object(Table, uuid):
    #current_app.logger.debug("Table: %s" % Table)
    #current_app.logger.debug("uuid: %s" % uuid)
    #obj = Table.query.filter_by(uuid=uuid, user_id=current_user.id).first()
    obj = Table.query.filter_by(uuid=uuid).first()
    if obj is None:
        raise NotFound(
            "{} {} not found for user {}".format(
                Table.__name__, uuid, current_user.username
            )
        )
    return obj


def make_task_explanation(args):
    explanation_language = args["language"]
    explanation_format = args["format"]
    run_uuid = args["run"]
    task_uuid = args["task"]

    try:
        task_uuid = UUID(task_uuid)
        run_uuid = UUID(run_uuid)
    except:
        raise BadRequest

    task = find_object(Task, task_uuid)
    current_app.logger.debug("TASK: %s" % task)

    explanation = task.explanation(explanation_language, explanation_format)
    if explanation:
        current_app.logger.info(
            "Explanation for task %s exists, not generating" % task_uuid
        )
        current_app.logger.debug("Explanation: %s" % explanation.explanation_content)
    else:
        run = find_object(InvestigatorRun, run_uuid)
        explanation = generate_task_explanation(
            task, run, explanation_language, explanation_format
        )
        current_app.logger.debug("GENERATE: %s" % explanation)

    return explanation.explanation_content


def make_run_explanation(args):
    explanation_language = args["language"]
    explanation_format = args["format"]
    run_uuid = args["run"]

    try:
        run_uuid = UUID(run_uuid)
    except:
        raise BadRequest

    run = find_object(InvestigatorRun, run_uuid)

    explanation = run.explanation(explanation_language, explanation_format)
    if explanation:
        current_app.logger.info(
            "Explanation for run %s exists, not generating" % run_uuid
        )
        current_app.logger.debug("Explanation: %s" % explanation.explanation_content)
    else:
        explanation = generate_explanation(
            run, explanation_language, explanation_format
        )
        current_app.logger.debug("GENERATE: %s" % explanation)

    return explanation.explanation_content


def generate_explanation(run, explanation_language, explanation_format):
    actions = get_run_actions(run.id)
    data = []
    for action in actions:
        if action["action_type"] in ["initialize", "update"]:
            tasks = get_action_tasks(action)
            if not tasks:
                continue

            action_id = action["id"]
            why = make_reason(action["why"])

            for task in tasks:
                data.append({"id": action_id, "reason": why, "task": make_task(task)})

    explanation = request_explanation(
        data, explanation_format, explanation_language, run.uuid
    )

    explanation = Explanation(
        explanation_language=explanation.get("language", explanation_language),
        explanation_format=explanation.get("format", explanation_format),
        explanation_content={"body": explanation.get("body")},
        generation_error=explanation.get("error"),
        run_id=run.id,
    )

    db.session.add(explanation)
    db.session.commit()

    return explanation


def request_explanation(data, explanation_format, explanation_language, run_uuid):

    payload = {
        "format": explanation_format,
        "language": explanation_language,
        "data": data,
        "run_uuid": str(run_uuid),
    }

    # current_app.logger.debug("PAYLOAD: %s" %json.dumps(payload))

    response = requests.post(
        Config.EXPLAINER_URI + "/report/json", data=json.dumps(payload)
    )

    try:
        explanation = response.json()
    except:
        return {"error": "%s: %s" % (response.status_code, response.reason)}

    return explanation


def get_run_actions(run_id):
    ret = []
    for action in InvestigatorAction.query.filter_by(run_id=run_id).all():
        if not action.action_type in ["initialize", "update"]:
            continue
        actions = action.action
        whys = action.why
        if not isinstance(whys, list):
            actions = [actions]
            whys = [whys]
            assert len(actions) == len(whys)
        for w, a in zip(whys, actions):
            ret.append(
                {
                    "id": action.id,
                    "action_type": action.action_type,
                    "action": a,
                    "why": w,
                }
            )
    return ret


def get_action_tasks(action):
    assert action["action_type"] in ["initialize", "update"]
    ret = []
    action = action["action"]
    if action:
        tasks = action.get("tasks_added_to_q")
        if tasks:
            ret += tasks
    return ret


def get_action_task_uuids(action):
    tasks = get_action_tasks(action)
    return [t["uuid"] for t in tasks]


def generate_task_explanation(task, run, explanation_language, explanation_format):
    payload_data = []
    actions = get_run_actions(run.id)
    task_to_action = {}
    for action in actions:
        if action["action_type"] in ["initialize", "update"]:
            uuids = get_action_task_uuids(action)
            task_to_action.update({u: action for u in uuids})

    tasks_to_explain = [str(task.uuid)]
    tasks_explained = []

    while tasks_to_explain:
        uuid = tasks_to_explain.pop()
        if uuid in tasks_explained or uuid == "root":
            continue
        tasks_explained.append(uuid)
        try:
            current_task = Task.query.filter_by(uuid=uuid).first()
        except Exception as e:
            current_app.logger.debug("UUID: %s" % uuid)
            raise e
        task_action = task_to_action[str(uuid)]

        task_dict = {"name": task.processor.name, "uuid": str(uuid)}
        if task.parameters:
            task_dict["parameters"] = task.parameters

        payload_data.append(
            {
                "id": task_action["id"],
                "reason": make_reason(task_action["why"]),
                "task": task_dict,
            }
        )

        tasks_to_explain += [p.uuid for p in current_task.parents]
        for c in current_task.collections:
            tasks_to_explain += c.origin

        current_app.logger.debug("TASKS_TO_EXPLAIN %s" % tasks_to_explain)

    explanation = request_explanation(
        payload_data, explanation_format, explanation_language, run.uuid
    )

    explanation = TaskExplanation(
        explanation_language=explanation.get("language", explanation_language),
        explanation_format=explanation.get("format", explanation_format),
        explanation_content={"body": explanation.get("body")},
        generation_error=explanation.get("error"),
        task_id=task.id,
    )

    db.session.add(explanation)
    db.session.commit()

    return explanation

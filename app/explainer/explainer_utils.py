import requests, json
from config import Config
from flask_login import current_user
from app.models import InvestigatorRun, InvestigatorAction
from uuid import UUID
from werkzeug.exceptions import NotFound, BadRequest
from flask import current_app

def get_languages():
    return requests.get(Config.EXPLAINER_URI + "/languages").json()


def get_formats():
    return requests.get(Config.EXPLAINER_URI + "/formats").json()

def make_reason(why):
    why["name"] = why.pop("reason")
    return why

def make_task(task):
    if task["parameters"]:
        return {"parameters":task["parameters"],
                "name":task["processor"],
                "uuid":task["uuid"]}
    else:
        return {"name":task["processor"],
                "uuid":task["uuid"]}

def make_explanation(args):

    explanation_language = args["language"]
    explanation_format = args["format"]
    run_uuid = args["run"]

    try:
        run_uuid = UUID(run_uuid)
    except:
        raise NotFound
    
    run = InvestigatorRun.query.filter_by(uuid=run_uuid, user_id=current_user.id).first()
    actions = InvestigatorAction.query.filter_by(run_id=run.id).all()
    data = []
    for action in actions:
        if action.action_type in ["initialize", "update"]:
            action_id = action.action_id
            why = make_reason(action.why)
            tasks = action.action['tasks_added_to_q']

            current_app.logger.debug("ACTION: %d REASON: %s TASKS: %s" %(action_id, why, [t["processor"] for t in tasks]))

            for task in tasks:
                data.append({"id":action_id,
                             "reason":why,
                             "task":make_task(task)})


    payload = {"format":explanation_format,
               "language":explanation_language,
               "data":data,
               "run_uuid":str(run_uuid)}

    #current_app.logger.debug("PAYLOAD: %s" %json.dumps(payload))
    #current_app.logger.debug("URI: %s" %Config.EXPLAINER_URI + "/report/json")
        
    response = requests.post(Config.EXPLAINER_URI + "/report/json", data=json.dumps(payload))
    
    try:
        explanation = response.json()
    except:
        return {"error": "%s: %s" % (response.status_code, response.reason)}

    # TODO: store explanation to DB
    
    return explanation

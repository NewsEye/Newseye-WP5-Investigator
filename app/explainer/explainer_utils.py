import requests, json
from config import Config
from flask_login import current_user
from app import db
from app.models import InvestigatorRun, InvestigatorAction, Explanation
from uuid import UUID
from werkzeug.exceptions import NotFound, BadRequest
from flask import current_app

def get_languages():
    return requests.get(Config.EXPLAINER_URI + "/languages").json()


def get_formats():
    return requests.get(Config.EXPLAINER_URI + "/formats").json()

def make_reason(why):
    try:
        why["name"] = why.pop("reason")
    except KeyError:
        why["name"] = "unknown"
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
    if run is None:
        raise NotFound(
            "{} {} not found for user {}".format(
                InvestigatorRun.__name__, run_uuid, current_user.username
            )
        )

    explanation = run.explanation(explanation_language, explanation_format)
    if explanation:
        current_app.logger.info("Explanation exists, not generating")
        current_app.logger.debug("Explanation: %s" % explanation.explanation_content)
    else:
        explanation = generate_explanation(run, explanation_language, explanation_format)
        current_app.logger.debug("GENERATE: %s" % explanation)

    return explanation.explanation_content
  
    


def generate_explanation(run, explanation_language, explanation_format): 
    actions = InvestigatorAction.query.filter_by(run_id=run.id).all()
    data = []
    for action in actions:
        if action.action_type in ["initialize", "update"]:
            tasks = action.action.get('tasks_added_to_q')
            if not tasks:
                continue
            
            action_id = action.action_id
            why = make_reason(action.why)
            
            current_app.logger.debug("ACTION: %d REASON: %s TASKS: %s" %(action_id, why, [t["processor"] for t in tasks]))

            for task in tasks:
                data.append({"id":action_id,
                             "reason":why,
                             "task":make_task(task)})


    payload = {"format":explanation_format,
               "language":explanation_language,
               "data":data,
               "run_uuid":str(run.uuid)}

    #current_app.logger.debug("PAYLOAD: %s" %json.dumps(payload))
    #current_app.logger.debug("URI: %s" %Config.EXPLAINER_URI + "/report/json")
        
    response = requests.post(Config.EXPLAINER_URI + "/report/json", data=json.dumps(payload))
    
    try:
        explanation = response.json()
    except:
        return {"error": "%s: %s" % (response.status_code, response.reason)}


    explanation = Explanation(
        explanation_language = explanation.get("language", explanation_language),
        explanation_format = explanation.get("format", explanation_format),
        explanation_content = {"body": explanation.get("body")},
        generation_error=explanation.get("error"),
        )

    explanation.run_id = run.id

    db.session.add(explanation)
    db.session.commit()

    return explanation

import requests
from app import db
from app.models import Report, Task
from config import Config
from flask_login import current_user
import json


def generate_report(task, report_language, report_format):
    payload = {
        'language': report_language,
        'format': report_format,
        'data': json.dumps({'root': [t.dict('reporter') for t in get_parents(task)]})
    }
    response = requests.post(Config.REPORTER_URI + "/report", data=payload)
    report_content = response.json()
    task_report = Report(report_language=report_language,
                         report_format=report_format,
                         task_uuid=task.uuid,
                         report_content=report_content)
    db.session.add(task_report)
    db.session.commit()
    return task_report


def get_languages():
    return requests.get(Config.REPORTER_URI + "/languages").json()


def get_formats():
    return requests.get(Config.REPORTER_URI + "/formats").json()


def get_history(make_tree=True):
    tasks = Task.query.filter_by(user_id=current_user.id)
    user_history = dict(zip([task.uuid for task in tasks], [task.dict(style='full') for task in tasks]))
    if not make_tree:
        return user_history
    tree = {'root': []}
    if not user_history:
        return tree
    for task in user_history.values():
        parent = task['hist_parent_id']
        if parent:
            if 'children' not in user_history[parent].keys():
                user_history[parent]['children'] = []
            user_history[parent]['children'].append(task)
        else:
            tree['root'].append(task)
    return tree


def get_parents(tasks):
    if not isinstance(tasks, list):
        tasks = [tasks]
    required_tasks = set(tasks)
    for task in tasks:
        current_task = task
        while current_task.target_uuid:
            current_task = Task.query.filter_by(uuid=current_task.target_uuid).first()
            if current_task.task_type == 'analysis':
                required_tasks.add(current_task)
    return required_tasks

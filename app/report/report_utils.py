# import requests
from app import db
from app.models import Report, Task
# from config import Config
from flask_login import current_user


# A placeholder for the real report generation functionality, see below for the real one
def generate_report(task, report_language, report_format):
    report_content = {
        'language': report_language,
        'body': "<{}>...</{}>".format(report_format, report_format),
        'head': "<h1>Report for {}</h1>".format(task)
    }
    task_report = Report(report_language=report_language,
                         report_format=report_format,
                         task_uuid=task.uuid,
                         report_content=report_content)
    db.session.add(task_report)
    task.task_report = task_report
    db.session.commit()
    return task_report


# This is the real version to use after the Reporter comes online
# def generate_report(task, report_language, report_format):
#     payload = {
#         'language': report_language,
#         'format': report_format,
#         'data': task.task_result.result
#     }
#     response = requests.post(Config.REPORTER_URI + "/report", json=payload)
#     report_content = response.json()
#     task_report = Report(report_language=report_language,
#                          report_format=report_format,
#                          task_uuid=task.uuid,
#                          report_content=report_content)
#     db.session.add(task_report)
#     db.session.commit()
#     return task_report


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
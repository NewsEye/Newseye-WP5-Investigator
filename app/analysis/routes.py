from flask import request, jsonify, current_app
from flask_login import login_required, current_user
from app.main import controller
from app.analysis import bp
from app.models import Task
from app.main.analysis_utils import UTILITY_LIST


@bp.route('/', methods=['GET'])
@login_required
def get_analysis_tasks():
    tasks = [task.dict(style='result') for task in Task.query.filter_by(user_id=current_user.id, task_type='analysis').all()]
    if len(tasks) == 1:
        tasks = tasks[0]
    return jsonify(tasks)


@bp.route('/', methods=['POST'])
@login_required
def start_analysis_task():
    # POST: Start a new analysis task, and return its basic information to the user
    query = request.json
    if not isinstance(query, list):
        query = [query]
    query = [('analysis', item) for item in query if item.get('target_uuid') or item.get('target_search')]
    if not query:
        return '''Target data is not specified. Include either a 'target_uuid' or 'target_search' parameter.''', 400
    for item in query:
        if item[1].get('utility') is None:
            return '''Required parameter 'utility' missing for request {}'''.format(item[1]), 400
    try:
        results = [task.dict() for task in controller.execute_tasks(query)]
        # If any of the tasks is not finished, the status code is set to 202, otherwise it is 200
        status = 200
        for task in results:
            if task['task_status'] != 'finished':
                status = 202
                break
        if len(results) == 1:
            results = results[0]
        return jsonify(results), status
    except KeyError as e:
        current_app.logger.exception(e)
        return 'Missing parameter for chosen analysis utility', 400
    except Exception as e:
        current_app.logger.exception(e)
        return 'Something went wrong...', 500


@bp.route('/<string:task_uuid>')
@login_required
def get_analysis_task(task_uuid):
    task = Task.query.filter_by(uuid=task_uuid, user_id=current_user.id, task_type='analysis').first()
    if task is None:
        return 'Task {} not found for user {}'.format(task_uuid, current_user.username), 404
    return jsonify(task.dict(style='result'))


# TODO: Do this properly instead of using the hardcoded hack of a tool list
@bp.route('/utilities/')
@login_required
def get_utility_list():
    return jsonify(UTILITY_LIST), 200

from flask import request, jsonify, current_app
from flask_login import login_required, current_user
from app.main import core
from app.models import Task
from app.search import bp


@bp.route('/', methods=['POST'])
@login_required
def start_search_task():
    query = request.json
    if isinstance(query, list):
        query = [('search', item) for item in query]
    else:
        query = [('search', query)]
    try:
        results = [task.dict() for task in core.execute_tasks(query)]
        status = 200
        for task in results:
            if task['task_status'] != 'finished':
                status = 202
                break
        if len(results) == 1:
            results = results[0]
        return jsonify(results), status
    except Exception as e:
        current_app.logger.exception(e)
        return 'Something went wrong...', 500


@bp.route('/', methods=['GET'])
@login_required
def get_search_tasks():
    tasks = [task.dict(style='result') for task in Task.query.filter_by(user_id=current_user.id, task_type='search').all()]
    if len(tasks) == 1:
        tasks = tasks[0]
    return jsonify(tasks)


@bp.route('/<string:task_uuid>', methods=['GET'])
@login_required
def get_search_task(task_uuid):
    task = Task.query.filter_by(uuid=task_uuid, user_id=current_user.id, task_type='search').first()
    if task is None:
        return 'Task {} not found for user {}'.format(task_uuid, current_user.username), 404
    return jsonify(task.dict(style='result'))

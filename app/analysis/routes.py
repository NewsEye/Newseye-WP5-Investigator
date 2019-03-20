from flask import request, jsonify, current_app
from flask_login import login_required
from app.main import core
from app.analysis import bp
from app.models import Task


@bp.route('/', methods=['GET', 'POST'])
@login_required
def analyze():
    if request.method == 'GET':
        try:
            task_ids = core.run_query_task(('analysis', request.args.to_dict()), return_tasks=False)
            response = core.get_tasks_by_task_id(task_ids)
            return jsonify(list(response.values()))
        except TypeError as e:
            current_app.logger.exception(e)
            return 'Invalid tool name or invalid number of arguments for the chosen tool', 400
    if request.method == 'POST':
        try:
            arguments = request.json
            task_ids = core.run_query_task(('analysis', arguments), return_tasks=False)
            response = {'task_id': task_ids[0]}
            response.update(arguments)
            return jsonify(response)
        except KeyError as e:
            current_app.logger.exception(e)
            return 'Missing parameter for chosen analysis tool', 400
        except Exception as e:
            current_app.logger.exception(e)
            return 'Something went wrong...', 500


@bp.route('/<string:task_id>')
def analysis(task_id):
    task = Task.query.filter_by(uuid=task_id).first()
    if task is None:
        return 'Invalid task_id', 400
    return jsonify(task.dict())

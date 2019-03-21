from flask import request, jsonify, current_app
from flask_login import login_required
from app.main import core
from app.analysis import bp
from app.models import Task
from app.main.analysis_tools import TOOL_LIST


@bp.route('/', methods=['GET', 'POST'])
@login_required
def analyze():
    if request.method == 'GET':
        query = request.args.to_dict()
        query = ('analysis', query)
    if request.method == 'POST':
        query = request.json
        if isinstance(query, list):
            query = [('analysis', item) for item in query]
        else:
            query = ('analysis', query)
    try:
        results = [task.dict() for task in core.run_query_task(query)]
        for task in results:
            if task['task_status'] != 'finished':
                status = 202
                break
        else:
            status = 200
        if len(results) == 1:
            results = results[0]
        return jsonify(results), status
    except KeyError as e:
        current_app.logger.exception(e)
        return 'Missing parameter for chosen analysis tool', 400
    except Exception as e:
        current_app.logger.exception(e)
        return 'Something went wrong...', 500


@bp.route('/<string:task_id>')
@login_required
def analysis(task_id):
    task = Task.query.filter_by(uuid=task_id).first()
    if task is None:
        return 'Invalid task_id', 400
    return jsonify(task.dict())


# TODO: Do this properly instead of using the hardcoded hack of a tool list
@bp.route('/tools/')
@login_required
def tools():
    return jsonify(TOOL_LIST), 200

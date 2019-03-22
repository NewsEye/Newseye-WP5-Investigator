from flask import request, jsonify, current_app
from flask_login import login_required
from app.main import core
from app.search import bp


@bp.route('/', methods=['GET', 'POST'])
@login_required
def search():
    if request.method == 'GET':
        query = request.args.to_dict(flat=False)
        query = ('search', query)
    if request.method == 'POST':
        query = request.json
        if isinstance(query, list):
            query = [('search', item) for item in query]
        else:
            query = ('search', query)
    try:
        results = [task.dict(style='result') for task in core.run_query_task(query)]
        for task in results:
            if task['task_status'] != 'finished':
                status = 202
                break
        else:
            status = 200
        if len(results) == 1:
            results = results[0]
        return jsonify(results), status
    except Exception as e:
        current_app.logger.exception(e)
        return 'Something went wrong...', 500

from flask import request, jsonify, current_app
from flask_login import login_required
from app.assistant import core
from app.search import bp


@bp.route('/search')
@login_required
def search():
    query = request.args.to_dict(flat=False)
    try:
        results = core.run_query_task(('search', query))
    except Exception as e:
        current_app.logger.exception(e)
        return 'Something went wrong...', 500
    results = [task.dict() for task in results]
    try:
        for task in results:
            if task['task_status'] != 'finished':
                return jsonify(results), 202
        return jsonify(results)
    except Exception as e:
        current_app.logger.exception(e)
        return 'Something went wrong...', 500

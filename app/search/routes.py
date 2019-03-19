from flask import request, jsonify, current_app
from flask_login import login_required
from app.assistant import core
from app.search import bp


@bp.route('/search')
@login_required
def search():
    query = request.args.to_dict(flat=False)
    try:
        result = core.run_query_task(('search', query))[0].dict()
    except Exception as e:
        current_app.logger.exception(e)
        return 'Something went wrong...', 500
    try:
        if result['task_status'] != 'finished':
            return jsonify(result), 202
        return jsonify(result)
    except Exception as e:
        current_app.logger.exception(e)
        return 'Something went wrong...', 500

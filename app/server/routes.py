from flask import session, request, jsonify, current_app
from app.assistant import core
from app.server import bp


@bp.route('/search')
def quick_query():
    if 'username' not in session:
        return 'You are not logged in', 401
    username = session['username']
    query = request.args.to_dict(flat=False)
    try:
        results = core.run_query_task(username, ('query', query))
    except Exception as e:
        current_app.logger.exception(e)
        return 'Something went wrong...', 500
    try:
        for result in results:
            if result['task_status'] != 'finished':
                return jsonify(results), 202
        return jsonify(results)
    except Exception as e:
        current_app.logger.exception(e)
        return 'Something went wrong...', 500


@bp.route('/api/analysis', methods=['GET', 'POST'])
def analyze():
    if 'username' not in session:
        return 'You are not logged in', 401
    username = session['username']
    if request.method == 'GET':
        try:
            task_ids = core.run_query_task(username, ('analysis', request.args.to_dict()), return_tasks=False)
            response = core.get_tasks_by_task_id(task_ids)
            return jsonify(list(response.values()))
        except TypeError as e:
            current_app.logger.exception(e)
            return 'Invalid tool name or invalid number of arguments for the chosen tool', 400
    if request.method == 'POST':
        try:
            arguments = request.json
            username = arguments.pop('username')
            task_ids = core.run_query_task(username, ('analysis', arguments), return_tasks=False)
            response = {'task_id': task_ids[0], 'username': username}
            response.update(arguments)
            return jsonify(response)
        except KeyError as e:
            current_app.logger.exception(e)
            return 'Missing parameter for chosen analysis tool', 400
        except Exception as e:
            current_app.logger.exception(e)
            return 'Something went wrong...', 500


@bp.route('/api/analysis/<string:task_id>')
def get_results(task_id):
    try:
        result = core.get_results(task_id)
        return jsonify(result[task_id])
    except TypeError as e:
        current_app.logger.exception(e)
        return 'Invalid task_id', 400


@bp.route('/api/history')
def get_history():
    if 'username' not in session:
        return 'You are not logged in', 401
    username = session['username']
    history = core.get_history(username)
    return jsonify(history)


@bp.route('/test/multiquery')
def test_multiquery():
    test_query = [
        {'q': ['lighthouse']},
        {'q': ['ghost']}
    ]
    if 'username' not in session:
        return 'You are not logged in', 401
    username = session['username']
    return jsonify(core.run_query_task(username, test_query))

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
        current_app.loggerexception(e)
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
            current_app.loggerexception(e)
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
            current_app.loggerexception(e)
            return 'Missing parameter for chosen analysis tool', 400
        except Exception as e:
            current_app.loggerexception(e)
            return 'Something went wrong...', 500


@bp.route('/api/analysis/<string:task_id>')
def get_results(task_id):
    try:
        result = core.get_results(task_id)
        return jsonify(result[task_id])
    except TypeError as e:
        current_app.loggerexception(e)
        return 'Invalid task_id', 400


@bp.route('/api/login')
def login():
    if 'username' in session:
        return 'User {} already logged in!'.format(session['username']), 401
    username = request.args.get('username')
    if username is None:
        return "Missing parameter: username", 400
    try:
        last_login = core.login_user(username)
    except IndexError as e:
        return 'Invalid username {}!'.format(username), 401
    except Exception as e:
        current_app.loggerexception(e)
        return 'Something went wrong...', 500
    session['username'] = username
    return 'Welcome, {}. Last login: {}'.format(username, last_login), 200


@bp.route('/api/add_user')
def add_user():
    if 'username' not in session:
        return 'You are not logged in', 401
    username = session['username']
    new_user = request.args.get('new_user')
    if new_user is None:
        return "Missing parameter: new_user", 400
    try:
        core.add_user(username, new_user)
        return 'User {} created'.format(new_user), 200
    except TypeError as e:
        current_app.loggerexception(e)
        return 'Cannot add user {}: username already in use!'.format(new_user), 400
    except ValueError as e:
        current_app.loggerexception(e)
        # Todo: fix this as well when adding proper admin users!
        return "Only user 'admin' can add new users!", 401
    except Exception as e:
        current_app.loggerexception(e)
        return 'Something went wrong...', 500


# Logout. Ignores all parameters.
@bp.route('/api/logout')
def logout():
    if 'username' not in session:
        return 'You are not logged in', 401
    username = session.pop('username')
    return 'Goodbye, {}'.format(username), 200


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

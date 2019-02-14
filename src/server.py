from service import AssistantService
from flask import Flask, redirect, url_for, request, jsonify, session
from flask_cors import CORS


#
# START INIT
#

# Logging
import logging
formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
log = logging.getLogger('root')
log.setLevel(logging.DEBUG)
#log.setLevel(5) # Enable for way too much logging, even more than DEBUG
log.addHandler(handler)

# Flask
app = Flask(__name__)
app.secret_key = '1234'

# cors = CORS(app)

service = AssistantService()


#
# END INIT
#




@app.route('/search')
def quick_query():
    if 'username' not in session:
        return 'You are not logged in', 401
    username = session['username']
    query = request.args.to_dict(flat=False)
    try:
        results = service.core.run_query_task(username, ('query', query))
    except Exception as e:
        log.exception(e)
        return 'Something went wrong...', 500
    try:
        for result in results:
            if result['task_status'] != 'finished':
                return jsonify(results), 202
        return jsonify(results)
    except Exception as e:
        log.exception(e)
        return 'Something went wrong...', 500


@app.route('/api/analysis', methods=['GET', 'POST'])
def analyze():
    if 'username' not in session:
        return 'You are not logged in', 401
    username = session['username']
    if request.method == 'GET':
        try:
            result = service.core.run_query_task(username, ('analysis', request.args.to_dict()))
            return jsonify(result)
        except TypeError as e:
            log.exception(e)
            return 'Invalid tool name or invalid number of arguments for the chosen tool', 400
    if request.method == 'POST':
        try:
            arguments = request.json
            username = arguments.pop('username')
            task_ids = service.core.run_query_task(username, ('analysis', arguments), return_tasks=False)
            response = {'task_id': task_ids[0], 'username': username}
            response.update(arguments)
            return jsonify(response)
        except KeyError as e:
            log.exception(e)
            return 'Missing parameter for chosen analysis tool', 400
        except Exception as e:
            log.exception(e)
            return 'Something went wrong...', 500


@app.route('/api/analysis/<string:task_id>')
def get_results(task_id):
    try:
        result = service.core.get_results(task_id)
        return jsonify(result[task_id])
    except TypeError as e:
        log.exception(e)
        return 'Invalid task_id', 400


@app.route('/api/login')
def login():
    if 'username' in session:
        return 'User {} already logged in!'.format(session['username']), 401
    username = request.args.get('username')
    if username is None:
        return "Missing parameter: username", 400
    try:
        last_login = service.core.login_user(username)
    except IndexError as e:
        return 'Invalid username {}!'.format(username), 401
    except Exception as e:
        log.exception(e)
        return 'Something went wrong...', 500
    session['username'] = username
    return 'Welcome, {}. Last login: {}'.format(username, last_login), 200


@app.route('/api/add_user')
def add_user():
    if 'username' not in session:
        return 'You are not logged in', 401
    username = session['username']
    new_user = request.args.get('new_user')
    if new_user is None:
        return "Missing parameter: new_user", 400
    try:
        service.core.add_user(username, new_user)
        return 'User {} created'.format(new_user), 200
    except TypeError as e:
        log.exception(e)
        return 'Cannot add user {}: username already in use!'.format(new_user), 400
    except ValueError as e:
        log.exception(e)
        # Todo: fix this as well when adding proper admin users!
        return "Only user 'admin' can add new users!", 401
    except Exception as e:
        log.exception(e)
        return 'Something went wrong...', 500


# Logout. Ignores all parameters.
@app.route('/api/logout')
def logout():
    if 'username' not in session:
        return 'You are not logged in', 401
    username = session.pop('username')
    return 'Goodbye, {}'.format(username), 200


@app.route('/api/history')
def get_history():
    if 'username' not in session:
        return 'You are not logged in', 401
    username = session['username']
    history = service.core.get_history(username)
    return jsonify(history)


@app.route('/test/multiquery')
def test_multiquery():
    test_query = [
        {'q': ['lighthouse']},
        {'q': ['ghost']}
    ]
    if 'username' not in session:
        return 'You are not logged in', 401
    username = session['username']
    return jsonify(service.core.run_query_task(username, test_query))


def main():
    app.run(host='0.0.0.0')


if __name__ == '__main__':
    main()

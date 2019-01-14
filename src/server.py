from service import AssistantService
from flask import Flask, redirect, url_for, request, jsonify, session
from flask_cors import CORS


#
# START INIT
#

# Logging
# import logging
# formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
# handler = logging.StreamHandler()
# handler.setFormatter(formatter)
# log = logging.getLogger('root')
# log.setLevel(logging.INFO)
# #log.setLevel(5) # Enable for way too much logging, even more than DEBUG
# log.addHandler(handler)

# Flask
app = Flask(__name__)
app.secret_key = '1234'

# cors = CORS(app)

service = AssistantService()


#
# END INIT
#


@app.route('/api/query', methods=['POST', 'GET', 'DELETE'])
def query():
    if 'username' not in session:
        return 'You are not logged in', 401
    username = session['username']
    if request.method == 'GET':
        return jsonify(service.core.get_query(username))
    elif request.method == 'POST':
        new_query = request.get_json()
        if not _is_valid_query(new_query):
            return 'Invalid query', 400
        if 'q' not in new_query.keys():
            new_query['q'] = ''
        service.core.set_query(username, new_query)
        return jsonify(service.core.get_query(username))
    elif request.method == 'DELETE':
        service.core.clear_query(username)
        return '', 204


@app.route('/api/state/<string:state_id>')
def change_state(state_id):
    if 'username' not in session:
        return 'You are not logged in', 401
    username = session['username']
    new_state = service.core.get_state(username, state_id)
    service.core.set_state(username, new_state)
    return jsonify(new_state)


@app.route('/catalog.json')
def passthrough():
    if 'username' not in session:
        return 'You are not logged in', 401
    username = session['username']
    query = request.args.to_dict(flat=False)
    try:
        result = service.core.run_query(username, query, switch_state=False, store_results=False)
    except TypeError:
        session.pop('username')
        return 'You are not logged in', 401
    return jsonify(result.result)


@app.route('/search')
def quick_query():
    if 'username' not in session:
        return 'You are not logged in', 401
    username = session['username']
    query = request.args.to_dict(flat=False)
    try:
        results = service.core.run_query(username, query)
    except Exception:
        print(Exception)
        return 'Something went wrong...', 500
    try:
        for result in results:
            if 'message' in result.result.keys():
                return jsonify(results), 202
        return jsonify(results)
    except Exception:
        print(Exception)
        return 'Something went wrong...', 500


@app.route('/api/analysis')
def analyze():
    if 'username' not in session:
        return 'You are not logged in', 401
    username = session['username']
    try:
        current_state = service.core.run_analysis(username, request.args)
        return jsonify(current_state)
    except TypeError:
        print(TypeError)
        return 'Invalid number of arguments for the chosen tool', 400
    except Exception:
        print(Exception)
        return 'Something went wrong...', 500


@app.route('/api/login')
def login():
    if 'username' in session:
        return 'User {} already logged in!'.format(session['username']), 401
    username = request.args.get('username')
    if username is None:
        return "Missing parameter: username", 400
    session['username'] = username
    try:
        last_login = service.core.login_user(username)
    except IndexError:
        return 'Invalid username {}!'.format(username), 401
    except Exception:
        print(Exception)
        return 'Something went wrong...', 500
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
    except TypeError:
        return 'Cannot add user {}: username already in use!'.format(new_user), 400
    except ValueError:
        # Todo: fix this as well when adding proper admin users!
        return "Only user 'jariavik' can add new users!", 401
    except Exception:
        print(Exception)
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
    return jsonify(service.core.run_query(username, test_query))


@app.route('/test/analysis/topic')
def test_topic_analysis():
    if 'username' not in session:
        return 'You are not logged in', 401
    username = session['username']
    return jsonify(service.core.topic_analysis(username))


def _is_valid_query(query):
    for key in query.keys():
        if type(query[key]) != list:
            return False
    return True


def main():
    app.run(host='0.0.0.0')


if __name__ == '__main__':
    main()

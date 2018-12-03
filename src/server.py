from service import AssistantService
import sys
import argparse
from flask import Flask, redirect, url_for, request, jsonify, session
from flask_cors import CORS


#
# START INIT
#

# CLI parameters
parser = argparse.ArgumentParser(description='Run the Assistant server.')
parser.add_argument('port', type=int, default=8080, help='port number to attach to')
args = parser.parse_args()
sys.argv = sys.argv[0:1]

# Logging
import logging
formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
log = logging.getLogger('root')
log.setLevel(logging.INFO)
#log.setLevel(5) # Enable for way too much logging, even more than DEBUG
log.addHandler(handler)

# Flask
app = Flask(__name__)
app.secret_key = '1234'

cors = CORS(app)

service = AssistantService()


#
# END INIT
#


@app.route('/api/query', methods=['POST', 'GET', 'DELETE'])
def query():
    if 'user_id' not in session:
        return 'You are not logged in', 401
    user_id = session['user_id']
    if request.method == 'GET':
        return jsonify(service.core.get_query(user_id))
    elif request.method == 'POST':
        new_query = request.get_json()
        if not _is_valid_query(new_query):
            return 'Invalid query', 400
        if 'q' not in new_query.keys():
            new_query['q'] = ''
        service.core.set_query(user_id, new_query)
        return jsonify(service.core.get_query(user_id))
    elif request.method == 'DELETE':
        service.core.clear_query(user_id)
        return '', 204


@app.route('/api/state/<string:state_id>')
def get_state(state_id):
    if 'user_id' not in session:
        return 'You are not logged in', 401
    user_id = session['user_id']
    return jsonify(service.core.get_state(user_id, state_id))


@app.route('/catalog.json')
def passthrough():
    if 'user_id' not in session:
        return 'You are not logged in', 401
    user_id = session['user_id']
    query = request.args.to_dict(flat=False)
    try:
        service.core.set_query(user_id, query)
        result = service.core.run_query(user_id, threaded=False, switch_state=False)
    except TypeError:
        session.pop('user_id')
        return 'You are not logged in', 401
    return jsonify(result.query_results)


@app.route('/search')
def quick_query():
    if 'user_id' not in session:
        return 'You are not logged in', 401
    user_id = session['user_id']
    query = request.args.to_dict(flat=False)
    try:
        service.core.set_query(user_id, query)
        result = service.core.run_query(user_id)
    except TypeError:
        session.pop('user_id')
        print(session)
        return 'You are not logged in', 401
    if 'message' in result.query_results.keys():
        return jsonify(result), 202
    return jsonify(result)


@app.route('/api/analysis')
def analyze():
    if 'user_id' not in session:
        return 'You are not logged in', 401
    user_id = session['user_id']
    try:
        current_state = service.core.run_analysis(user_id, request.args)
    except TypeError:
        print(TypeError)
        return 'Invalid number of arguments for the chosen tool', 400
    except Exception:
        print(Exception)
        return 'Something went wrong...', 500
    return jsonify(current_state)


@app.route('/api/login')
def login():
    username = request.args.get('username')
    if username is None:
        return "Missing parameter: username", 400
    session['user_id'] = username
    try:
        service.core.add_user(session['user_id'])
    except TypeError:
        return 'You are already logged in', 403
    except Exception:
        print(Exception)
        return 'Something went wrong', 500
    return 'Welcome {}'.format(username), 200


# Logout. Ignores all parameters.
@app.route('/api/logout')
def logout():
    if 'user_id' not in session:
        return 'You are not logged in', 401
    try:
        service.core.forget_user(session['user_id'])
    except Exception:
        print(Exception)
        return 'Something went wrong', 500
    session.pop('user_id', None)
    return 'Goodbye', 200


@app.route('/api/history')
def get_history():
    if 'user_id' not in session:
        return 'You are not logged in', 401
    user_id = session['user_id']
    history = service.core.get_history(user_id)
    return jsonify(history)


@app.route('/test/multiquery')
def test_multiquery():
    test_query = [
        {'q': 'lighthouse'},
        {'q': 'ghost'}
    ]
    if 'user_id' not in session:
        return 'You are not logged in', 401
    user_id = session['user_id']
    service.core.set_query(user_id, test_query)
    return jsonify(service.core.run_multiquery(user_id))


@app.route('/test/analysis/topic')
def test_topic_analysis():
    if 'user_id' not in session:
        return 'You are not logged in', 401
    user_id = session['user_id']
    return jsonify(service.core.topic_analysis(user_id))


def _empty_query():
    return {
        'q': '',
    }


def _is_valid_query(query):
    for key in query.keys():
        if type(query[key]) != list:
            return False
    return True


def main():
    log.info("Starting with options port={}".format(args.port))
    app.run(host='0.0.0.0', port=args.port)
    log.info("Stopping")


if __name__ == '__main__':
    main()

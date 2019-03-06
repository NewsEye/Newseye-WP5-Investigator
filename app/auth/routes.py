from flask import session, request, current_app
from app.assistant import core
from app.server import bp


@bp.route('/login')
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
        current_app.logger.exception(e)
        return 'Something went wrong...', 500
    session['username'] = username
    return 'Welcome, {}. Last login: {}'.format(username, last_login), 200


@bp.route('/add_user')
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
        current_app.logger.exception(e)
        return 'Cannot add user {}: username already in use!'.format(new_user), 400
    except ValueError as e:
        current_app.logger.exception(e)
        # Todo: fix this as well when adding proper admin users!
        return "Only user 'admin' can add new users!", 401
    except Exception as e:
        current_app.logger.exception(e)
        return 'Something went wrong...', 500


# Logout. Ignores all parameters.
@bp.route('/logout')
def logout():
    if 'username' not in session:
        return 'You are not logged in', 401
    username = session.pop('username')
    return 'Goodbye, {}'.format(username), 200

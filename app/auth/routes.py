from flask import request
from flask_login import login_user, logout_user, current_user, login_required
from datetime import datetime
from app import db
from app.auth import bp
from app.models import User


@bp.route("/login")
def login():
    if current_user.is_authenticated:
        return "User {} already logged in!".format(current_user.username), 401
    username = request.args.get("username")
    if username is None:
        return "Missing parameter: username", 400
    user = User.query.filter_by(username=username).first()
    if user is None:
        return "Invalid username or password!", 401
    last_login = user.last_seen
    login_user(user)
    current_user.last_seen = datetime.utcnow()
    db.session.commit()
    response = "Welcome, {}. Last login: {}".format(user.username, last_login)
    return response, 200


@bp.route("/logout")
@login_required
def logout():
    username = current_user.username
    logout_user()
    return "Goodbye, {}".format(username), 200


@bp.route("/add_user", methods=["POST"])
@login_required
def add_user():
    if current_user.username != "admin":
        # Todo: fix this as well when adding proper admin users!
        return "Only user 'admin' can add new users!", 401
    new_user = request.json.get("new_user")
    if new_user is None:
        return "Missing parameter: new_user", 400

    user = User.query.filter_by(username=new_user).first()
    if user:
        return "Cannot add user {}: username already in use!".format(new_user), 400
    user = User(username=new_user)
    db.session.add(user)
    db.session.commit()
    return "{} added to database".format(user), 200

from datetime import datetime
from werkzeug.http import http_date
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
import uuid
import jwt
from jwt.exceptions import ExpiredSignatureError, InvalidSignatureError
from flask import current_app
from flask_login import UserMixin
from app import db, login
from config import Config


class User(UserMixin, db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True)
    created_on = db.Column(db.DateTime, default=datetime.utcnow)
    last_seen = db.Column(db.DateTime, default=datetime.utcnow)
    all_tasks = db.relationship('Task', back_populates='user', lazy='dynamic', foreign_keys="Task.user_id")

    def __repr__(self):
        return '<User {}>'.format(self.username)


class Result(db.Model):
    __tablename__ = 'results'
    id = db.Column(db.Integer, primary_key=True)
    task_type = db.Column(db.String(255), nullable=False)
    task_parameters = db.Column(JSONB, nullable=False)
    result = db.Column(db.JSON)
    last_updated = db.Column(db.DateTime, default=datetime.utcnow)
    __table_args__ = (UniqueConstraint('task_type', 'task_parameters', name='uq_results_task_type_task_parameters'),)

    def __repr__(self):
        return '<Result {}: {}>'.format(self.task_type, self.task_parameters)


class Report(db.Model):
    __tablename__ = 'reports'
    id = db.Column(db.Integer, primary_key=True)
    task_uuid = db.Column(UUID(as_uuid=True), db.ForeignKey('tasks.uuid'))
    report_language = db.Column(db.String(255))
    report_format = db.Column(db.String(255))
    report_content = db.Column(db.JSON)
    report_generated = db.Column(db.DateTime, default=datetime.utcnow)
    # TODO: Add cascading delete when the task is deleted
    task = db.relationship('Task', back_populates='task_reports', foreign_keys=[task_uuid])

    def __repr__(self):
        return '<Report>'


class Task(db.Model):
    # TODO: columns for target_uuid and utility name
    
    
    __tablename__ = 'tasks'
    id = db.Column(db.Integer, primary_key=True)
    # external id
    uuid = db.Column(UUID(as_uuid=True), unique=True, nullable=False, default=uuid.uuid4)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'))

    # search history of a user
    # currently not used
    # to make top-level relations between tasks
    hist_parent_id = db.Column(UUID(as_uuid=True), db.ForeignKey('tasks.uuid'))

    # search/analysis
    task_type = db.Column(db.String(255), nullable=False)
    task_parameters = db.Column(JSONB, nullable=False)

    # force refresh: if True executes analysis utility once again, if False tries to find result from DB
    force_refresh = db.Column(db.Boolean)
    
    # created/running/finished/failed
    task_status = db.Column(db.String(255))

    # parent task
    target_uuid = db.Column(UUID(as_uuid=True), db.ForeignKey('tasks.uuid'))
    
    # timestamps
    task_started = db.Column(db.DateTime, default=datetime.utcnow)
    task_finished = db.Column(db.DateTime)
    last_accessed = db.Column(db.DateTime, default=datetime.utcnow)

    user = db.relationship('User', back_populates='all_tasks', foreign_keys=[user_id])

    # shortcuts for searching children given parents
    hist_children = db.relationship('Task', primaryjoin="Task.uuid==Task.hist_parent_id")

    # result
    # search in the Result table a result with the same type and the same parameters
    # parameters are json object, might be slow (in the future)

    task_result = db.relationship('Result', primaryjoin="and_(foreign(Task.task_type)==Result.task_type, foreign(Task.task_parameters)==Result.task_parameters)")

    # generated by Reporter
    task_reports = db.relationship('Report', back_populates='task', foreign_keys="Report.task_uuid")

    # different versions of the output
    def dict(self, style='status'):
        if style == 'status':
            return {
                'uuid': str(self.uuid),
                'task_type': self.task_type,
                'task_parameters': self.task_parameters,
                'task_status': self.task_status,
                'task_started': http_date(self.task_started),
                'task_finished': http_date(self.task_finished),
            }
        elif style == 'result':
            return {
                'uuid': str(self.uuid),
                'task_type': self.task_type,
                'task_parameters': self.task_parameters,
                'task_status': self.task_status,
                'task_started': http_date(self.task_started),
                'task_finished': http_date(self.task_finished),
                'task_result': self.task_result.result if self.task_result else None,
            }
        elif style == 'full':
            return {
                'uuid': str(self.uuid),
                'task_type': self.task_type,
                'task_parameters': self.task_parameters,
                'task_status': self.task_status,
                'task_result': self.task_result.result if self.task_result else None,
                'hist_parent_id': self.hist_parent_id,
                'task_started': http_date(self.task_started),
                'task_finished': http_date(self.task_finished),
                'last_accessed': http_date(self.last_accessed),
            }
        elif style == 'reporter':
            return {
                'uuid': str(self.uuid),
                'task_type': self.task_type,
                'task_parameters': self.task_parameters,
                'task_status': self.task_status,
                'task_result': self.task_result.result if self.task_result else None,
                'hist_parent_id': str(self.hist_parent_id),
                'task_started': http_date(self.task_started),
                'task_finished': http_date(self.task_finished),
                'last_accessed': http_date(self.last_accessed),
            }
        else:
            raise KeyError('''Unknown value for parameter 'style'! Valid options: status, result, full. ''')

    def __repr__(self):
        return '<Task {}: {}>'.format(self.task_type, self.task_parameters)


# Needed by flask_login
@login.user_loader
def load_user(id):
    return User.query.get(int(id))


# User login using a Bearer Token, if it exists
@login.request_loader
def load_user_from_request(request):
    token = request.headers.get('Authorization')
    if token is None:
        return None
    if token[:4] == 'JWT ':
        token = token.replace('JWT ', '', 1)
        try:
            decoded = jwt.decode(token, Config.SECRET_KEY, algorithm='HS256')
        except (ExpiredSignatureError, InvalidSignatureError):
            return None
        user = User.query.filter_by(username=decoded['username']).first()
        if not user:
            user = User(username=decoded['username'])
            db.session.add(user)
            current_app.logger.info("Added new user '{}' to the database".format(user.username))
        else:
            user.last_seen = datetime.utcnow()
        db.session.commit()
        return user
    return None

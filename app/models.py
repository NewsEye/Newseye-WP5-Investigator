from datetime import datetime
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
import uuid
from flask_login import UserMixin
from app import db, login


class User(UserMixin, db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True)
    created_on = db.Column(db.DateTime, default=datetime.utcnow)
    last_seen = db.Column(db.DateTime, default=datetime.utcnow)
    current_task_id = db.Column(db.Integer, db.ForeignKey('tasks.id'))
    all_tasks = db.relationship('Task', back_populates='user', lazy='dynamic', foreign_keys="Task.user_id")
    current_task = db.relationship('Task', foreign_keys=[current_task_id])

    def __repr__(self):
        return '<User {}>'.format(self.username)


class Query(db.Model):
    __tablename__ = 'queries'
    id = db.Column(db.Integer, primary_key=True)
    query_type = db.Column(db.String(255), nullable=False)
    query_parameters = db.Column(JSONB, nullable=False)
    query_result = db.Column(db.JSON)
    last_updated = db.Column(db.DateTime, default=datetime.utcnow)
    last_accessed = db.Column(db.DateTime, default=datetime.utcnow)
    __table_args__ = (UniqueConstraint('query_type', 'query_parameters', name='uq_queries_query_type_query_parameters'),)

    def __repr__(self):
        return '<Query {}: {}>'.format(self.query_type, self.query_parameters)


# TODO: Move status to queries
class Task(db.Model):
    __tablename__ = 'tasks'
    id = db.Column(db.Integer, primary_key=True)
    uuid = db.Column(UUID(as_uuid=True), unique=True, nullable=False, default=uuid.uuid4())
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    hist_parent_id = db.Column(UUID(as_uuid=True), db.ForeignKey('tasks.uuid'))
    data_parent_id = db.Column(UUID(as_uuid=True), db.ForeignKey('tasks.uuid'))
    query_type = db.Column(db.String(255), nullable=False)
    query_parameters = db.Column(JSONB, nullable=False)
    task_status = db.Column(db.String(255))
    last_updated = db.Column(db.DateTime, default=datetime.utcnow)
    last_accessed = db.Column(db.DateTime, default=datetime.utcnow)
    user = db.relationship('User', back_populates='all_tasks', foreign_keys=[user_id])
    hist_children = db.relationship('Task', primaryjoin="Task.uuid==Task.hist_parent_id")
    data_children = db.relationship('Task', primaryjoin="Task.uuid==Task.data_parent_id")
    task_result = db.relationship('Query', primaryjoin="and_(foreign(Task.query_type)==Query.query_type, foreign(Task.query_parameters)==Query.query_parameters)")

    def dict(self):
        return {
            'uuid': self.uuid,
            'username': self.user.username,
            'query_type': self.query_type,
            'query_parameters': self.query_parameters,
            'task_status': self.task_status,
            'task_result': self.task_result.query_result if self.task_result else None,
            'hist_parent_id': self.hist_parent_id,
            'data_parent_id': self.data_parent_id,
        }

    def __repr__(self):
        return '<Task {}: {}>'.format(self.query_type, self.query_parameters)


@login.user_loader
def load_user(id):
    return User.query.get(int(id))

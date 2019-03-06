from datetime import datetime
from sqlalchemy.dialects.postgresql import UUID, JSONB
import uuid
from hashlib import md5
from time import time
from flask import current_app
# from werkzeug.security import generate_password_hash, check_password_hash
# import jwt
from app import db


class User(db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True)
    created_on = db.Column(db.DateTime, default=datetime.utcnow)
    last_seen = db.Column(db.DateTime, default=datetime.utcnow)
    tasks = db.relationship('Task', backref='user', lazy='dynamic')

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

    def __repr__(self):
        return '<Query {}>'.format(self.id)


class Task(db.Model):
    __tablename__ = 'tasks'
    id = db.Column(db.Integer, primary_key=True)
    uuid = db.Column(UUID(as_uuid=True), unique=True, nullable=False, default=uuid.uuid4())
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    prev_id = db.Column(db.Integer, db.ForeignKey('tasks.id'))
    next_tasks = db.relationship('Task', backref=db.backref('prev_task', remote_side=[id]), lazy='dynamic')
    query_type = db.Column(db.String(255), nullable=False)
    query_parameters = db.Column(JSONB, nullable=False)
    task_status = db.Column(db.String(255))
    last_updated = db.Column(db.DateTime, default=datetime.utcnow)
    last_accessed = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return '<Task {}>'.format(self.uuid)

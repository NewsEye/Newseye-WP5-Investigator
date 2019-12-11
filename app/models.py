from datetime import datetime
from werkzeug.http import http_date
from sqlalchemy import UniqueConstraint, Integer, ForeignKey
from sqlalchemy.dialects.postgresql import UUID, JSONB
import uuid
import jwt
from jwt.exceptions import ExpiredSignatureError, InvalidSignatureError
from flask import current_app
from flask_login import UserMixin
from app import db, login
from config import Config
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class User(UserMixin, db.Model):
    # currently user is always the same, th edemonstrator
    # we might need smth more precise, in coordination w/ Axel
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True)
    created_on = db.Column(db.DateTime, default=datetime.utcnow)
    last_seen = db.Column(db.DateTime, default=datetime.utcnow)
    all_tasks = db.relationship('TaskInstance', back_populates='user', lazy='dynamic', foreign_keys="TaskInstance.user_id")

    def __repr__(self):
        return '<User {}>'.format(self.username)



document_dataset_association = db.Table('doc_dataset', Base.metadata,
                                        db.Column('dataset_id', Integer, ForeignKey('dataset.id')),
                                        db.Column('document_id', Integer, ForeignKey('document.id'))
                                        )
    

class Dataset(db.Model):
    __tablename__ = 'dataset'
    id = db.Column(db.Integer, primary_key=True)
    dataset_name = db.Column(db.String(255))
    __table_args__ = (UniqueConstraint('dataset_name', name='uq_dataset_name'),)
    creation_history = db.relationship('DatasetOperations', back_populates='dataset')
    documents = db.relationship('Document',
                                secondary = document_dataset_association,
                                back_populates = 'datasets')
    tasks = db.relationship("Task", back_populates="dataset")

    
class DatasetOperations(db.Model):
    # this table could be used to reconstruct the dataset using all operations one by one
    # later on we can also do some reasoning using these operations
    __tablename__ = 'dataset_operations'
    id = db.Column(db.Integer, primary_key=True)
    operation = db.Column(db.String(255)) # create, add, remove, drop
    search_query = db.Column(JSONB)
    # these are documents that where explicitly added/deleted to/from the dataset, by user
    documents = db.Column(db.String(255))

    dataset_id = db.Column(Integer, ForeignKey('dataset.id'))
    dataset = db.relationship('Dataset', back_populates='creation_history')

    
class Document(db.Model):
    __tablename__ = 'documents'
    id = db.Column(db.Integer, primary_key=True)
    # name use in the main Solr database
    solr_id = db.Column(db.String(255))
    __table_args__ = (UniqueConstraint('solr_id', name='uq_solr_id'),)
    datasets = db.relationship('Dataset',
                                secondary = document_dataset_association,
                                back_populates = 'datasets')        
    
    
class Task(db.Model):
    __tablename__ = 'tasks'
    id = db.Column(db.Integer, primary_key=True)
    processor = db.Column(db.String(255))
    parameters = db.Column(JSONB)
    dataset_id = db.Column(Integer, ForeignKey('dataset.id'))
    dataset = db.relationship('Dataset', back_populates='tasks')
    task_results = db.relationship('Result', back_populates='task', foreign_keys="Result.task_id")
    task_instances = db.relationship('TaskInstance', back_populates='task', foreign_keys="TaskInstance.task_id")
    __table_args__ = (UniqueConstraint('processor', 'parameters', 'dataset_id',
                                       name='uq_processor_parameters_dataset'),)

    @property
    def task_result(self):
        if self.task_results:
            return sorted(self.task_results, key=lambda r: r.last_updated)[-1]
    


class TaskInstance(db.Model):
    __tablename__ = "task_instances"
    id = db.Column(db.Integer, primary_key=True)
    
    # external id
    uuid = db.Column(UUID(as_uuid=True), unique=True, nullable=False, default=uuid.uuid4)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    task_id = db.Column(db.Integer, db.ForeignKey('tasks.id'))
    task = db.relationship('Task', back_populates='task_instances', foreign_keys=[task_id])
    
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    user = db.relationship('User', back_populates='all_tasks', foreign_keys=[user_id])

    # force refresh: if True executes analysis utility once again, if False tries to find result from DB
    force_refresh = db.Column(db.Boolean)

    # created/running/finished/failed
    task_status = db.Column(db.String(255))
    
    # timestamps
    task_started  = db.Column(db.DateTime, default=datetime.utcnow)
    task_finished = db.Column(db.DateTime)

    # result: keeps result for a current user even if result in Task table already updated
    result_id = db.Column(db.Integer, db.ForeignKey('results.id'))
    
    @property
    def task_result(self):
        if self.result_id:
            return next((result for result in self.task.task_results if result.id == self.result_id), None)
        else:
            the_most_recent_result = self.task.task_result
            if the_most_recent_result:
                if not self.force_refresh:
                    self.result_id = the_most_recent_result.id
                return the_most_recent_result

    @property
    def task_report(self):
        result = self.task_result
        if result:
            reports = result.result_reports
            if reports:
                return sorted(reports, key=lambda r: r.report_generated)[-1]

    @property
    def result_with_interestingness(self):
        if self.task_result:
            return {'result' : self.task_result.result,
                    'interestingness' : self.task_result.interestingness}

        
class Result(db.Model):
    __tablename__ = 'results'
    id = db.Column(db.Integer, primary_key=True)
    task_id = db.Column(db.Integer, db.ForeignKey('tasks.id'))
    task = db.relationship('Task', back_populates='task_results', foreign_keys=[task_id])
    result = db.Column(db.JSON)
    interestingness = db.Column(db.JSON)
    last_updated = db.Column(db.DateTime, default=datetime.utcnow)
    result_reports = db.relationship('Report', back_populates='result', foreign_keys='Report.result_id')
    
    def __repr__(self):
        return '<Result id: {} task: {} date: {}>'.format(self.id, self.task_id, self.last_updated)



class Report(db.Model):
    __tablename__ = 'reports'
    id = db.Column(db.Integer, primary_key=True)

    result_id = db.Column(db.Integer, db.ForeignKey('results.id'))
    result = db.relationship('Result', back_populates='result_reports', foreign_keys=[result_id])

    report_language = db.Column(db.String(255))
    report_format = db.Column(db.String(255))
    report_content = db.Column(db.JSON)
    report_generated = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return '<Report>'

    
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

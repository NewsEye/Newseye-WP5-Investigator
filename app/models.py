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
from sqlalchemy.dialects import postgresql

Base = declarative_base()

class User(UserMixin, db.Model):
    # currently user is always the same, th edemonstrator
    # we might need smth more precise, in coordination w/ Axel
    __tablename__ = 'user'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True)
    created_on = db.Column(db.DateTime, default=datetime.utcnow)
    last_seen = db.Column(db.DateTime, default=datetime.utcnow)
    all_tasks = db.relationship('Task', back_populates='user')

    def __repr__(self):
        return '<User {}>'.format(self.username)

   

document_dataset_relation = db.Table('document_dataset_relation',
                                     db.Column('dataset_id', db.Integer, db.ForeignKey('dataset.id'), primary_key=True),
                                     db.Column('document_id', db.Integer, db.ForeignKey('document.id'), primary_key=True))

    
class Document(db.Model):
    __tablename__ = 'document'
    id = db.Column(db.Integer, primary_key=True)
    # name used in the main Solr database
    solr_id = db.Column(db.String(255))
    __table_args__ = (UniqueConstraint('solr_id', name='uniq_solr_id'),)
    datasets = db.relationship("Dataset", secondary=document_dataset_relation, back_populates = 'documents')
   
class Dataset(db.Model):
    __tablename__ = 'dataset'
    id = db.Column(db.Integer, primary_key=True)
    dataset_name = db.Column(db.String(255))
    __table_args__ = (UniqueConstraint('dataset_name', name='uq_dataset_name'),)
    documents = db.relationship("Document", secondary=document_dataset_relation, back_populates = 'datasets')
    creation_history = db.relationship('DatasetTransformation', back_populates='dataset')
    tasks = db.relationship("Task", back_populates="dataset")

    
class DatasetTransformation(db.Model):
    # this table could be used to reconstruct the dataset using all transformations one by one
    # later on we can also do some reasoning using these transformations
    __tablename__ = 'dataset_transformation'
    id = db.Column(db.Integer, primary_key=True)
    transformation = db.Column(db.Enum("create", "add", "remove", "drop", name="transformation"), nullable=False)
    search_query = db.Column(JSONB)
    # these are documents that where explicitly added/deleted to/from the dataset
    # the dataset contains other documents, defined via search queries, but they are stored in document table
    document = db.Column(db.String(255))
    dataset_id = db.Column(Integer, ForeignKey('dataset.id'))
    dataset = db.relationship('Dataset', foreign_keys=[dataset_id], back_populates='creation_history')

 
class Processor(db.Model):
     __tablename__ = 'processor'
     id = db.Column(db.Integer, primary_key=True)
     name = db.Column(db.String(255))
     parameters = db.Column(JSONB)
     input_type = db.Column(db.String(255))
     output_type = db.Column(db.String(255))
     description = db.Column(db.String(10000))
     import_path = db.Column(db.String(1024))
     tasks = db.relationship('Task', back_populates='processor')
     __table_args__ = (UniqueConstraint('name', 'import_path', name='uq_processor_name_and_path'),)
     

task_parent_child_relation = db.Table('task_parent_child_relation',
                                     db.Column('parent_id', db.Integer, db.ForeignKey('task.id'), primary_key=True),
                                     db.Column('child_id', db.Integer, db.ForeignKey('task.id'), primary_key=True))

     
class Task(db.Model):
    __tablename__ = 'task'
    id = db.Column(db.Integer, primary_key=True)

    parents = db.relationship("Task", secondary=task_parent_child_relation,
                              primaryjoin=task_parent_child_relation.c.child_id==id,
                              secondaryjoin=task_parent_child_relation.c.parent_id==id,
                              backref="children")
    
    processor_id = db.Column(Integer, ForeignKey('processor.id'), nullable=False)
    processor = db.relationship('Processor', foreign_keys=[processor_id], back_populates='tasks')
    parameters = db.Column(JSONB)
    dataset_id = db.Column(Integer, ForeignKey('dataset.id'))
    dataset = db.relationship('Dataset', back_populates='tasks')

    __table_args__ = (UniqueConstraint('processor_id', 'parameters', 'dataset_id',
                                       'user_id',  # TODO: reuse results from other users
                                       name='uq_processor_parameters_dataset'),)

    uuid = db.Column(UUID(as_uuid=True), unique=True, nullable=False, default=uuid.uuid4)
    
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    user = db.relationship('User', back_populates='all_tasks', foreign_keys=[user_id])
    
    # force refresh: if True executes analysis utility once again, if False tries to find result from DB
    # mostly for debugging
    force_refresh = db.Column(db.Boolean)

    # created/running/finished/failed
    task_status = db.Column(db.Enum("created", "running", "finished", "failed", name="task_status"))
    
    # timestamps
    task_started  = db.Column(db.DateTime, default=datetime.utcnow)
    task_finished = db.Column(db.DateTime)

    task_results = db.relationship('Result', back_populates='task')
    result_id = db.Column(db.Integer) 
    
    @property
    def task_result(self):
        if self.result_id:
            # ??? is there a way to query result directly by id?
            return next((result for result in self.task.task_results if result.id == self.result_id), None)
        else:
            if self.task_results:
                the_most_recent_result = sorted(self.task_results, key=lambda r: r.last_updated)[-1]
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
    __tablename__ = 'result'
    id = db.Column(db.Integer, primary_key=True)
    task_id = db.Column(db.Integer, db.ForeignKey('task.id'))
    task = db.relationship('Task', back_populates='task_results', foreign_keys=[task_id])
    result = db.Column(db.JSON)
    interestingness = db.Column(db.JSON)
    last_updated = db.Column(db.DateTime, default=datetime.utcnow)
    result_reports = db.relationship('Report', back_populates='result')
    
    def __repr__(self):
        return '<Result id: {} task: {} date: {}>'.format(self.id, self.task_id, self.last_updated)


class Report(db.Model):
    __tablename__ = 'report'
    id = db.Column(db.Integer, primary_key=True)

    result_id = db.Column(db.Integer, db.ForeignKey('result.id'))
    result = db.relationship('Result', back_populates='result_reports', foreign_keys=[result_id])

    report_language = db.Column(db.String(255))
    report_format = db.Column(db.String(255))
    report_content = db.Column(db.JSON)
    report_generated = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return '<Report>'

    
class InvestigatorRun(db.Model):
    # TODO: make explicit relations w other tables
    __tablename__ = 'investigator_run'
    id = db.Column(db.Integer, primary_key=True)
    root_dataset_id = db.Column(db.Integer, db.ForeignKey('dataset.id'))
    user_parameters = db.Column(db.JSON)
    run_status = db.Column(db.String(255))
    final_result = db.Column(db.JSON)
    
    # ??? done_tasks = None # relation to tasks; might be redundant with action information

    root_action_id = db.Column(db.Integer, db.ForeignKey('investigator_action.id')) 


    
class InvestigatorAction(db.Model):
    __tablename__ = 'investigator_action'
    id = db.Column(db.Integer, primary_key=True)
    run_id = db.Column(db.Integer, db.ForeignKey('investigator_run.id'))
    action_id = db.Column(db.Integer) # step number inside run
    __table_args__ = (UniqueConstraint('run_id', 'action_id', name='uq_run_and_action'),)

    action_type = db.Column(db.Enum("initialize", "select", "execute", "update", "stop", name="action_type"))

    input_queue = db.Column(postgresql.ARRAY(db.Integer)) # task ids
    
    why = db.Column(db.JSON)

    action = db.Column(db.JSON) # might be assosiated with some tasks
    # INITIALIZE: list of initial tasks
    # SELECT: action contains list of selected tasks and their place in the input, output_queue
    # EXECUTE: list of tasks that should be executed
    # UPDATE: all the modifications: add/remove/insert/permute
    
    output_queue = db.Column(postgresql.ARRAY(db.Integer)) 
    
    
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
        user = User.query.filter_by(username=decoded['username']).one_or_none()
        if not user:
            user = User(username=decoded['username'])
            db.session.add(user)
            current_app.logger.info("Added new user '{}' to the database".format(user.username))
        else:
            user.last_seen = datetime.utcnow()
        db.session.commit()
        return user
    return None

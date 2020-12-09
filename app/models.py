from datetime import datetime
from werkzeug.http import http_date
from sqlalchemy import UniqueConstraint, Integer, ForeignKey
from sqlalchemy.dialects.postgresql import UUID, JSONB, ARRAY
from sqlalchemy.ext.declarative import declarative_base
import uuid
import jwt
from jwt.exceptions import ExpiredSignatureError, InvalidSignatureError
from flask import current_app
from flask_login import UserMixin
from app import db, login
from config import Config

Base = declarative_base()


class User(UserMixin, db.Model):
    # currently user is always the same, th edemonstrator
    # we might need smth more precise, in coordination w/ Axel
    __tablename__ = "user"
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True)
    created_on = db.Column(db.DateTime, default=datetime.utcnow)
    last_seen = db.Column(db.DateTime, default=datetime.utcnow)
    all_tasks = db.relationship("Task", back_populates="user")
    all_runs = db.relationship("InvestigatorRun", back_populates="user")

    def __repr__(self):
        return "<User {}>".format(self.username)


class DocumentDatasetRelation(db.Model):
    __tablename__ = "document_dataset_relation"
    dataset_id = db.Column(db.Integer, db.ForeignKey("dataset.id"))
    document_id = db.Column(db.Integer, db.ForeignKey("document.id"))
    db.Column("relevance", db.Integer, nullable=False)
    dataset = db.relationship("Dataset", back_populates="documents")
    document = db.relationship("Document", back_populates="datasets")
    relevance = db.Column(db.Integer)
    __table_args__ = (db.PrimaryKeyConstraint("dataset_id", "document_id"),)


class Document(db.Model):
    # currently not used, just store document names
    # in principle it might be possible to store link to task for document-level tasks
    # and also store document facets, e.g. language
    __tablename__ = "document"
    id = db.Column(db.Integer, primary_key=True)
    # name used in the main Solr database
    solr_id = db.Column(db.String(255))
    __table_args__ = (UniqueConstraint("solr_id", name="uniq_solr_id"),)
    datasets = db.relationship("DocumentDatasetRelation", back_populates="document")


class Dataset(db.Model):
    __tablename__ = "dataset"
    id = db.Column(db.Integer, primary_key=True)
    dataset_name = db.Column(db.String(255))
    user = db.Column(db.String(255))
    __table_args__ = (
        UniqueConstraint("dataset_name", "user", name="uq_dataset_name_and_user"),
    )
    documents = db.relationship("DocumentDatasetRelation", back_populates="dataset")
    hash_value = db.Column(db.String(255), nullable=False)
    created_on = db.Column(db.DateTime, default=datetime.utcnow)
    tasks = db.relationship("Task", back_populates="dataset")

    def __repr__(self):
        return "<Dataset {}, dataset_name: {}, {} documents, {} tasks>".format(
            self.id, self.dataset_name, len(self.documents), len(self.tasks)
        )

    def make_query(self):
        return {
            "q": "*:*",
            "fq": "{!terms f=id}"
            + ",".join([d.document.solr_id for d in self.documents]),
        }


class SolrQuery(db.Model):
    __tablename__ = "solr_query"
    id = db.Column(db.Integer, primary_key=True)
    search_query = db.Column(JSONB)
    # we expect one output, but there could be more due to database versions
    solr_outputs = db.relationship("SolrOutput", back_populates="solr_query")
    tasks = db.relationship("Task", back_populates="solr_query")

    def solr_output(self, retrieve):
        if self.solr_outputs:
            # return the first one with correct retrive
            return sorted(
                [o for o in self.solr_outputs if o.retrieve == retrieve],
                key=lambda o: o.last_updated,
            )[-1]

    def __repr__(self):
        return "<SolrQuery {}, search_query {}>".format(self.id, self.search_query)


class SolrOutput(db.Model):
    # not used at the moment
    # in the future some queries might be too heavy, better to store localy
    __tablename__ = "solr_output"
    id = db.Column(db.Integer, primary_key=True)
    output = db.Column(JSONB, nullable=False)
    retrieve = db.Column(
        db.Enum(
            "default", "all", "facets", "docids", "tokens", "stems", name="retrieve"
        )
    )
    last_updated = db.Column(db.DateTime, default=datetime.utcnow)
    solr_query_id = db.Column(Integer, ForeignKey("solr_query.id"), nullable=False)
    solr_query = db.relationship(
        "SolrQuery", foreign_keys=[solr_query_id], back_populates="solr_outputs"
    )


class Processor(db.Model):
    __tablename__ = "processor"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    parameter_info = db.Column(JSONB)

    # TODO: enum for input/output types, to have more control
    # TODO: prerequisite processors
    input_type = db.Column(db.String(255), nullable=False)
    # dataset
    output_type = db.Column(db.String(255), nullable=False)
    # facet_list, word_list, bigram_list, timeseries

    description = db.Column(db.String(10000))
    import_path = db.Column(db.String(1024))
    tasks = db.relationship("Task", back_populates="processor")

    deprecated = db.Column(db.Boolean, default=False)

    def __repr__(self):
        return "<Processor id: {}, name: {}, import_path: {}>".format(
            self.id, self.name, self.import_path
        )

    def dict(self):
        return {
            "name": self.name,
            "description": self.description,
            "parameters": self.parameter_info,
            "input_type": self.input_type,
            "output_type": self.output_type,
        }

    @staticmethod
    def find_by_name(name):
        processors = [
            p for p in Processor.query.filter_by(name=name).all() if not p.deprecated
        ]
        if processors:
            if len(processors) > 1:
                raise NotImplementedError("More than one processor with name %s" % name)
            return processors[0]


task_parent_child_relation = db.Table(
    "task_parent_child_relation",
    db.Column("parent_id", db.Integer, db.ForeignKey("task.id"), primary_key=True),
    db.Column("child_id", db.Integer, db.ForeignKey("task.id"), primary_key=True),
)

task_result_relation = db.Table(
    # many-to-many relation
    # a task may output more than one result
    # if task is rerun (e.g. as a part of another investigator's run) then we add a link into result table instead of copying result, which could be quite heavy
    "task_result_relation",
    db.Column("task_id", db.Integer, db.ForeignKey("task.id"), primary_key=True),
    db.Column("result_id", db.Integer, db.ForeignKey("result.id"), primary_key=True),
)


task_collection_relation = db.Table(
    # many-to-many relation
    "task_collection_relation",
    db.Column("task_id", db.Integer, db.ForeignKey("task.id"), primary_key=True),
    db.Column(
        "collection_id", db.Integer, db.ForeignKey("collection.id"), primary_key=True
    ),
)


class Task(db.Model):
    __tablename__ = "task"
    id = db.Column(db.Integer, primary_key=True)

    parents = db.relationship(
        "Task",
        secondary=task_parent_child_relation,
        primaryjoin=task_parent_child_relation.c.child_id == id,
        secondaryjoin=task_parent_child_relation.c.parent_id == id,
        backref="children",
    )

    processor_id = db.Column(Integer, ForeignKey("processor.id"), nullable=False)
    processor = db.relationship(
        "Processor", foreign_keys=[processor_id], back_populates="tasks"
    )
    parameters = db.Column(JSONB)

    # a task takes as an input a dataset or a solr query, never both
    dataset_id = db.Column(Integer, ForeignKey("dataset.id"))
    dataset = db.relationship(
        "Dataset", back_populates="tasks", foreign_keys=[dataset_id]
    )

    solr_query_id = db.Column(Integer, ForeignKey("solr_query.id"))
    solr_query = db.relationship(
        "SolrQuery", foreign_keys=[solr_query_id], back_populates="tasks"
    )

    input_data = db.Column(db.String(255))  # later on something else?
    uuid = db.Column(
        UUID(as_uuid=True), unique=True, nullable=False, default=uuid.uuid4
    )

    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    user = db.relationship("User", back_populates="all_tasks", foreign_keys=[user_id])

    # force refresh: if True executes analysis utility once again, if False tries to find result from DB
    # mostly for debugging
    force_refresh = db.Column(db.Boolean)

    # created/running/finished/failed
    task_status = db.Column(
        db.Enum("created", "running", "finished", "failed", name="task_status"),
        nullable=False,
    )
    # error message for failed tasks, might be something else for other types
    status_message = db.Column(db.String(255))

    # timestamps
    task_started = db.Column(db.DateTime, default=datetime.utcnow)
    task_finished = db.Column(db.DateTime)

    # if we need to run a task once again we make a copy of task
    # and add an additional relation to Result table
    # results contain data and can be heavy while tasks contain only parameters and should be light
    task_results = db.relationship(
        "Result", secondary=task_result_relation, back_populates="tasks"
    )

    collections = db.relationship(
        "Collection", secondary=task_collection_relation, back_populates="tasks"
    )

    def __repr__(self):
        if self.dataset:
            return "<Task id: {}, processor: {}, dataset: {}, status: {}>".format(
                self.id,
                self.processor.name,
                self.dataset.dataset_name,
                self.task_status,
            )
        elif self.solr_query:
            return "<Task id: {}, processor: {}, solr_query: {}, status: {}>".format(
                self.id,
                self.processor.name,
                self.solr_query.search_query,
                self.task_status,
            )
        else:
            return "<Task id: {}, processor: {}, parents: {}, status: {}>".format(
                self.id,
                self.processor.name,
                " ,".join([str(p.uuid) for p in self.parents]),
                self.task_status,
            )

    def data_dict(self):
        if self.dataset:
            return {"dataset": self.dataset.dataset_name}
        elif self.solr_query:
            return {"search_query": self.solr_query.search_query}
        elif self.processor.name in Config.PROCESSOR_EXCEPTION_LIST:
            return {
                k: v for k, v in self.parameters.items() if k.startswith("collection")
            }

    def dict(self, style="status"):
        ret = {
            "uuid": str(self.uuid),
            "processor": self.processor.name,
            "parameters": self.parameters,
            "task_status": self.task_status,
            "task_started": http_date(self.task_started),
        }
        if not self.task_status == "running":
            ret.update({"task_finished": http_date(self.task_finished)})

        if style == "status":
            pass

        elif style == "result":
            ret.update({"task_result": self.result_with_interestingness_and_images})

        elif style == "reporter":
            ret.update(
                {"task_result": self.result_with_interestingness,}
            )
            ret.update(self.data_dict())

        elif style == "investigator":
            if self.task_result:
                ret.update(
                    {"interestingness": self.task_result.interestingness["overall"]}
                )
            if self.parents:
                ret.update({"parents": [str(p.uuid) for p in self.parents]})
            ret.update({"collections": [c.collection_no for c in self.collections]})
            data_dict = self.data_dict()
            if data_dict:
                ret.update(data_dict)
        return ret

    @property
    def search_query(self):
        if self.dataset:
            return self.dataset.make_query()
        elif self.solr_query:
            return self.solr_query.search_query
        else:
            raise NotImplementedError("Cannot get query for task %s" % self)

    @property
    def task_result(self):
        if self.task_results:
            self.task_results = [tr for tr in self.task_results if tr]
            if len(self.task_results) > 1:
                current_app.logger.debug("TASK_RESULTS: %s" % self.task_results)
                raise NotImplementedError(
                    "Don't know what to do with more than one result"
                )
            elif self.task_results:
                return self.task_results[0]

    def report(self, language="en", format="p", need_links=True):
        result = self.task_result
        if result:
            return get_report(
                result.result_reports,
                language=language,
                format=format,
                need_links=need_links,
            )

    @property
    def result_with_interestingness(self):
        if self.task_result:
            return {
                "result": self.task_result.result,
                "interestingness": self.task_result.interestingness,
            }

    @property
    def result_with_interestingness_and_images(self):
        if self.task_result:
            res = self.result_with_interestingness
            res.update({"images": self.task_result.images})
            return res

    @property
    def interestingness(self):
        return self.task_result.interestingness["overall"]

    @property
    def parent_uuid(self):
        parent_uuids = [p.uuid for p in self.parents]
        return parent_uuids


class Result(db.Model):
    # ??? do we need uuids for results (separately from task uuids)
    __tablename__ = "result"
    id = db.Column(db.Integer, primary_key=True)
    tasks = db.relationship(
        "Task", secondary=task_result_relation, back_populates="task_results"
    )
    result = db.Column(db.JSON)

    images = db.Column(db.JSON)

    interestingness = db.Column(db.JSON)
    last_updated = db.Column(db.DateTime, default=datetime.utcnow)
    result_reports = db.relationship("Report", back_populates="result")

    def __repr__(self):
        return "<Result id: {} date: {} tasks: {} >".format(
            self.id, self.last_updated, self.tasks
        )


class Report(db.Model):
    __tablename__ = "report"
    id = db.Column(db.Integer, primary_key=True)

    # result for a single task
    result_id = db.Column(db.Integer, db.ForeignKey("result.id"))
    result = db.relationship(
        "Result", back_populates="result_reports", foreign_keys=[result_id]
    )

    # result for a set of tasks (investigator node)
    node_id = db.Column(db.Integer, db.ForeignKey("investigator_result.id"))
    node = db.relationship(
        "InvestigatorResult", back_populates="result_reports", foreign_keys=[node_id]
    )

    # result for a whole investigator run
    run_id = db.Column(db.Integer, db.ForeignKey("investigator_run.id"))
    run = db.relationship(
        "InvestigatorRun", back_populates="result_reports", foreign_keys=[run_id]
    )

    report_language = db.Column(db.String(255))
    report_format = db.Column(db.String(255))
    report_content = db.Column(db.JSON)
    head_generation_error = db.Column(db.String(255))
    body_generation_error = db.Column(db.String(255))

    report_generated = db.Column(db.DateTime, default=datetime.utcnow)
    need_links = db.Column(db.Boolean)

    def dict(self):
        ret = self.report_content
        ret.update({"language": self.report_language})
        return ret

    def __repr__(self):
        return "<Report>"


def get_report(reports, language="en", format="p", need_links=True):
    reports = [
        r
        for r in reports
        if r.report_language == language
        and r.report_format == format
        and not (r.head_generation_error or r.body_generation_error)
        and r.need_links == need_links
    ]
    if reports:
        return sorted(reports, key=lambda r: r.report_generated)[-1]


def get_explanation(explanations, language="en", format="ul"):
    explanations = [
        e
        for e in explanations
        if e.explanation_language == language
        and e.explanation_format == format
        and not e.generation_error
    ]
    if explanations:
        current_app.logger.debug("EXPLANATIONS: %s" % explanations)
        return sorted(explanations, key=lambda e: e.explanation_generated)[-1]


class Explanation(db.Model):
    __tablename__ = "explanation"
    id = db.Column(db.Integer, primary_key=True)

    run_id = db.Column(db.Integer, db.ForeignKey("investigator_run.id"))
    run = db.relationship(
        "InvestigatorRun", back_populates="run_explanations", foreign_keys=[run_id]
    )

    explanation_language = db.Column(db.String(255))
    explanation_format = db.Column(db.String(255))
    explanation_content = db.Column(db.JSON)
    generation_error = db.Column(db.String(255))

    explanation_generated = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return "<Explanation id: {} explanation_content: {} generation_error: {} ".format(
            self.id, self.explanation_content, self.generation_error
        )


class InvestigatorRun(db.Model):
    # TODO: make explicit relations w other tables
    __tablename__ = "investigator_run"
    id = db.Column(db.Integer, primary_key=True)
    uuid = db.Column(
        UUID(as_uuid=True), unique=True, nullable=False, default=uuid.uuid4
    )

    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    user = db.relationship("User", back_populates="all_runs", foreign_keys=[user_id])

    root_dataset_id = db.Column(db.Integer, db.ForeignKey("dataset.id"))
    root_dataset = db.relationship("Dataset", foreign_keys=[root_dataset_id])

    root_solr_query_id = db.Column(Integer, ForeignKey("solr_query.id"))
    root_solr_query = db.relationship("SolrQuery", foreign_keys=[root_solr_query_id])

    user_parameters = db.Column(db.JSON)
    run_status = db.Column(db.String(255))

    # timestamps
    run_started = db.Column(db.DateTime, default=datetime.utcnow)
    run_finished = db.Column(db.DateTime)

    # list of tasks ready for report
    # replaced with updated results after each investigator's cycle
    result = db.Column(db.JSON, default=[])
    result_reports = db.relationship("Report", back_populates="run")

    run_explanations = db.relationship("Explanation", back_populates="run")

    # all done tasks
    # unlike result never replaced, just updated
    done_tasks = db.Column(db.JSON)

    nodes = db.Column(db.JSON, default=[])

    root_action_id = db.Column(db.Integer)

    collections = db.Column(db.JSON, default=[])

    def data_dict(self):
        if self.root_dataset:
            return {"dataset": self.root_dataset.dataset_name}
        elif self.root_solr_query:
            return {"solr_query": self.root_solr_query.search_query}

    def dict(self, style="status"):
        ret = {
            "uuid": str(self.uuid),
            "user_parameters": self.user_parameters,
            "run_status": self.run_status,
            "run_started": http_date(self.run_started),
        }
        ret.update(self.data_dict())
        if self.run_status == "finished":
            ret.update({"run_finished": http_date(self.run_finished)})

        if style == "status":
            pass
        elif style == "result":
            ret.update(
                {
                    "result": self.result,
                    "nodes": self.nodes,
                    "collections": self.collections,
                }
            )

        return ret

    def report(self, language="en", format="p", need_links=True):
        if self.result_reports:
            return get_report(
                self.result_reports,
                language=language,
                format=format,
                need_links=need_links,
            )

    def explanation(self, language="en", format="ul"):
        if self.run_explanations:
            return get_explanation(
                self.run_explanations, language=language, format=format
            )

    def __repr__(self):
        if self.root_dataset:
            return "<InvestigatorRun id: {}, dataset: {}, status: {}>".format(
                self.id, self.root_dataset.dataset_name, self.run_status,
            )
        elif self.root_solr_query:
            return "<InvestigatorRun id: {}, solr_query: {}, status: {}>".format(
                self.id, self.root_solr_query.search_query, self.run_status,
            )


class InvestigatorAction(db.Model):
    __tablename__ = "investigator_action"
    id = db.Column(db.Integer, primary_key=True)

    run_id = db.Column(db.Integer, db.ForeignKey("investigator_run.id"))

    action_id = db.Column(db.Integer)  # step number inside run
    __table_args__ = (
        UniqueConstraint("run_id", "action_id", name="uq_run_and_action"),
    )
    action_type = db.Column(
        db.Enum(
            "initialize",
            "select",
            "execute",
            "report",
            "update",
            "stop",
            name="action_type",
        )
    )

    input_queue = db.Column(ARRAY(db.Integer))  # task ids
    output_queue = db.Column(ARRAY(db.Integer))

    why = db.Column(db.JSON)
    action = db.Column(db.JSON)  # might be assosiated with some tasks
    # INITIALIZE: list of initial tasks
    # SELECT: action contains list of selected tasks and their place in the input, output_queue
    # EXECUTE: list of tasks that should be executed
    # REPORT: selection of actions to report
    # UPDATE: all the modifications: add/remove/insert/permute

    timestamp = db.Column(db.DateTime, default=datetime.utcnow())

    def __repr__(self):
        return "<InvestigatorAction id: {} run_id: {} action_id: {} action_type: {} why: {}>".format(
            self.id, self.run_id, self.action_id, self.action_type, self.why
        )


class InvestigatorResult(db.Model):
    """
    Single result node---a set of actions, that could be reported to a user 
    """

    __tablename__ = "investigator_result"
    id = db.Column(db.Integer, primary_key=True)

    # can be queried from the demonstrator, needs uuid
    uuid = db.Column(
        UUID(as_uuid=True), unique=True, nullable=False, default=uuid.uuid4
    )
    run_id = db.Column(db.Integer, db.ForeignKey("investigator_run.id"))
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    user = db.relationship("User", foreign_keys=[user_id])

    # results for actions from .. to ..
    start_action_id = db.Column(db.Integer)
    end_action_id = db.Column(db.Integer)

    interestingness = db.Column(db.Float, default=0.0)
    result = db.Column(db.JSON, default=[])
    result_reports = db.relationship("Report", back_populates="node")

    def report(self, language="en", format="p", need_links=True):
        if self.result_reports:
            return get_report(
                self.result_reports,
                language=language,
                format=format,
                need_links=need_links,
            )

    def __repr__(self):
        return "<InvestigatorResult id: {}, uuid: {} run_id: {} start_action_id: {} end_cation_id: {} interestingness: {} result: {}".format(
            self.id,
            self.uuid,
            self.run_id,
            self.start_action_id,
            self.end_action_id,
            self.interestingness,
            self.result,
        )

    def dict(self, style="result"):
        return {
            "uuid": str(self.uuid),
            "result": self.result,
            "interestingness": self.interestingness,
        }


class Collection(db.Model):
    """
    A set of documents---dataset or a search query---used by investigator task
    """

    id = db.Column(db.Integer, primary_key=True)

    run_id = db.Column(db.Integer, db.ForeignKey("investigator_run.id"))
    data_type = db.Column(db.String(255))
    data_id = db.Column(db.Integer)
    collection_no = db.Column(db.Integer)  # no inside run

    # tasks that use this collection as an input
    tasks = db.relationship(
        "Task", secondary=task_collection_relation, back_populates="collections"
    )

    # task(s) that output this collection
    # in principle, more than one path could lead to the same collection, hence list
    origin = db.Column(db.JSON, default=[])

    # Collections belong to runs and unique numbers inside the run
    # for another run we can make the same collection once again, no problem
    __table_args__ = (
        UniqueConstraint("run_id", "collection_no", name="uq_run_id_and_no"),
    )

    def __repr__(self):
        return "<Collection id: {}, run_id: {}, collection_no: {}, data_type: {}>".format(
            self.id, self.run_id, self.collection_no, self.data_type
        )

    def search_query(self):
        if self.data_type == "dataset":
            dataset = Dataset.query.filter_by(id=self.data_id).one()
            return dataset.make_query()
        elif self.data_type == "search_query":
            solr_query = SolrQuery.query.filter_by(id=self.data_id).one()
            return solr_query.search_query

    def dict(self):
        return {
            "collection_no": self.collection_no,
            "collection_type": self.data_type,
            "search_query": self.search_query(),
            "origin": self.origin,
        }


# Needed by flask_login
@login.user_loader
def load_user(id):
    return User.query.get(int(id))


# User login using a Bearer Token, if it exists
@login.request_loader
def load_user_from_request(request):
    token = request.headers.get("Authorization")
    if token is None:
        return None
    if token[:4] == "JWT ":
        token = token.replace("JWT ", "", 1)
        try:
            decoded = jwt.decode(token, Config.SECRET_KEY, algorithm="HS256")
        except (ExpiredSignatureError, InvalidSignatureError):
            return None
        user = User.query.filter_by(username=decoded["username"]).one_or_none()
        if not user:
            user = User(username=decoded["username"])
            db.session.add(user)
            current_app.logger.info(
                "Added new user '{}' to the database".format(user.username)
            )
        else:
            user.last_seen = datetime.utcnow()
        db.session.commit()
        return user
    return None

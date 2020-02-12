from sqlalchemy.exc import IntegrityError
from flask import current_app
from flask_login import current_user
from app.models import User, Document, Dataset, DatasetTransformation
from app import db
import threading
import asyncio
from app.utils.search_utils import search_database


def execute_transformation(api_args):
    """
    Interface to execute dataset transformations from the API
    """
    t = threading.Thread(
        target=transformation_thread,
        args=[current_app._get_current_object(), current_user.id, api_args],
    )

    t.setDaemon(False)
    t.start()


#    i=0
#    while not Dataset.query.filter_by(dataset_name=api_args['dataset_name']).one_or_none():
#        time.sleep(1)
#
#    current_app.logger.debug(Dataset.query.filter_by(dataset_name=api_args['dataset_name']).one_or_none())
#
#    return Dataset.query.filter_by(dataset_name=api_args['dataset_name']).first()


def transformation_thread(app, user_id, args):
    with app.app_context():
        manipulator = Manipulator(User.query.get(user_id))
        asyncio.run(manipulator.execute_user_command(args))


class Manipulator(object):
    def __init__(self, user):
        # currently not used, need to think over (and coordinate with demonstrator)
        self.user = user
        self.command_dict = {
            "create": self.create_dataset,
            "add": self.add_to_dataset,
            "delete": self.delete_from_dataset,
        }
        self.dataset = None

    async def execute_user_command(self, args):
        current_app.logger.debug(args)
        await self.command_dict.get(args["command"])(
            **{k: v for k, v in args.items() if k != "command"}
        )

    async def create_dataset(self, dataset_name, searches, articles):
        current_app.logger.debug("Creating dataset %s" % dataset_name)

        try:
            dataset = Dataset(dataset_name=dataset_name)
            db.session.add(dataset)
            db.session.commit()

            transformation = DatasetTransformation(dataset_id=dataset.id, transformation="create")
            db.session.add(transformation)
            db.session.commit()

        except IntegrityError:
            raise

        await self.add_to_dataset(dataset, searches, articles)

    async def add_to_dataset(self, dataset, searches, articles):

        current_app.logger.debug("searches: %s" % searches)
        current_app.logger.debug("articles: %s" % articles)

        searches = eval(searches) if searches else []
        articles = eval(articles) if articles else []
        current_app.logger.debug("Adding searches into %s" % dataset.dataset_name)

        # 1. add documents to document table
        if searches:
            search_results = await search_database(searches, retrieve="docids")
            doc_ids = [doc["id"] for result in search_results for doc in result["docs"]]
        else:
            doc_ids = []

        # here articles are solr_ids, nothing to query from solr
        doc_ids += articles

        current_app.logger.debug("DOC_IDS: %s" % doc_ids)

        await self.add_documents_to_dataset(dataset, doc_ids)

        # 2. record operations in dataset transformation table
        operations = [
            DatasetTransformation(transformation="add", dataset_id=dataset.id, search_query=search)
            for search in searches
        ]
        operations += [
            DatasetTransformation(dataset_id=dataset.id, transformation="add", document=article)
            for article in articles
        ]

        db.session.add_all(operations)
        db.session.commit()

    async def add_documents_to_dataset(self, dataset, document_ids):
        for solr_id in document_ids:
            # TODO: all this should be done more clever in parallel, *if* we have to imnplement it on our side
            # most probably this part would be moved to Demonstrator
            document = Document.query.filter_by(solr_id=solr_id).one_or_none()
            if not document:
                document = Document(solr_id=solr_id)
                db.session.add(document)
            document.datasets.append(dataset)

    async def delete_from_dataset(self, dataset, searches, articles):
        raise NotImplementedError

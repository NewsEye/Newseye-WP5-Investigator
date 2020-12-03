from werkzeug.exceptions import BadRequest
from config import Config
import requests
import os
from werkzeug.exceptions import BadRequest, NotFound
from app.models import Dataset, Document, DocumentDatasetRelation
from app import db
from flask import current_app
import json


def get_dataset(dataset):
    # current_app.logger.debug("DATASET!!!!!!: %s type: %s" %(dataset, type(dataset)))
    if isinstance(dataset, Dataset):
        dataset_name, user = dataset.dataset_name, dataset.user
    else:
        dataset_name, user = dataset["name"], dataset["user"]
        dataset = Dataset.query.filter_by(
            dataset_name=dataset_name, user=user
        ).one_or_none()

    if not dataset or not uptodate(dataset):
        current_app.logger.debug("REQUESTING...")
        request_dataset(dataset_name, user)
    return Dataset.query.filter_by(dataset_name=dataset_name).first()


def get_token():
    # TODO get token once and require only in case of authentication error
    url = os.path.join(Config.DATASET_URI, "authenticate")
    payload = json.dumps(
        {"email": Config.DATASET_EMAIL, "password": Config.DATASET_PASSWORD}
    )
    headers = {"content-type": "application/json"}
    response = requests.request(
        "POST", url, data=payload, headers=headers, verify=False
    )
    token = response.json()["auth_token"]
    return "JWT " + token


def uptodate(dataset):
    return dataset.hash_value == get_hash_value(dataset.dataset_name, dataset.user)


def get_hash_value(dataset_name, user):
    url = os.path.join(Config.DATASET_URI, "list_datasets")
    payload = json.dumps({"email": user})
    headers = {"content-type": "application/json", "authorization": get_token()}
    response = requests.request(
        "POST", url, data=payload, headers=headers, verify=False
    )
    # current_app.logger.debug("PAYLOAD: %s" %payload)
    # current_app.logger.debug("RESPONSE: %s" %response)
    for d in response.json():
        if d[0] == dataset_name:
            return str(d[1])
    raise BadRequest("Dataset {} does not exist for {}".format(dataset_name, user))


def request_dataset(dataset_name, user):
    url = os.path.join(Config.DATASET_URI, "get_dataset_content")
    payload = json.dumps({"email": user, "dataset_name": dataset_name})

    headers = {"content-type": "application/json", "authorization": get_token()}
    # current_app.logger.debug("PAYLOAD: %s" %payload)

    response = requests.request(
        "POST", url, data=payload, headers=headers, verify=False
    )
    # current_app.logger.debug("RESPONSE: %s" %response)
    if response.status_code is 404:
        raise NotFound("Dataset {} is not found for {}".format(dataset_name, user))
    make_dataset(dataset_name, user, response.json())


def make_dataset(dataset_name, user, document_list):
    current_app.logger.debug("DATASET_NAME: %s USER: %s DOCUMENT_LIST %s" %(dataset_name, user, document_list))
    dataset = Dataset.query.filter_by(
        dataset_name=dataset_name, user=user
    ).one_or_none()
    # current_app.logger.debug("make_dataset: %s" %dataset)
    if dataset:
        DocumentDatasetRelation.query.filter_by(dataset_id=dataset.id).delete()
    else:
        dataset = Dataset(
            dataset_name=dataset_name,
            user=user,
            hash_value=get_hash_value(dataset_name, user),
        )
        current_app.logger.debug("else: %s" % dataset)
        db.session.add(dataset)
    db.session.commit()
    # current_app.logger.debug("made_dataset: %s" %dataset)
    relations = []
    for d in document_list:
        if d["type"] != "article":
            # TODO: add all documents from these issues?
            # for now: skip
            continue
        document = get_document(d["id"])
        relations.append(
            DocumentDatasetRelation(
                dataset_id=dataset.id, document_id=document.id, relevance=d["relevancy"]
            )
        )
    db.session.add_all(relations)
    db.session.commit()


def get_document(document_id):
    document = Document.query.filter_by(solr_id=document_id).one_or_none()
    if not document:
        document = Document(solr_id=document_id)
        db.session.add(document)
        db.session.commit()
    return document

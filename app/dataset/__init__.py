from flask_restplus import Namespace

ns = Namespace('dataset', description='Creation and manipulation of datasets')

from app.dataset import routes

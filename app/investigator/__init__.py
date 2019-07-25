from flask_restplus import Namespace

ns = Namespace('investigator', description='Unsupervised data investigation')

from app.investigator import routes

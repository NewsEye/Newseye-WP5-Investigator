from flask_restplus import Namespace

ns = Namespace('search', description='Search operations')

from app.search import routes

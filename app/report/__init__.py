from flask_restplus import Namespace

ns = Namespace('report', description='Report operations')

from app.report import routes

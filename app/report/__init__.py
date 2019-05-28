from flask import Blueprint
from flask_restplus import Api

bp = Blueprint('report', __name__)
api = Api(bp, doc='/docs')

from app.report import routes

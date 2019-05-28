from flask import Blueprint
from flask_restplus import Api

bp = Blueprint('search', __name__)
api = Api(bp, doc='/docs')

from app.search import routes

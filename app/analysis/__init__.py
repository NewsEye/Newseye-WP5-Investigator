from flask import Blueprint

bp = Blueprint('analysis', __name__)

from app.analysis import routes

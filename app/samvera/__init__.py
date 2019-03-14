from flask import Blueprint

bp = Blueprint('samvera', __name__)

from app.samvera import routes

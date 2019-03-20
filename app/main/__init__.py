from flask import Blueprint

bp = Blueprint('main', __name__)

from app.main.core import SystemCore

core = SystemCore()

from app.main import routes

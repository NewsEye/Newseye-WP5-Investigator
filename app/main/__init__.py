from flask import Blueprint

bp = Blueprint('main', __name__)

from main.core import SystemCore

core = SystemCore()

from app.main import routes

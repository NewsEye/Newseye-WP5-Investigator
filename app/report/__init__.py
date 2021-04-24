from flask_restplus import Namespace

ns = Namespace("report", description="Reporter calls")

from app.report import routes

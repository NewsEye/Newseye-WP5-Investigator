from flask_restplus import Namespace

ns = Namespace("explainer", description="Explainer calls")

from app.explainer import routes

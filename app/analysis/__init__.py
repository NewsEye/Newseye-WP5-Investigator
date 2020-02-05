from flask_restplus import Namespace
from flask import current_app

ns = Namespace("analysis", description="Analysis operations")

from app.analysis import routes

from app.analysis.facet_processors import ExtractFacets
from app.analysis.word_processors import ExtractWords, ExtractBigrams


def initialize_processors(app):
    with app.app_context():
        ExtractFacets(initialize=True).make_processor()
        ExtractWords(initialize=True).make_processor()
        ExtractBigrams(initialize=True).make_processor()

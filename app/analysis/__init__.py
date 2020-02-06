from flask_restplus import Namespace
from flask import current_app

ns = Namespace("analysis", description="Analysis operations")

from app.analysis import routes

from app.analysis.facet_processors import ExtractFacets, GenerateTimeSeries
from app.analysis.word_processors import ExtractWords, ExtractBigrams


def initialize_processors(app):
    with app.app_context():
        for cls in [ExtractFacets, GenerateTimeSeries, ExtractWords, ExtractBigrams]:
            cls(initialize=True).make_processor()

from flask_restplus import Namespace
from flask import current_app

ns = Namespace("analysis", description="Analysis operations")

from app.analysis import routes

from app.analysis.facet_processors import ExtractFacets, GenerateTimeSeries
from app.analysis.word_processors import ExtractWords, ExtractBigrams

from app.analysis.summarization_processor import Summarization

def initialize_processors(app):
    # adding processors to the database (if they are not there already)
    with app.app_context():
        for cls in [ExtractFacets, GenerateTimeSeries, ExtractWords, ExtractBigrams, Summarization]:
            cls(initialize=True).make_processor()

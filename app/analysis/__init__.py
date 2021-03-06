from flask_restplus import Namespace
from flask import current_app

ns = Namespace("analysis", description="Analysis operations")

from app.analysis import routes

from app.analysis.facet_processors import ExtractFacets, GenerateTimeSeries
from app.analysis.word_processors import ExtractWords, ExtractBigrams
from app.analysis.summarization_processor import Summarization
from app.analysis.topic_processors import (
    TopicModelDocumentLinking,
    QueryTopicModel,
    TopicModelDocsetComparison,
)
from app.analysis.data_transformation import (
    SplitByFacet,
    Comparison,
    FindBestSplitFromTimeseries,
)
from app.analysis.name_processors import ExtractNames, TrackNameSentiment
from app.analysis.embeddings_processors import ExpandQuery


def initialize_processors(app):
    # adding processors to the database (if they are not there already)
    with app.app_context():
        for cls in [
            ExtractFacets,
            GenerateTimeSeries,
            ExtractWords,
            ExtractBigrams,
            Summarization,
            TopicModelDocumentLinking,
            TopicModelDocsetComparison,
            QueryTopicModel,
            SplitByFacet,
            Comparison,
            FindBestSplitFromTimeseries,
            ExtractNames,
            TrackNameSentiment,
            ExpandQuery,
        ]:
            cls(initialize=True).make_processor()

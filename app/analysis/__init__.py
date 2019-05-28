from flask import Blueprint
from flask_restplus import Api

from app.analysis.analysis_utils import ExtractFacets, CommonFacetValues, GenerateTimeSeries, ExtractDocumentIds, QueryTopicModel, LemmaFrequencyTimeseries, AnalyseLemmaFrequency
from app.analysis.step_detection import FindStepsFromTimeSeries

bp = Blueprint('analysis', __name__)
api = Api(bp, doc='/docs')

UTILITY_MAP = {
    'extract_facets': ExtractFacets(),
    'common_facet_values': CommonFacetValues(),
    'generate_time_series': GenerateTimeSeries(),
    'find_steps_from_time_series': FindStepsFromTimeSeries(),
    'extract_document_ids': ExtractDocumentIds(),
    # This is disabled for now because it doesn't support the real data
    # 'query_topic_model': QueryTopicModel(),
    # These don't work yet without the pickled indexes
    # 'lemma_frequency_timeseries': LemmaFrequencyTimeseries(),
    # 'analyse_lemma_frequency': AnalyseLemmaFrequency(),
}

from app.analysis import routes

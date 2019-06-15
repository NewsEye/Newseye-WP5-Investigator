from flask_restplus import Namespace

from app.analysis.analysis_utils import ExtractFacets, CommonFacetValues, GenerateTimeSeries, ExtractDocumentIds, \
    LemmaFrequencyTimeseries, AnalyseLemmaFrequency
from app.analysis.topic_models import QueryTopicModel
from app.analysis.step_detection import FindStepsFromTimeSeries

ns = Namespace('analysis', description='Analysis operations')

UTILITY_MAP = {
    'extract_facets': ExtractFacets(),
    'common_facet_values': CommonFacetValues(),
    'generate_time_series': GenerateTimeSeries(),
    'find_steps_from_time_series': FindStepsFromTimeSeries(),
    'extract_document_ids': ExtractDocumentIds(),
    'query_topic_model': QueryTopicModel(),
    # These don't work yet without the pickled indexes
    # 'lemma_frequency_timeseries': LemmaFrequencyTimeseries(),
    # 'analyse_lemma_frequency': AnalyseLemmaFrequency(),
}

from app.analysis import routes

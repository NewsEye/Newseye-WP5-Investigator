from flask_restplus import Namespace

from app.analysis.analysis_utils import (
    ExtractFacets,
    CommonFacetValues,
    GenerateTimeSeries,
    ExtractDocumentIds,
)

# from app.analysis.analysis_utils import LemmaFrequencyTimeseries, AnalyseLemmaFrequency
from app.analysis.topic_models import QueryTopicModel, TopicModelDocumentLinking
from app.analysis.step_detection import FindStepsFromTimeSeries
from app.analysis.basic_statistics import ExtractWords, ComputeTfIdf  # MakeBasicStats,
from app.investigator.result_comparison import ComparisonUtility

ns = Namespace("analysis", description="Analysis operations")

UTILITY_MAP = {
    "extract_facets": ExtractFacets(),
    "common_facet_values": CommonFacetValues(),
    "generate_time_series": GenerateTimeSeries(),
    "find_steps_from_time_series": FindStepsFromTimeSeries(),
    "extract_document_ids": ExtractDocumentIds(),
    "query_topic_model": QueryTopicModel(),
    "extract_words": ExtractWords(),
    #'make_basic_stats': MakeBasicStats(),
    "compute_tf_idf": ComputeTfIdf(),
    "tm_document_linking": TopicModelDocumentLinking(),
    "comparison": ComparisonUtility()
    # These don't work yet without the pickled indexes
    # 'lemma_frequency_timeseries': LemmaFrequencyTimeseries(),
    # 'analyse_lemma_frequency': AnalyseLemmaFrequency(),
}

INPUT_TYPE_MAP = {
    # there are more than one utility that outputs 'id_list'
    # this map specifies default utility to be used
    "id_list": "extract_document_ids"
}


from app.analysis import routes

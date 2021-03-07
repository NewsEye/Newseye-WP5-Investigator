from flask_restplus import Namespace

ns = Namespace("investigator", description="Investigator")

#### PROCESSOR SETS

PROCESSORSETS = {
    "DESCRIPTION": [
        {"name": "ExtractFacets", "parameters": {}},
        {"name": "ExtractWords", "parameters": {"unit": "tokens"}},
        {"name": "ExtractBigrams", "parameters": {"unit": "tokens"}},
        {"name": "GenerateTimeSeries", "parameters": {}},
        {"name": "ExtractNames", "parameters": {}},
    ],
    "EXTRACT_WORDS": [{"name": "ExtractWords", "parameters": {"unit": "tokens"}}],
    "SPLIT_BY_LANGUAGE": [
        {"name": "SplitByFacet", "parameters": {"facet": "LANGUAGE"}}
    ],
    "SUMMARIZATION": [{"name": "Summarization", "parameters": {}}],
    "MONOLINGUAL_BIG": [
        {"name": "QueryTopicModel", "parameters": {}},
        {"name": "SplitByFacet", "parameters": {"facet": "NEWSPAPER_NAME"}},
    ],
    "SPLIT_BY_SOURCE": [
        {"name": "SplitByFacet", "parameters": {"facet": "NEWSPAPER_NAME"}},
    ],
    "SPLIT_BY_YEAR": [{"name": "SplitByFacet", "parameters": {"facet": "PUB_YEAR"}},],
    "EXPAND_QUERY": [{"name": "ExpandQuery", "parameters": {"max_number": 10}}],
    "FIND_BEST_SPLIT": [{"name": "FindBestSplitFromTimeseries", "parameters": {}}],
    "COMPARE_NAMES": [{"name": "Comparison", "source": "ExtractNames"}],
    "COMPARE_TOPICS": [{"name": "Comparison", "source": "TOPICS"}],
    "TRACK_NAME_SENTIMENT": [{"name": "TrackNameSentiment", "parameters": {}}],
}


PROCESSOR_PRIORITY = {
    # heruistics
    # fast query, part of description
    "ExtractFacets": 2,
    # not so fast, requires many queries but they are parts of description
    "ExtractWords": 4,
    "ExtractBigrams": 4,
    "ExtractNames": 4,
    # requires only facets:
    "GenerateTimeSeries": 3,
    # fast, does not query Solr, uses previous results
    "FindBestSplitFromTimeseries": 1,
    "SplitByFacet": 1,
    "Comparison": 1,
    # slow, end of path:
    "TrackNameSentiment": 7,
    # superslow, end of path:
    "Summarization": 1,
    # requires many queries:
    "TopicModelDocumentLinking": 6,
    "QueryTopicModel": 6,
    # also requires many solr queries but then switches to another API
    "TopicModelDocsetComparison": 5,
    # uses another API
    "ExpandQuery": 1,
}

from app.investigator import routes

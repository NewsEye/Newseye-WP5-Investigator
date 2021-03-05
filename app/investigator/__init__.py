from flask_restplus import Namespace

ns = Namespace("investigator", description="Investigator")

#### PROCESSOR SETS

processorsets = {
    "DESCRIPTION": [
        {"name": "ExtractFacets", "parameters": {}},
        {"name": "ExtractWords", "parameters": {"unit": "tokens"}},
        {"name": "ExtractBigrams", "parameters": {"unit": "tokens"}},
        {"name": "GenerateTimeSeries", "parameters": {}},
        {"name": "ExtractNames", "parameters": {}},
    ],
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
    
    "EXPAND_QUERY": [{"name": "ExpandQuery", "parameters": {"max_number": 10}}],

    "FIND_BEST_SPLIT": [{"name": "FindBestSplitFromTimeseries", "parameters": {}}],

    "COMPARE_NAMES" : [{"name": "Comparison", "source": "ExtractNames"}]
}


from app.investigator import routes

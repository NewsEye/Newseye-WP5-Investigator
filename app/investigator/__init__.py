from flask_restplus import Namespace

ns = Namespace("investigator", description="Investigator")

#### PROCESSOR SETS

processorsets = {
    "DESCRIPTION": [
        {"name": "ExtractFacets", "parameters": {}},
        {"name": "ExtractWords", "parameters": {"unit":"tokens"}},
        {"name": "ExtractBigrams", "parameters": {"unit":"tokens"}},
        {"name": "GenerateTimeSeries", "parameters": {}},
    ],
    "SPLIT": [
        {"name": "SplitByFacet", "parameters": {"facet": "LANGUAGE"}},
        {"name": "SplitByFacet", "parameters": {"facet": "PUB_YEAR"}},
        {"name": "SplitByFacet", "parameters": {"facet": "NEWSPAPER_NAME"}},
    ],
    "SUMMARIZATION": [{"name": "Summarization", "parameters": {}}],
    "TOPIC_MODEL": [{"name": "QueryTopicModel", "parameters": "LANG"}],
}


from app.investigator import routes

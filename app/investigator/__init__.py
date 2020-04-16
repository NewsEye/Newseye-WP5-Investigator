from flask_restplus import Namespace

ns = Namespace("investigator", description="Investigator")

#### PROCESSOR SETS

processorsets = {
    "DESCRIPTION": [
        {"name": "ExtractFacets", "parameters": {}},
        {"name": "ExtractWords", "parameters": {}},
        {"name": "ExtractBigrams", "parameters": {}},
        {"name": "GenerateTimeSeries", "parameters": {}},
    ]
}


from app.investigator import routes

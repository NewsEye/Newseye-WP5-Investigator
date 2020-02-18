from flask_restplus import Namespace

ns = Namespace("investigator", description="Investigator")

#### PROCESSOR SETS

processorsets = {
    "DESCRIPTION" : ["ExtractFacets", "ExtractWords", "ExtractBigrams", "GenerateTimeSeries"]
    }


from app.investigator import routes

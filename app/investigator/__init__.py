from flask_restplus import Namespace

ns = Namespace('investigator', description='Unsupervised data investigation')

from app.investigator.patterns import BasicStats, Facets, FindSteps, Topics, DocumentLinkingTM

ANALYSING_PATTERNS = [BasicStats, Facets, FindSteps, Topics]
LINKING_PATTERNS = [DocumentLinkingTM]

from app.investigator import routes

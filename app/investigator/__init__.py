
from flask_restplus import Namespace

ns = Namespace('investigator', description='Unsupervised data investigation')


from app.investigator.patterns import BasicStats, Facets, Topics, DocumentLinkingTM


ANALYSING_PATTERNS = [BasicStats, Facets, Topics]
LINKING_PATTERNS = [DocumentLinkingTM]
    
from app.investigator import routes

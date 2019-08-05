
from flask_restplus import Namespace

ns = Namespace('investigator', description='Unsupervised data investigation')


from app.investigator.patterns import BasicStats, Facets, Topics

DEFAULT_PATTERNS = [BasicStats, Facets, Topics]
    

from app.investigator import routes

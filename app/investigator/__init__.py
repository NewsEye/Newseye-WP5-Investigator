from flask_restplus import Namespace

ns = Namespace("investigator", description="Unsupervised data investigation")

from app.investigator.patterns import (
    BasicStats,
    Facets,
    FindSteps,
    Topics,
    DocumentLinkingTM,
)

# patterns
ANALYSIS = [BasicStats, Facets, FindSteps, Topics]
ANALYSIS_LINKED_DOCS = [
    p for p in ANALYSIS if p != FindSteps
]  # doesn't make sense to  detect steps
# using only few documents;
# needs more thinking
LINKING = [DocumentLinkingTM]

from app.investigator import routes

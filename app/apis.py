from flask_restplus import Api

from app.search import ns as search_ns
from app.analysis import ns as analysis_ns
from app.report import ns as report_ns
from app.investigator import ns as investigator_ns

api = Api(
    title='NewsEye Personal Research Assistant API',
    version='1.0',
    description='The API for the NewsEye PRA. Currently only accessible via NewsEye Demonstrator.',
    doc='/docs',
    # All API metadatas
)

api.add_namespace(search_ns, path='/api/search')
api.add_namespace(analysis_ns, path='/api/analysis')
api.add_namespace(report_ns, path='/api/report')
api.add_namespace(investigator_ns, path='/api/investigator')

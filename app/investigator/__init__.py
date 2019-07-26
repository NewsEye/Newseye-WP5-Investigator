from flask_restplus import Namespace

ns = Namespace('investigator', description='Unsupervised data investigation')

DEFAULT_UTILITIES = ['common_facet_values',
#                     'query_topic_model',  # TODO: parameters
                     'compute_tf_idf']


from app.investigator import routes



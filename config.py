import os
from dotenv import load_dotenv

basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, 'investigator.env'))


class Config(object):
    SECRET_KEY = os.environ.get('SECRET_KEY') or '1234'


    # internal database for tasks and results
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or 'postgresql+psycopg2:///newseye_investigator'
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # not used currently
    MAIL_SERVER = os.environ.get('MAIL_SERVER')
    MAIL_PORT = int(os.environ.get('MAIL_PORT') or 25)
    MAIL_USE_TLS = os.environ.get('MAIL_USE_TLS') is not None
    MAIL_USERNAME = os.environ.get('MAIL_USERNAME')
    MAIL_PASSWORD = os.environ.get('MAIL_PASSWORD')
    ADMINS = ['jari.avikainen@helsinki.fi']


    # The URI for the Reporter API
    REPORTER_URI = "http://newseye-wp5.cs.helsinki.fi:4218/api"

    # This should contain the URI for the topic modelling tools
    TOPIC_MODEL_URI = "https://newseye-wp4.cs.helsinki.fi"

    HEADERS = {}
    COOKIES = {}

    # SOLR_URI = "http://localhost:9983/solr/hydra-development/select"
    SOLR_URI = "http://newseye.cs.helsinki.fi:9983/solr/hydra-development/select"
    SOLR_PARAMETERS = {
        'default': {
            'mm': 1,  # minimal matching
            'wt': 'json',
            'qf': 'all_text_tfr_siv all_text_tfi_siv all_text_tde_siv all_text_tse_siv',
        },
        'all': {
            'fl': 'system_create_dtsi, system_modified_dtsi, has_model_ssim, id, title_ssi, date_created_dtsi, date_created_ssim, language_ssi, original_uri_ss, nb_pages_isi, thumbnail_url_ss, member_ids_ssim, object_ids_ssim, member_of_collection_ids_ssim, timestamp, year_isi, _version_, all_text_tfi_siv, score',
            'facet.field': ['year_isi', 'language_ssi', 'member_of_collection_ids_ssim', 'has_model_ssim'],
        },
        'facets': {
            'facet.field': ['year_isi', 'language_ssi', 'member_of_collection_ids_ssim', 'has_model_ssim'],
            'rows': 0
        },
        'docids': {
            'fl': 'id',
            'rows': 0,
        },
        'words': {
            'qf'  : 'id',
            'fl'  : 'level_reading_order text_tfr_siv text_tse_siv text_tde_siv text_tfi_siv id',
            'fq'  : 'level:4.pages.blocks.lines.words',
            'rows': 0,
            },
    }

    SUPPORTED_LANGUAGES = ['fi', 'de', 'fr']

    DOCUMENTS_KEY = 'docs'
    FACETS_KEY = 'facets'
    FACET_ID_KEY = 'name'
    FACET_ITEMS_KEY = 'items'
    FACET_VALUE_LABEL_KEY = 'label'
    FACET_VALUE_HITS_KEY = 'hits'
    AVAILABLE_FACETS = {
        'LANGUAGE': 'language_ssi',
        'NEWSPAPER_NAME': 'member_of_collection_ids_ssim',
        'PUB_DATE': 'date_created_dtsi',
        'TYPE_OF_DOCUMENT': 'has_model_ssim',
        'PUB_YEAR': 'year_isi',
    }


    PA_API_URI="http://localhost:5000/api/"
    REPORTER_API_URI="https://newseye-wp5.cs.helsinki.fi/api/"

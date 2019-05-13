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
    TOPIC_MODEL_URI = "http://newseye-wp4.cs.helsinki.fi:7100"

    # external database to fetch data from
    # DATABASE_IN_USE = 'demo'
    DATABASE_IN_USE = 'newseye'
    HEADERS = {}
    COOKIES = {}

    if DATABASE_IN_USE == 'demo':
        BLACKLIGHT_URI = "https://demo.projectblacklight.org/catalog.json"
        BLACKLIGHT_DEFAULT_PARAMETERS = {'utf8': "%E2%9C%93"}
        DOCUMENTS_KEY = 'data'
        FACETS_KEY = 'included'
        FACET_ID_KEY = 'id'
        FACET_ATTRIBUTES_KEY = 'attributes'
        FACET_ITEMS_KEY = 'items'
        FACET_VALUE_LABEL_KEY = 'label'
        FACET_VALUE_HITS_KEY = 'hits'
        AVAILABLE_FACETS = {
            'PUB_YEAR': 'pub_date_ssim',
            'TOPIC': 'subject_ssim',
            'ERA': 'subject_era_ssim',
            'REGION': 'subject_geo_ssim',
            'LANGUAGE': 'language_ssim',
            'FORMAT': 'format',
        }

    if DATABASE_IN_USE == 'newseye':
        BLACKLIGHT_URI = "https://platform.newseye.eu/en/catalog.json"
        BLACKLIGHT_DEFAULT_PARAMETERS = {
            # 'utf8': "%E2%9C%93",
            # 'locale': 'en',
            # 'search_field': 'all_fields,'
        }
        # Replace 'username' and 'password' with your own for dev purposes
        NEWSEYE_USERNAME = os.environ.get('NEWSEYE_USERNAME') or 'username'
        NEWSEYE_PASSWORD = os.environ.get('NEWSEYE_PASSWORD') or 'password'
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
        }

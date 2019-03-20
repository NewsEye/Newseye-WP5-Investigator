import os
from dotenv import load_dotenv

basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '.env'))


class Config(object):
    SECRET_KEY = os.environ.get('SECRET_KEY') or '1234'
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or 'postgresql+psycopg2:///newseye_investigator'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    MAIL_SERVER = os.environ.get('MAIL_SERVER')
    MAIL_PORT = int(os.environ.get('MAIL_PORT') or 25)
    MAIL_USE_TLS = os.environ.get('MAIL_USE_TLS') is not None
    MAIL_USERNAME = os.environ.get('MAIL_USERNAME')
    MAIL_PASSWORD = os.environ.get('MAIL_PASSWORD')
    ADMINS = ['jari.avikainen@helsinki.fi']

    DATABASE_IN_USE = 'demo'
    # DATABASE_IN_USE = 'newseye'

    if DATABASE_IN_USE == 'demo':
        BLACKLIGHT_URI = "https://demo.projectblacklight.org/catalog.json"
        BLACKLIGHT_DEFAULT_PARAMETERS = {'utf8': "%E2%9C%93"}
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
            'utf8': "%E2%9C%93",
            'locale': 'en',
            'search_field': 'all_fields,'
        }
        AVAILABLE_FACETS = {
            'LANGUAGE': 'language_ssi',
            'NEWSPAPER_NAME': 'member_of_collection_ids_ssim',
            'PUB_DATE': 'date_created_dtsi'
        }

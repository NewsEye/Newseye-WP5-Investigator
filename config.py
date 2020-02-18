import os
from dotenv import load_dotenv

basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, "investigator.env"))


class Config(object):
    SECRET_KEY = os.environ.get("SECRET_KEY") or "1234"

    # internal database for tasks and results
    SQLALCHEMY_DATABASE_URI = (
        os.environ.get("DATABASE_URL")
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # not used currently
    MAIL_SERVER = os.environ.get("MAIL_SERVER")
    MAIL_PORT = int(os.environ.get("MAIL_PORT") or 25)
    MAIL_USE_TLS = os.environ.get("MAIL_USE_TLS") is not None
    MAIL_USERNAME = os.environ.get("MAIL_USERNAME")
    MAIL_PASSWORD = os.environ.get("MAIL_PASSWORD")
    ADMINS = ["lidia.pivovarova@helsinki.fi"]

    # The URI for the Reporter API
    REPORTER_URI = "http://newseye-wp5.cs.helsinki.fi:4218/api"
    #REPORTER_URI = "http://localhost:8080/api"
    # This should contain the URI for the topic modelling tools
    TOPIC_MODEL_URI = "https://newseye-wp4.cs.helsinki.fi"

    HEADERS = {}
    COOKIES = {}


    SOLR_URI = os.environ.get('SOLR_URI')
    SOLR_MAX_RETURN_VALUES = 100000

    # SOLR_URI = "http://localhost:9983/solr/hydra-development/select"
    # test DB:
    # SOLR_URI = "http://localhost:9984/solr/hydra-development/select"
    # SOLR_URI = "http://newseye.cs.helsinki.fi:9983/solr/hydra-development/select"
    SOLR_PARAMETERS = {
        "default": {
            "mm": 1,  # minimal matching
            "wt": "json",
            "qf": "all_text_tfr_siv all_text_tfi_siv all_text_tde_siv all_text_tse_siv",
        },
        "all": {
            "fl": "system_create_dtsi, system_modified_dtsi, has_model_ssim, id, title_ssi, date_created_dtsi, date_created_ssim, language_ssi, original_uri_ss, nb_pages_isi, thumbnail_url_ss, member_ids_ssim, object_ids_ssim, member_of_collection_ids_ssim, timestamp, year_isi, _version_, all_text_tfi_siv, score",
            "facet.field": [
                "year_isi",
                "language_ssi",
                "member_of_collection_ids_ssim",
                "has_model_ssim",
            ],
        },
        "facets": {
            "facet.field": [
                "year_isi",
                "language_ssi",
                "member_of_collection_ids_ssim",
                "has_model_ssim",
            ],
            "rows": 0,
        },
        "docids": {"fl": "id", "rows": 0,},
        "stems": {
            "tv.all": True,
            "tv.fl": "all_text_tfr_siv all_text_tfi_siv all_text_tde_siv all_text_tse_siv",
            "fl": "nothing",  # non-existing field to retrun nothing; otherwise all text will be returned
            "defType": "edismax",
        },
        "tokens": {
            "tv.all": True,
            "tv.fl": "all_text_unstemmed_tfr_siv all_text_unstemmed_tfi_siv all_text_unstemmed_tde_siv all_text_unstemmed_tse_siv",
            "fl": "nothing",  # non-existing field to retrun nothing; otherwise all text will be returned
            "defType": "edismax",
        },
        'words' : {
            'q' : '*:*',
            'fl': '*,[child parentFilter=level:1. childFilter=level:4. limit=100000]',
            'qf': 'id'
        },
        'pages' : {
            'fl' : 'member_ids_ssim'
            },
    }

    SUPPORTED_LANGUAGES = ["fi", "de", "fr"]

    DOCUMENTS_KEY = "docs"
    FACETS_KEY = "facets"
    FACET_ID_KEY = "name"
    FACET_ITEMS_KEY = "items"
    FACET_VALUE_LABEL_KEY = "label"
    FACET_VALUE_HITS_KEY = "hits"
    AVAILABLE_FACETS = {
        "LANGUAGE": "language_ssi",
        "NEWSPAPER_NAME": "member_of_collection_ids_ssim",
        "PUB_YEAR": "year_isi",
    }

    # for TESTs
    PA_API_URI = "https://newseye-wp5.cs.helsinki.fi/api/"
    PA_API_URI = "http://localhost:5000/api/"
    # REPORTER_API_URI="https://newseye-wp5.cs.helsinki.fi/api/"
    # REPORTER_API_URI="https://localhost:8080/api/"

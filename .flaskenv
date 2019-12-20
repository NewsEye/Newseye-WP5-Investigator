import os
from dotenv import load_dotenv

basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, 'investigator.env'))

FLASK_APP=investigator.py
FLASK_DEBUG=os.environ.get('FLASK_DEBUG')

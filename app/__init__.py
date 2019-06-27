from flask import Flask
from config import Config
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_login import LoginManager

import os
import logging
from logging.handlers import RotatingFileHandler

db = SQLAlchemy()
migrate = Migrate()
login = LoginManager()


def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(Config)

    db.init_app(app)
    migrate.init_app(app, db)
    login.init_app(app)

    from app.apis import api
    api.init_app(app)

    # FLASK_DEBUG in .flaskenv
    if not app.debug and not app.testing:
        if not os.path.exists('logs'):
            os.mkdir('logs')
        file_handler = RotatingFileHandler('logs/investigator.log', maxBytes=10240,
                                          backupCount=10)
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'))

        # for log files
        file_handler.setLevel(logging.INFO)

        # for console
        app.logger.addHandler(file_handler)

        app.logger.setLevel(logging.INFO)
        app.logger.info('NewsEye Investigator startup')

    return app

# This is required for the migrations to work properly
from app import models

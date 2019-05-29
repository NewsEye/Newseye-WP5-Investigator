from flask_restplus import reqparse


class AuthParser(reqparse.RequestParser):

    def __init__(self):
        super(AuthParser, self).__init__()
        self.add_argument('Authorization', location='headers', required=True, help="A valid access token")

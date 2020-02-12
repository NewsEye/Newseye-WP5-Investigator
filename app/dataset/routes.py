from flask import current_app
from flask_login import login_required, current_user
from flask_restplus import Resource
from app.auth import AuthParser
from app.dataset import manipulator
from app.dataset import ns
from werkzeug.exceptions import BadRequest, InternalServerError, NotFound
from app.models import Dataset, Task, Result, Report, Document


@ns.route("/")
class DatasetX(Resource):
    # name conflict with models
    @login_required
    @ns.expect(AuthParser())
    def get(self):
        """
        Retrieve all datasets available for the user
        """
        # TODO
        raise NotImplementedError

    post_parser = AuthParser()
    post_parser.add_argument(
        "dataset_name", location="json", help="A dataset name (should be unique)"
    )
    post_parser.add_argument(
        "command", location="json", help="A command that needs to be executed: CREATE|ADD|DELETE",
    )
    post_parser.add_argument("searches", location="json", help="Solr search queries")
    post_parser.add_argument(
        "articles", location="json", help="Articles to be added into the dataset"
    )
    #    post_parser.add_argument('issues', location='json', help='Articles to be added into the dataset')

    @login_required
    @ns.expect(post_parser)
    def post(self):
        """ 
        Modifies dataset according to query.
        """

        args = self.post_parser.parse_args()
        args.pop("Authorization")

        current_app.logger.debug("DATASET_NAME %s" % args["dataset_name"])

        if args["command"] == "create":
            if Dataset.query.filter_by(dataset_name=args["dataset_name"]).first():
                return {"error": "Dataset %s already exists" % args["dataset_name"]}
        else:
            if not Dataset.query.filter_by(dataset_name=args["dataset_name"]):
                return {"error": "Dataset %s does not exist" % args["dataset_name"]}

        transformation = manipulator.execute_transformation(args)

        return 200, transformation

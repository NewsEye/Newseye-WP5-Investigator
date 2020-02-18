from flask import current_app
from flask_login import login_required, current_user
from flask_restplus import Resource
from app.auth import AuthParser
from app.main import controller
from app.investigator import ns
from app.models import Task, InvestigatorRun, InvestigatorResult
from uuid import UUID
from werkzeug.exceptions import BadRequest, InternalServerError, NotFound
from flask import request


@ns.route("/")
class Investigator(Resource):
    @login_required
    @ns.expect(AuthParser())
    def get(self):
        """
        Retrieve all investigator runs started by the user
        """
        return {"error":"NotImplemented"}

    # Define parser for the POST endpoint
    post_parser = AuthParser()

    post_parser.add_argument(
        "dataset", location="json", help="The name of the dataset to apply processor",
    )

    post_parser.add_argument(
        "search_query",
        type=dict,
        location="json",
        help="A JSON object containing a search query that defines the input data for the analysis task",
    )

    post_parser.add_argument(
        "source_uuid",
        location="json",
        help="A task_uuid that defines the input data for the analysis task",
    )

    post_parser.add_argument(
        "parameters",
        type=dict,
        default={},
        location="json",
        help="A JSON object containing some parameters.",
    )

    
    ## TODO: force_refresh: what should be rerun and to which extend?
###    post_parser.add_argument(
###        "force_refresh",
###        type=bool,
###        default=False,
###        location="json",
###        help="Set to true to redo the analysis even if an older result exists",
###    )

    @login_required
    @ns.expect(post_parser)
    @ns.response(200, "The task has been executed, and the results are ready for retrieval")
    @ns.response(202, "The task has been accepted, and is still running.")
    def post(self):
        """
        Start a new investigator, and return its basic information to the user. Source data should be defined using either the search_query OR the source_uuid parameter.
        """
        args = self.post_parser.parse_args()
        args.pop("Authorization")

        current_app.logger.debug("args: %s" % args)

        try:
            run = controller.investigator_run(args)

            if run.run_status == "finished":
                return InvestigatorRun.query.filter_by(uuid=run.uuid).first().dict(style="result")
            elif run.run_status == "running":
                return run.dict(), 202
            else:
                raise InternalServerError
        except BadRequest:
            raise
        except Exception as e:
            raise InternalServerError


@ns.route("/result")
class Run(Resource):
    @login_required
    @ns.expect(AuthParser())
    def get(self):
        """
        Retrieve results for investigator run
        """
        args = request.args
        
        if "run" in args:
            uuid = args.get("run")
            Table = InvestigatorRun
        elif "node" in args:
            uuid = args.get("node")
            Table = InvestigatorResult
        else:
            raise BadRequest("A 'run' or 'node' must be in a query")
        
        try:
            uuid = UUID(uuid)
        except ValueError:
            raise NotFound
        ret_value = Table.query.filter_by(uuid=uuid).first()
        if ret_value is None:
            raise NotFound("{} not found for user {}".format(uuid, current_user.username))
        
        return ret_value.dict(style="result")





    

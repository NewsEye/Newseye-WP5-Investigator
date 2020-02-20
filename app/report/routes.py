from flask_login import login_required, current_user
from flask_restplus import Resource
from app.auth import AuthParser
from app.report import ns
from app.models import Task, Report
from app.report.report_utils import make_report, get_formats, get_languages
from uuid import UUID
from werkzeug.exceptions import NotFound, BadRequest

from flask import current_app


@ns.route("/report")
@ns.param("task_uuid", "The UUID of the analysis task for which a report should be retrieved")
class ReportTask(Resource):
    parser = AuthParser()
    parser.add_argument(
        "language", default="en", help="The language the report should be written in."
    )
    parser.add_argument("format", default="p", help="The format of the body of the report.")

    parser.add_argument("task", help="task uuid")
    parser.add_argument("node", help="node uuid")
    parser.add_argument("run", help="run uuid")

    @login_required
    @ns.expect(parser)
    def get(self):
        """
        Retrieve a report generated from the task results.
        """
        args = self.parser.parse_args()
        report = make_report(args)
        return report


@ns.route("/languages")
class LanguageList(Resource):
    @login_required
    @ns.expect(AuthParser())
    def get(self):
        """
        List the languages supported by the Reporter component.
        """
        return get_languages()


@ns.route("/formats")
class FormatList(Resource):
    @login_required
    @ns.expect(AuthParser())
    def get(self):
        """
        List the text formatting options supported by the Reporter component.
        """
        return get_formats()

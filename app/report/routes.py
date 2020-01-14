from flask_login import login_required, current_user
from flask_restplus import Resource
from app.auth import AuthParser
from app.report import ns
from app.models import Task, Report
from app.report.report_utils import generate_report, get_formats, get_languages
from uuid import UUID
from werkzeug.exceptions import NotFound, BadRequest


@ns.route('/<string:task_uuid>')
@ns.param('task_uuid', "The UUID of the analysis task for which a report should be retrieved")
class ReportTask(Resource):
    parser = AuthParser()
    parser.add_argument('language', default='en', help="The language the report should be written in.")
    parser.add_argument('format', default='p', help="The format of the body of the report.")

    @login_required
    @ns.expect(parser)
    def get(self, task_uuid):
        """
        Retrieve a report generated from the task results.
        """
        try:
            task_uuid = UUID(task_uuid)
        except ValueError:
            raise NotFound
        args = self.parser.parse_args()
        report_language = args['language']
        report_format = args['format']
        task = Task.query.filter_by(uuid=task_uuid, user_id=current_user.id).first()
        if task is None:
            raise NotFound('Task {} not found for user {}'.format(task_uuid, current_user.username))
        if task.task_type == 'search':
            raise BadRequest('Task {} is a search task. Reports can only be generated from analysis tasks.')
#        task_report = Report.query.filter_by(task_uuid=task_uuid, report_format=report_format, report_language=report_language).first()
        task_report = task.task_report
        if not task_report:
            task_report = generate_report(task, report_language, report_format)
        return task_report.report_content


@ns.route('/languages')
class LanguageList(Resource):
    @login_required
    @ns.expect(AuthParser())
    def get(self):
        """
        List the languages supported by the Reporter component.
        """
        return get_languages()


@ns.route('/formats')
class FormatList(Resource):
    @login_required
    @ns.expect(AuthParser())
    def get(self):
        """
        List the text formatting options supported by the Reporter component.
        """
        return get_formats()


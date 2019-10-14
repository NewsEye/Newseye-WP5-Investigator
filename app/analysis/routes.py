from flask import current_app
from flask_login import login_required, current_user
from flask_restplus import Resource
from app.auth import AuthParser
from app.main import controller
from app.analysis import ns
from app.models import TaskInstance
from app.analysis import UTILITY_MAP
from uuid import UUID
from werkzeug.exceptions import BadRequest, InternalServerError, NotFound


@ns.route('/')
class AnalysisTaskList(Resource):
    @login_required
    @ns.expect(AuthParser())
    def get(self):
        """
        Retrieve all analysis tasks started by the user
        """
        tasks = [task.dict(style='result') for task in
                 TaskInstance.query.filter_by(user_id=current_user.id, task_type='analysis').all()]
        if len(tasks) == 1:
            tasks = tasks[0]
        return tasks

    # Define parser for the POST endpoint
    post_parser = AuthParser()
    post_parser.add_argument('utility', location='json', required=True, help='The name of the analysis utility to execute')
    post_parser.add_argument('search_query', type=dict, location='json', help='A JSON object containing a search query that defines the input data for the analysis task')
    post_parser.add_argument('source_uuid', location='json', help='A task_uuid that defines the input data for the analysis task')
    post_parser.add_argument('utility_parameters', type=dict, default={}, location='json', help='A JSON object containing utility-specific parameters')
    post_parser.add_argument('force_refresh', type=bool, default=False, location='json', help='Set to true to redo the analysis even if an older result exists')

    @login_required
    @ns.expect(post_parser)
    @ns.response(200, 'The task has been executed, and the results are ready for retrieval')
    @ns.response(202, 'The task has been accepted, and is still running.')
    def post(self):
        """
        Start a new analysis task, and return its basic information to the user. Source data should be defined using either the search_query OR the source_uuid parameter.
        """
        args = self.post_parser.parse_args()
        args.pop('Authorization')
        query = ('analysis', args)
        try:
            task = controller.execute_tasks(query)[0].dict()
            if task['task_status'] == 'finished':
                return TaskInstance.query.filter_by(uuid=task['uuid']).first().dict(style='result')
            elif task['task_status'] == 'running':
                return task, 202
            else:
                raise InternalServerError
        except BadRequest:
            raise
        except Exception as e:
            current_app.logger.exception(e)
            raise InternalServerError


@ns.route('/<string:task_uuid>')
@ns.param('task_uuid', "The UUID of the analysis task for which results should be retrieved")
class AnalysisTask(Resource):
    @login_required
    @ns.expect(AuthParser())
    def get(self, task_uuid):
        """
        Retrieve results for an analysis task
        """
        try:
            task_uuid = UUID(task_uuid)
        except ValueError:
            raise NotFound
        task = TaskInstance.query.filter_by(uuid=task_uuid).first()
        if task is None:
            raise NotFound('Task {} not found for user {}'.format(task_uuid, current_user.username))
        return task.dict(style='result')


@ns.route('/utilities/')
class UtilityList(Resource):
    @login_required
    @ns.expect(AuthParser())
    def get(self):
        """
        Retrieve information on the available analysis utilities
        """
        response = [utility.get_description() for utility in UTILITY_MAP.values() if utility.utility_name != 'comparison']
        return response

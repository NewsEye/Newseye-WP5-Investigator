from flask import current_app
from flask_login import login_required, current_user
from flask_restplus import Resource
from app.auth import AuthParser
from app.main import controller
from app.investigator import ns
from app.models import TaskInstance
from uuid import UUID
from werkzeug.exceptions import BadRequest, InternalServerError, NotFound


@ns.route('/')
class AnalysisTaskList(Resource):
    @login_required
    @ns.expect(AuthParser())
    def get(self):
        """
        Retrieve all investigation tasks started by the user
        """
        tasks = [task.dict(style='result') for task in
                 TaskInstance.query.filter_by(user_id=current_user.id, task_type='analysis').all()]
        if len(tasks) == 1:
            tasks = tasks[0]
        return tasks

    # Define parser for the POST endpoint
    post_parser = AuthParser()
    post_parser.add_argument('search_query', type=dict, location='json', help='A JSON object containing a search query that defines the input data for the investigator')
    post_parser.add_argument('source_uuid', location='json', help='A task_uuid that defines the input data for the investigator')
    post_parser.add_argument('force_refresh', type=bool, default=False, location='json', help='Set to true to redo the analysis even if an older result exists')

    # TODO: force_refresh levels, e.g. to update only top-most 
    
    
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
        query = ('investigator', args)
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
@ns.param('task_uuid', "The UUID of the investigator task for which results should be retrieved")
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


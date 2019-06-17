from flask import current_app, request
from flask_login import login_required, current_user
from flask_restplus import Resource
from app.auth import AuthParser
from app.main import controller
from app.models import Task
from app.search import ns
from uuid import UUID
from werkzeug.exceptions import InternalServerError, NotFound


@ns.route('/')
class SearchTaskList(Resource):
    @login_required
    @ns.expect(AuthParser())
    def get(self):
        """
        Retrieve all search tasks started by the user
        """
        tasks = [task.dict(style='result') for task in Task.query.filter_by(user_id=current_user.id, task_type='search').all()]
        if len(tasks) == 1:
            tasks = tasks[0]
        return tasks

    post_parser = AuthParser()
    post_parser.add_argument('q', location='json', help='A search query. Search the document text fields by default.')

    @login_required
    @ns.expect(post_parser)
    @ns.response(200, 'The task has been executed, and the results are ready for retrieval')
    @ns.response(202, 'The task has been accepted, and is still running.')
    def post(self):
        """
        Start a new search task defined in the body. Accepts the same arguments as the underlying SolR index.
        By default searches the full text fields, and returns documents matching one or more of the keywords.
        """
        args = request.json
        query = ('search', args)
        try:
            task = controller.execute_tasks(query)[0].dict()
            if task['task_status'] == 'finished':
                return task
            elif task['task_status'] == 'running':
                return task, 202
            else:
                raise InternalServerError
        except Exception as e:
            current_app.logger.exception(e)
            raise InternalServerError


@ns.route('/<string:task_uuid>')
@ns.param('task_uuid', "The UUID of the search task for which results should be retrieved")
class SearchTask(Resource):
    @login_required
    @ns.expect(AuthParser())
    @ns.response(200, 'Success')
    @ns.response(404, "A task matching the specified task_uuid wasn't found for the user.")
    def get(self, task_uuid):
        """
        Retrieve results for a search task
        """
        try:
            task_uuid = UUID(task_uuid)
        except ValueError:
            raise NotFound
        task = Task.query.filter_by(uuid=task_uuid, user_id=current_user.id, task_type='search').first()
        if task is None:
            raise NotFound('Task {} not found for user {}'.format(task_uuid, current_user.username))
        return task.dict(style='result')

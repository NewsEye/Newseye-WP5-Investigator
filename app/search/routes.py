from flask import request, current_app
from flask_login import login_required, current_user
from flask_restplus import Resource
from app.main import controller
from app.models import Task
from app.search import api
from werkzeug.exceptions import InternalServerError


@api.route('/')
class SearchTaskList(Resource):
    @login_required
    def get(self):
        """
        Returns all search tasks started by the user
        """
        tasks = [task.dict(style='result') for task in Task.query.filter_by(user_id=current_user.id, task_type='search').all()]
        if len(tasks) == 1:
            tasks = tasks[0]
        return tasks

    @login_required
    def post(self):
        """
        Starts a new search task
        """
        query = request.json
        if isinstance(query, list):
            query = [('search', item) for item in query]
        else:
            query = [('search', query)]
        try:
            results = [task.dict() for task in controller.execute_tasks(query)]
            status = 200
            for task in results:
                if task['task_status'] != 'finished':
                    status = 202
                    break
            if len(results) == 1:
                results = results[0]
            return results, status
        except Exception as e:
            current_app.logger.exception(e)
            raise InternalServerError


@api.route('/<string:task_uuid>')
class SearchTask(Resource):
    @login_required
    def get(self, task_uuid):
        """
        Displays a search task's results
        """
        task = Task.query.filter_by(uuid=task_uuid, user_id=current_user.id, task_type='search').first()
        if task is None:
            return 'Task {} not found for user {}'.format(task_uuid, current_user.username), 404
        return task.dict(style='result')

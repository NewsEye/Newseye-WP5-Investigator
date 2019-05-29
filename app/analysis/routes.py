from flask import request, current_app
from flask_login import login_required, current_user
from flask_restplus import Resource
from app.main import controller
from app.analysis import ns
from app.models import Task
from app.analysis import UTILITY_MAP
from werkzeug.exceptions import BadRequest, InternalServerError


@ns.route('/')
class AnalysisTaskList(Resource):
    @login_required
    def get(self):
        tasks = [task.dict(style='result') for task in
                 Task.query.filter_by(user_id=current_user.id, task_type='analysis').all()]
        if len(tasks) == 1:
            tasks = tasks[0]
        return tasks

    @login_required
    def post(self):
        # POST: Start a new analysis task, and return its basic information to the user
        query = request.json
        if not isinstance(query, list):
            query = [query]
        query = [('analysis', item) for item in query]
        for item in query:
            utility_name = item[1].get('utility')
            if utility_name is None:
                raise BadRequest("Required parameter 'utility' missing for request {}".format(item[1]))
            if utility_name not in UTILITY_MAP.keys():
                raise BadRequest("Utility '{}' is currently not supported.".format(utility_name))
        try:
            results = [task.dict() for task in controller.execute_tasks(query)]
            # If any of the tasks is not finished, the status code is set to 202, otherwise it is 200
            status = 200
            for task in results:
                if task['task_status'] != 'finished':
                    status = 202
                    break
            if len(results) == 1:
                results = results[0]
            return results, status
        except KeyError as e:
            current_app.logger.exception(e)
            raise BadRequest('Missing parameter for chosen analysis utility')
        except Exception as e:
            current_app.logger.exception(e)
            raise InternalServerError


@ns.route('/<string:task_uuid>')
class AnalysisTask(Resource):
    @login_required
    def get(self, task_uuid):
        task = Task.query.filter_by(uuid=task_uuid, user_id=current_user.id, task_type='analysis').first()
        if task is None:
            return 'Task {} not found for user {}'.format(task_uuid, current_user.username), 404
        return task.dict(style='result')


@ns.route('/utilities/')
class UtilityList(Resource):
    @login_required
    def get(self):
        response = [utility.get_description() for utility in UTILITY_MAP.values()]
        return response, 200

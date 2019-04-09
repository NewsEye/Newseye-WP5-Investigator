from flask import jsonify, request
from flask_login import login_required, current_user
from app.report import bp
from app.models import Task, Report
from app.report.report_utils import generate_report, get_history


@bp.route('/history/')
@login_required
def history():
    return jsonify(get_history())


@bp.route('/<string:task_uuid>', methods=['GET'])
@login_required
def get_task_report(task_uuid):
    report_language = request.args.get('language', 'en')
    report_format = request.args.get('format', 'p')
    task = Task.query.filter_by(uuid=task_uuid, user_id=current_user.id).first()
    if task is None:
        return 'Task {} not found for user {}'.format(task_uuid, current_user.username), 404
    task_report = Report.query.filter_by(task_uuid=task_uuid, report_format=report_format, report_language=report_language).first()
    if task_report:
        return jsonify(task_report.report_content)
    else:
        task_report = generate_report(task, report_language, report_format)
    return jsonify(task_report.report_content)

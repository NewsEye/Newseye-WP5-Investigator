from app import db
from app.models import Report


def generate_report(task, report_language, report_format):
    task_report = Report(report_language=report_language,
                         report_format=report_format,
                         task_uuid=task.uuid,
                         report_content={
                            'language': report_language,
                            'body': "<{}>...</{}>".format(report_format, report_format),
                            'head': "<h1>Report for {}</h1>".format(task)
                         })
    db.session.add(task_report)
    task.task_report = task_report
    db.session.commit()
    return task_report

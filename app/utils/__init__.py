from flask import current_app
from app.models import Task, InvestigatorRun
from app import db

def update_status(app):
    with app.app_context():
        current_app.logger.info("Updating status")

        tasks = Task.query.filter_by(task_status="running").all()
        for task in tasks:
            task.task_status = "stopped"
            db.session.commit()
        current_app.logger.info("%s running tasks updated to stopped" %len(tasks))
            
        tasks = Task.query.filter_by(task_status="created").all()
        for task in tasks:
            task.task_status = "stopped"
            db.session.commit()
        current_app.logger.info("%s created tasks updated to stopped" %len(tasks))
            
        runs = InvestigatorRun.query.filter_by(run_status="running").all()
        for run in runs:
            run.run_status = "stopped"
            db.session.commit()
        current_app.logger.info("%s running runs updated to stopped" %len(runs))
        
        runs = InvestigatorRun.query.filter_by(run_status="created").all()
        for run in runs:
            run.run_status = "stopped"
            db.session.commit()
        current_app.logger.info("%s created runs updated to stopped" %len(runs))


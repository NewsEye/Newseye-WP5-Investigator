from app import create_app, db
from app.models import User, Result, Task, Report, InvestigatorRun


app = create_app()

from app.analysis import initialize_processors
initialize_processors(app)

from app.utils import update_status
update_status(app)



@app.shell_context_processor
def make_shell_context():
    return {"db": db, "User": User, "Result": Result, "Task": Task, "Report": Report}


def main():
    app.run(host="0.0.0.0")


if __name__ == "__main__":
    main()

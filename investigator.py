from app import create_app, db
from app.models import User, Result, Task, Report

app = create_app()


@app.shell_context_processor
def make_shell_context():
    return {"db": db, "User": User, "Result": Result, "Task": Task, "Report": Report}


def main():
    app.run(host="0.0.0.0")


if __name__ == "__main__":
    main()

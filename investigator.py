from app import create_app, db
from app.models import User, Query, Task

app = create_app()


@app.shell_context_processor
def make_shell_context():
    return {'db': db, 'User': User, 'Query': Query, 'Task': Task}


# def main():
#     app.run(host='0.0.0.0')
#
#
# if __name__ == '__main__':
#     main()

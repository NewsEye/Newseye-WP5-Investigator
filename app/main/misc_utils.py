from flask_login import current_user

from app.main.core import Task


def get_history(make_tree=True):
    tasks = Task.query.filter_by(user_id=current_user.id)
    user_history = dict(zip([task.uuid for task in tasks], [task.dict(style='full') for task in tasks]))
    if not make_tree:
        return user_history
    tree = {'root': []}
    if not user_history:
        return tree
    for task in user_history.values():
        parent = task['hist_parent_id']
        if parent:
            if 'children' not in user_history[parent].keys():
                user_history[parent]['children'] = []
            user_history[parent]['children'].append(task)
        else:
            tree['root'].append(task)
    return tree

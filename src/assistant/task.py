class Task(dict):

    __getattr__ = dict.__getitem__

    __setattr__ = dict.__setitem__

    __delattr__ = dict.__delitem__

    def __init__(self, task_id=None, task_type=None, task_parameters=None, task_status="created", task_result=None, result_id=None, parent_id=None, username=None, created_on=None, last_updated=None):
        self['task_id'] = task_id
        self['task_type'] = task_type
        self['task_parameters'] = task_parameters
        self['task_status'] = task_status
        self['task_result'] = task_result
        self['result_id'] = result_id
        self['parent_id'] = parent_id
        self['username'] = username
        self['created_on'] = created_on
        self['last_updated'] = last_updated

import uuid


class Query(dict):

    __getattr__ = dict.__getitem__

    __setattr__ = dict.__setitem__

    __delattr__ = dict.__delitem__

    def __init__(self, query_id=None, query=None, result=None, parent_id=None, username=None):
        self['query_id'] = query_id
        self['query'] = query
        self['result'] = result
        self['parent_id'] = parent_id
        self['username'] = username


    @property
    def query_id(self):
        return self['query_id']

    @property
    def query(self):
        return self['query']

    @property
    def result(self):
        return self['result']

    @result.setter
    def result(self, result):
        self['result'] = result

    @property
    def parent_id(self):
        return self['parent_id']

    @property
    def username(self):
        return self['username']

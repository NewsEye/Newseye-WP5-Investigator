import uuid


class State(dict):

    __getattr__ = dict.__getitem__

    __setattr__ = dict.__setitem__

    __delattr__ = dict.__delitem__

    def __init__(self, state_id=uuid.uuid4().hex, last_query=None, query_results=None, parent_id=None):
        self['id'] = state_id
        self['last_query'] = last_query
        self['query_results'] = query_results
        self['parent'] = parent_id
        self['children'] = []
        self['analysis_results'] = {}

    @property
    def id(self):
        return self['id']

    @property
    def last_query(self):
        return self['last_query']

    @property
    def analysis_results(self):
        return self['analysis_results']

    @property
    def query_results(self):
        return self['query_results']

    @query_results.setter
    def query_results(self, result):
        self['query_results'] = result

    @property
    def parent(self):
        return self['parent']

    @property
    def children(self):
        return self['children']

    def add_child(self, child_id):
        self['children'].append(child_id)

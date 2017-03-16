import json


class SQLTree(object):
    def __init__(self):
        self.query = None
        self.success = False
        self.query_type = None
        self.expressions = []
        self.db = None
        self.table = None
        self.fields = {}
        self.options = {}
        self.where = None
        self.limit = None
        self.order = {
            'by': None,
            'direction': 'ASC'
        }
        self.wait = False

    def reset(self):
        self.__init__()

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return json.dumps({
            'query': self.query,
            'success': self.success,
            'query_type': self.query_type,
            'expressions': self.expressions,
            'db': self.db,
            'table': self.table,
            'fields': self.fields,
            'options': self.options,
            'where': self.where,
            'limit': self.limit,
            'order': self.order
        })

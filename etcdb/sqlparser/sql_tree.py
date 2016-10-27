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
        self.limit = None
        self.order = {
            'by': None,
            'direction': 'ASC'
        }

    def reset(self):
        self.__init__()

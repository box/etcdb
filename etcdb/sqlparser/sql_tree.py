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

    def reset(self):
        self.__init__()

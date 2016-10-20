from etcdb.etcddate import EtcdDate
from etcdb.etcdtime import EtcdTime


class EtcdTimestamp(object):
    """
    An object holding a time stamp value.
    """
    def __init__(self, year, month, day, hour, minute, second):
        self.date = EtcdDate(year, month, day)
        self.time = EtcdTime(hour, minute, second)

    @property
    def year(self):
        return self.date.year

    @property
    def month(self):
        return self.date.month

    @property
    def day(self):
        return self.date.day

    @property
    def hour(self):
        return self.time.hour

    @property
    def minute(self):
        return self.time.minute

    @property
    def second(self):
        return self.time.second

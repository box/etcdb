class EtcdTime(object):
    """
    An object holding a time value
    """
    def __init__(self, hour, minute, second):
        self.hour = hour
        self.minute = minute
        self.second = second

class EtcdString(object):
    """An object capable of holding a binary (long) string value. """
    def __init__(self, string):
        self._string = string

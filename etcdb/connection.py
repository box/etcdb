# -*- coding: utf-8 -*-
"""Connection class definition"""

from pyetcd.client import Client

from etcdb import NotSupportedError
from etcdb.cursor import Cursor


class Connection(object):
    """Etcd connection"""
    _db = None
    """Database name"""

    def __init__(self, timeout=1, **kwargs):

        self._client = Client(**self._santize_pyetcd_kwargs(kwargs))
        if 'db' in kwargs:
            self._db = kwargs['db']
        self._timeout = timeout
        self._cursor = Cursor(self)

    @property
    def client(self):
        """Return etcd client instance"""
        return self._client

    @staticmethod
    def close():
        """Close the connection now (rather than whenever .
        __del__() is called). """
        pass

    @staticmethod
    def commit():
        """Commit any pending transaction to the database."""
        raise NotSupportedError('Transactions are not supported by etcd')

    @staticmethod
    def rollback():
        """This method is optional since not all databases provide
        transaction support."""
        raise NotSupportedError('Transactions are not supported by etcd')

    def cursor(self):
        """Return a new Cursor Object using the connection."""
        return self._cursor

    @staticmethod
    def autocommit(autocommit):
        """Set autocommit mode. Does nothing for non-transactional etcd"""
        pass

    @staticmethod
    def _santize_pyetcd_kwargs(kwargs):
        """
        Strips out keyword arguments that aren't listed in allowed_kwargs.

        :param kwargs: input dictionary with keyword arguments.
        :return: dictionary without non-allowed keys.
        """
        allowed_kwargs = [
            'host', 'port', 'srv_domain', 'version_prefix', 'protocol',
            'allow_reconnect'
        ]
        args = {}
        for arg in kwargs:
            if arg in allowed_kwargs:
                args[arg] = kwargs[arg]
        return args

    @property
    def db(self):
        """Current database."""
        return self._db

    @property
    def timeout(self):
        """Connection timeout."""
        return self._timeout


def Connect(**kwargs):  # pylint: disable=invalid-name
    """Factory function for Connection."""
    return Connection(**kwargs)

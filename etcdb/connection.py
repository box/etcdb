# -*- coding: utf-8 -*-
import inspect

from pyetcd.client import Client

from etcdb import NotSupportedError
from etcdb.converters import conversions
from etcdb.cursor import Cursor


class Connection(object):
    """Etcd connection"""
    _db = None
    """Database name"""

    def __init__(self, timeout=1, **kwargs):

        self._client = Client(**self._santize_pyetcd_kwargs(kwargs,
                                                            inspect.getargspec(Client.__init__).args))
        self.client = self._client
        self.encoders = conversions
        if 'db' in kwargs:
            self._db = kwargs['db']
        self._timeout = timeout
        self._cursor = Cursor(self)

    def close(self):
        """Close the connection now (rather than whenever .__del__() is called). """
        pass

    def commit(self):
        """Commit any pending transaction to the database."""
        raise NotSupportedError('Transactions are not supported by etcd')

    def rollback(self):
        """This method is optional since not all databases provide transaction support."""
        raise NotSupportedError('Transactions are not supported by etcd')

    def cursor(self):
        """Return a new Cursor Object using the connection."""
        return self._cursor

    def autocommit(self, autocommit):
        pass

    @staticmethod
    def _santize_pyetcd_kwargs(kwargs, allowed_kwargs):
        """
        Strips out keyword arguments that aren't listed in allowed_kwargs

        :param kwargs: input dictionary with keyword arguments
        :param allowed_kwargs: list of allowed keyword arguments
        :return: dictionary without non-allowed keys
        """
        args = {}
        for arg in kwargs:
            if arg in allowed_kwargs:
                args[arg] = kwargs[arg]
        return args

    @property
    def db(self):
        return self._db

    @property
    def timeout(self):
        return self._timeout


def Connect(**kwargs):
    """Factory function for Connection."""
    return Connection(**kwargs)

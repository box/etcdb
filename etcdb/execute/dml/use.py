"""Implement USE query."""
from pyetcd import EtcdKeyNotFound

from etcdb import OperationalError


def use_database(etcd_client, tree):
    """
    Return database name if it exists or raise exception.

    :param etcd_client: etcd client
    :type etcd_client: pyetcd.client.Client
    :param tree: Parsing tree.
    :type tree: SQLTree
    :return: Database name
    :raise OperationalError: if database doesn't exist.
    """
    try:
        etcd_client.read('/%s' % tree.db)
        return tree.db
    except EtcdKeyNotFound:
        raise OperationalError("Unknown database '%s'" % tree.db)

"""Module with Data Defition Language queries."""
from pyetcd import EtcdKeyNotFound

from etcdb import OperationalError


def database_exists_or_raise(etcd_client, db):
    """If database db doesn't exit raise OperationalError.

    :param etcd_client: Etcd client.
    :type etcd_client: Client
    :param db: Database name.
    :type db: str
    :raise OperationalError: if database doesn't exist"""
    if not db:
        raise OperationalError('No database selected')
    # Check if database exists
    try:
        etcd_client.read('/%s' % db)
    except EtcdKeyNotFound:
        raise OperationalError("Unknown database '%s'" % db)

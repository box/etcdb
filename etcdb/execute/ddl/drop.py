"""Implement DROP queries."""
from pyetcd import EtcdKeyNotFound

from etcdb import OperationalError
from etcdb.execute.ddl import database_exists_or_raise


def drop_database(etcd_client, tree):
    """
    Drop database.

    :param etcd_client: Etcd client
    :type etcd_client: Client
    :param tree: Parsing tree
    :type tree: SQLTree
    :raise OperationalError: if database doesn't exist
    """
    try:
        etcd_client.rmdir('/%s' % tree.db, recursive=True)
    except EtcdKeyNotFound:
        raise OperationalError("Can't drop database '%s';"
                               " database doesn't exist" % tree.db)


def drop_table(etcd_client, tree, db=None):
    """
    Drop table.

    :param etcd_client: Etcd client
    :type etcd_client: Client
    :param tree: Parsing tree
    :type tree: SQLTree
    :param db: Database name to use if not defined in the parsing tree.
    :type db: str
    :raise OperationalError: if database is not selected
        or if table doesn't exist.
    """
    if not db:
        db = tree.db
    database_exists_or_raise(etcd_client, db)

    try:
        key = '/%s/%s' % (db, tree.table)
        etcd_client.rmdir(key, recursive=True)
    except EtcdKeyNotFound:
        if tree.options['if_exists']:
            pass
        else:
            raise OperationalError("Unknown table '%s'" % tree.table)

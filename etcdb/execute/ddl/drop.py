from pyetcd import EtcdKeyNotFound

from etcdb import OperationalError


def drop_database(etcd_client, tree):
    """

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

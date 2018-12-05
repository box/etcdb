"""
Data modification language routines.
"""
from etcdb import OperationalError
from etcdb.lock import WriteLock


def get_exclusive_lock(etcd_client, tree, db):
    """
    Acquire a write lock on a table. The lock may be explicitly
    given from a parsing tree when UPDATE or INSERT specifies it with
    the USE LOCK statement.

    :param etcd_client: etcd connection
    :type etcd_client: pyetcd.client.Client
    :param tree: Parsing tree
    :type tree: SQLTree
    :param db: Database name. It doesn't necessary come from the parsing tree.
        That's why it has to specified.
    :type db: str
    :return: Write lock on a table from the parsing tree in the given
        database db.
    :rtype: WriteLock
    """
    lock = WriteLock(etcd_client, db, tree.table)
    if tree.lock is None:
        lock.acquire()
    else:
        valid_lock = False
        for write_lock in lock.writers():
            if write_lock.id == tree.lock:
                valid_lock = True

        if not valid_lock:
            raise OperationalError(
                'Lock %s has no grant to update' % tree.lock
            )
    return lock

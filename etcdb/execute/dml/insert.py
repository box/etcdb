"""Implement INSERT query."""
import json

from pyetcd import EtcdKeyNotFound

from etcdb import IntegrityError, ProgrammingError
from etcdb.execute.dml import get_exclusive_lock
from etcdb.resultset import ColumnSet


def get_table_columns(etcd_client, db, tbl):
    """
    Get primary key column for table db.tbl.

    :param etcd_client: Etcd client.
    :param db: database name.
    :param tbl: table name.
    :return: Primary key column.
    :rtype: ColumnSet
    :raise ProgrammingError: if table or database doesn't exist
    """
    try:
        response = etcd_client.read('/{db}/{tbl}/_fields'.format(db=db,
                                                                 tbl=tbl))
        _fields = response.node['value']
        return ColumnSet(json.loads(_fields))
    except EtcdKeyNotFound:
        raise ProgrammingError("Table %s.%s doesn't exist" % (db, tbl))


def get_pk_field(etcd_client, db, tbl):
    """
    Get primary key column for table db.tbl.

    :param etcd_client: Etcd client.
    :param db: database name.
    :param tbl: table name.
    :return: Primary key column.
    :rtype: Column
    """
    column_set = get_table_columns(etcd_client, db, tbl)
    return column_set.primary


def _get_next_auto_inc(etcd_client, db, tbl):
    key = '/{db}/{tbl}/_auto_inc'.format(
        db=db,
        tbl=tbl,
    )
    try:
        etcd_result = etcd_client.read(key)
        return int(etcd_result.node['value'])
    except EtcdKeyNotFound:
        return 1


def _set_next_auto_inc(etcd_client, db, tbl):
    auto_inc_value = _get_next_auto_inc(etcd_client, db, tbl)
    key = '/{db}/{tbl}/_auto_inc'.format(
        db=db,
        tbl=tbl,
    )
    etcd_client.write(key, auto_inc_value + 1)


def insert(etcd_client, tree, db):
    """
    Execute INSERT query

    :param etcd_client: etcd client
    :type etcd_client: pyetcd.client.Client
    :param tree: Parse tree
    :type tree: SQLTree
    :param db: Current database
    :type db: str
    :raise IntegrityError: if duplicate primary key
    """
    lock = get_exclusive_lock(etcd_client, tree, db)

    primary_key = None

    try:

        # get pk value
        pk_field = get_pk_field(etcd_client, db, tree.table)
        try:
            primary_key = tree.fields[str(pk_field)]
        except KeyError:
            if pk_field.auto_increment:
                primary_key = _get_next_auto_inc(etcd_client, db, tree.table)
                tree.fields[str(pk_field)] = primary_key
            else:
                raise IntegrityError("Primary key is not given")

        etcd_client.read('/{db}/{tbl}/{pk}'
                         .format(db=db,
                                 tbl=tree.table,
                                 pk=primary_key))
        raise IntegrityError("Duplicate entry '%s' for key 'PRIMARY'"
                             % primary_key)
    except EtcdKeyNotFound:
        if primary_key:
            etcd_client.write('/{db}/{tbl}/{pk}'
                              .format(db=db,
                                      tbl=tree.table,
                                      pk=primary_key),
                              json.dumps(tree.fields))
            _set_next_auto_inc(etcd_client, db, tree.table)
            return 1
        raise IntegrityError("Primary key is not given")

    finally:
        if tree.lock is None:
            lock.release()

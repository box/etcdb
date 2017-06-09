from pyetcd import EtcdKeyNotFound

from etcdb import OperationalError
from etcdb.resultset import ResultSet, ColumnSet, Column, Row


def show_databases(etcd_client):
    """
    Execute SHOW [FULL] TABLES query

    :param etcd_client: etcd client
    :type etcd_client: pyetcd.client.Client
    :return: ResultSet instance
    :rtype: ResultSet
    """
    etcd_response = etcd_client.read('/')
    columns = ColumnSet().add(Column('Database'))
    result_set = ResultSet(columns)
    try:
        for node in etcd_response.node['nodes']:
            val = node['key'].lstrip('/')
            result_set.add_row(Row((val,)))

    except KeyError:
        pass
    return result_set


def show_tables(etcd_client, tree, db):
    """
    Execute SHOW [FULL] TABLES query#

    :param etcd_client: etcd client
    :type etcd_client: pyetcd.client.Client
    :param db: Current database
    :type db: str
    :param tree: Parse tree
    :type tree: SQLTree
    :return: ResultSet instance
    :rtype: ResultSet
    """
    columns = ColumnSet().add(Column('Tables_in_%s' % db))
    if tree.options['full']:
        columns.add(Column('Table_type'))

    result_set = ResultSet(columns)

    try:
        etcd_response = etcd_client.read('/%s' % db)
    except EtcdKeyNotFound:
        raise OperationalError('No database selected')

    try:
        for node in etcd_response.node['nodes']:
            table_name = node['key'].replace('/%s/' % db, '', 1)
            row = (table_name, )
            if tree.options['full']:
                row += ('BASE TABLE',)
            result_set.add_row(Row(row))
    except KeyError:
        pass

    return result_set

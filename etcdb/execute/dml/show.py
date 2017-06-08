from etcdb.resultset import ResultSet, ColumnSet, Column, Row


def show_databases(etcd_client):
    """
    Execute SHOW [FULL] TABLES query

    :param etcd_client: etcd client
    :type etcd_client: pyetcd.client.Client
    :param tree: Parse tree
    :type tree: SQLTree
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


def show_tables(etcd_client, db, tree):
    """
    Execute SHOW [FULL] TABLES query

    :param etcd_client: etcd client
    :type etcd_client: pyetcd.client.Client
    :param db: Current database
    :type db: str
    :param tree: Parse tree
    :type tree: SQLTree
    :return: ResultSet instance
    :rtype: ResultSet
    """

    etcd_response = etcd_client.read('/%s' % db)
    columns = ColumnSet()

    # rows = ResultSet()

    try:
        for node in etcd_response.node['nodes']:
            row = (node['key'].replace('/%s/' % db, '', 1),)
            if tree.options['full']:
                row += ('BASE TABLE',)
            rows += (row,)
    except KeyError:
        pass

    col_names = ('Table',)

    if tree.options['full']:
        col_names += ('Type',)

    return col_names, rows




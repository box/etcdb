import json

from pyetcd import EtcdKeyNotFound

from etcdb import OperationalError, ProgrammingError
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


def desc_table(etcd_client, tree, db):
    """
    Execute DESC table query#

    :param etcd_client: etcd client
    :type etcd_client: pyetcd.client.Client
    :param tree: Parse tree
    :type tree: SQLTree
    :param db: Current database
    :type db: str
    :return: ResultSet instance
    :rtype: ResultSet
    """
    key = '/{db}/{table}/_fields'.format(db=db, table=tree.table)
    try:
        etcd_result = etcd_client.read(key)
    except EtcdKeyNotFound:
        raise ProgrammingError('Table `{db}`.`{table}` '
                               'doesn\'t exist'.format(db=db,
                                                       table=tree.table))
    columns = ColumnSet()
    columns.add(Column('Field'))
    columns.add(Column('Type'))
    columns.add(Column('Null'))
    columns.add(Column('Key'))
    columns.add(Column('Default'))
    columns.add(Column('Extra'))

    result_set = ResultSet(columns)

    fields = json.loads(etcd_result.node['value'])

    for k, v in fields.iteritems():
        field_type = v['type']

        if v['options']['nullable']:
            nullable = 'YES'
        else:
            nullable = 'NO'

        indexes = ''
        if 'primary' in v['options'] and v['options']['primary']:
            indexes = 'PRI'

        if 'unique' in v['options'] and v['options']['unique']:
            indexes = 'UNI'

        try:
            default_value = v['options']['default']
        except KeyError:
            default_value = ''

        extra = ''
        if 'auto_increment' in v['options'] and v['options']['auto_increment']:
            extra = 'auto_increment'

        row = (k, field_type, nullable, indexes, default_value, extra)
        result_set.add_row(Row(row))

    return result_set

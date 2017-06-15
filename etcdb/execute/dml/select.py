"""Implement SELECT query."""
import json

from etcdb import OperationalError
from etcdb.eval_expr import eval_expr, EtcdbFunction, etcdb_version, etcdb_count
from etcdb.execute.dml.insert import get_table_columns
from etcdb.resultset import ResultSet, ColumnSet, Column, Row


def list_table(etcd_client, db, tbl):
    """
    Read primary key values in table db.tbl.

    :param etcd_client: etcd client.
    :type etcd_client: pyetcd.client.Client
    :param db: database name.
    :param tbl: table name.
    :return: list of primary keys.
    :rtype: list
    """
    pks = []
    table_key = "/{db}/{tbl}".format(db=db, tbl=tbl)
    etcd_result = etcd_client.read(table_key)

    if 'nodes' in etcd_result.node:

        nodes = etcd_result.node['nodes']

        for node in nodes:
            pk_key = node['key']
            primary_key = pk_key.replace(table_key + '/', '', 1)
            pks.append(primary_key)

        pks = sorted(pks)

        return pks
    else:
        return []


def prepare_columns(tree):
    """
    Generate ColumnSet for query result. ColumnsSet doesn't include
    a grouping function.

    :return: Columns of the query result.
    :rtype: ColumnSet
    """
    columns = ColumnSet()
    for select_item in tree.expressions:

        expr, alias = select_item

        colname, colvalue = eval_expr(None, tree=expr)
        if alias:
            colname = alias

        columns.add(Column(colname))

    return columns


def get_row_by_primary_key(etcd_client, db, table, primary_key):
    """
    Read row from etcd by its primary key value.

    :param etcd_client:
    :param db:
    :param table:
    :param primary_key: Primary key value.
    :return: Row
    :rtype: Row
    """
    key = "/{db}/{tbl}/{pk}".format(db=db,
                                    tbl=table,
                                    pk=primary_key)
    etcd_response = etcd_client.read(key)
    row = ()
    for _, value in json.loads(etcd_response.node['value']).iteritems():
        row += (value,)

    return Row(row)


def group_function(table_columns, table_row, tree):
    """True if resultset should be grouped

    :return: Grouping function or None and its position.
    :rtype: tuple(EtcdbFunction, int)"""

    try:

        group_position = 0
        for select_item in tree.expressions:
            expr = select_item[0]
            expr_value = eval_expr((table_columns, table_row),
                                   tree=expr)[1]

            if isinstance(expr_value, EtcdbFunction):

                if expr_value.group:
                    return expr_value, group_position
            group_position += 1
        return None, None
    except TypeError:
        return None, None


def eval_row(table_columns, table_row, tree, result_set):
    """Find values of a row. table_columns are fields in the table.
    The result columns is taken from tree.expressions.

    :param table_columns: Columns in the table row.
    :type table_columns: ColumnSet
    :param table_row: Input row.
    :type table_row: Row
    :param tree: Parsing tree.
    :type tree: SQLTree
    :param result_set: Some functions like COUNT(*) need result set to
        calculate return value.
    :type result_set: ResultSet
    """
    result_row = ()

    for select_item in tree.expressions:
        expr = select_item[0]
        expr_value = eval_expr((table_columns, table_row),
                               tree=expr)[1]

        result_row += (expr_value, )

    return result_row


def group_result_set(func, result_set, table_row, tree, pos):
    """Apply a group function to result set and return an aggregated row.

    :param func: Aggregation function.
    :type func: callable
    :param result_set: Result set to aggregate.
    :type result_set: ResultSet
    :param table_row: Table row to base aggregated row on.
    :type table_row: Row
    :param tree: Parsing tree.
    :type tree: SQLTree
    :param pos: Aggregate function position in the resulting row.
    :type pos: int
    :return: Result set with aggregated row.
    :rtype: ResultSet"""
    group_value = func(result_set)
    row = list(eval_row(result_set.columns, table_row, tree, result_set))
    row[pos] = group_value
    row = Row(tuple(row))
    return ResultSet(prepare_columns(tree), [row])


def execute_select_plain(etcd_client, tree, db):
    """Execute SELECT that reads rows from table."""

    result_columns = prepare_columns(tree)
    result_set = ResultSet(result_columns)

    table_columns = get_table_columns(etcd_client, db, tree.table)

    last_row = None
    for primary_key in list_table(etcd_client, db, tree.table):

        table_row = get_row_by_primary_key(etcd_client, db, tree.table,
                                           primary_key)

        if tree.where:
            expr = tree.where
            if eval_expr((table_columns, table_row), expr)[1]:
                row = eval_row(table_columns, table_row, tree, result_set)
                result_set.add_row(row)
                last_row = table_row
        else:
            row = eval_row(table_columns, table_row, tree, result_set)
            result_set.add_row(row)
            last_row = table_row

    g_function, pos = group_function(table_columns, last_row, tree)
    if g_function:
        result_set = group_result_set(g_function, result_set, last_row,
                                      tree, pos)

    return result_set


def execute_select_no_table(tree):
    """Execute SELECT that doesn't read from a table.
    SELECT VERSION() or similar."""
    result_columns = prepare_columns(tree)
    result_set = ResultSet(result_columns)

    result_row = ()

    for select_item in tree.expressions:
        expr = select_item[0]
        expr_value = eval_expr((result_columns, None), tree=expr)[1]
        result_row += (expr_value, )

    result_set.add_row(result_row)
    return result_set


def fix_tree_star(tree, etcd_client, db, tbl):
    """If parsing tree contains [["*", null], null] expression it means
    the query was SELECT * . So, the expressions needs to be replaced
    with actual field names.

    """
    if tree.expressions == [(("*", None), None)]:
        tree.expressions = []
        for field in get_table_columns(etcd_client, db, tbl):
            tree.expressions.append(
                (
                    (
                        'bool_primary', (
                            'predicate', (
                                'bit_expr', (
                                    'simple_expr', (
                                        'IDENTIFIER',
                                        str(field)
                                    )
                                )
                            )
                        )
                    ),
                    None
                )
            )
    return tree


def execute_select(etcd_client, tree, db):
    """
    Execute SELECT query.

    :param etcd_client: etcd client.
    :type etcd_client: pyetcd.client.Client
    :param db: Current database.
    :type db: str
    :param tree: Parse tree.
    :type tree: SQLTree
    :return: ResultSet instance.
    :rtype: ResultSet
    """
    if not db:
        raise OperationalError('No database selected')

    if tree.table:
        tree = fix_tree_star(tree, etcd_client, db, tree.table)
        result_set = execute_select_plain(etcd_client, tree, db)
    else:
        result_set = execute_select_no_table(tree)

    if tree.limit is not None:
        result_set = ResultSet(result_set.columns, result_set[:tree.limit])

    return result_set

"""Implement WAIT query."""
from etcdb.eval_expr import eval_expr
from etcdb.execute.dml.insert import get_table_columns
from etcdb.execute.dml.select import prepare_columns, list_table, \
    get_row_by_primary_key, eval_row
from etcdb.resultset import ResultSet


def execute_wait(etcd_client, tree, db):
    """Execute WAIT."""

    result_columns = prepare_columns(tree)
    result_set = ResultSet(result_columns)

    table_columns = get_table_columns(etcd_client, db, tree.table)

    for primary_key in list_table(etcd_client, db, tree.table):

        table_row = get_row_by_primary_key(etcd_client, db, tree.table,
                                           primary_key)
        etcd_index = table_row.etcd_index

        if tree.where:
            expr = tree.where
            if eval_expr((table_columns, table_row), expr)[1]:
                new_row = get_row_by_primary_key(etcd_client, db, tree.table,
                                                 primary_key, wait=True,
                                                 wait_index=etcd_index+1)
                row = eval_row(table_columns, new_row, tree)
                result_set.add_row(row)
        else:
            row = eval_row(table_columns, table_row, tree)
            result_set.add_row(row)

    return result_set

"""Implement WAIT query."""
import time

from etcdb import WAIT_WAIT_TIMEOUT, InternalError
from etcdb.eval_expr import eval_expr
from etcdb.execute.dml.insert import get_table_columns
from etcdb.execute.dml.select import prepare_columns, list_table, \
    get_row_by_primary_key, eval_row
from etcdb.resultset import ResultSet, Row


def execute_wait(etcd_client, tree, db):
    """Execute WAIT.

    :param etcd_client: Etcd client.
    :type etcd_client: Client
    :param tree: Parsing tree.
    :type tree: SQLTree
    :param db: Current database.
    :type db: str
    """

    result_columns = prepare_columns(tree)
    result_set = ResultSet(result_columns)

    table_columns = get_table_columns(etcd_client, db, tree.table)

    for primary_key in list_table(etcd_client, db, tree.table):

        table_row = get_row_by_primary_key(etcd_client, db, tree.table,
                                           primary_key)
        etcd_index = table_row.etcd_index

        if tree.where:
            expr = tree.where
            try:
                wait_index = tree.options['after']
            except KeyError:
                wait_index = etcd_index + 1

            if eval_expr((table_columns, table_row), expr)[1]:
                start = time.time()
                while True:
                    if time.time() > start + WAIT_WAIT_TIMEOUT:
                        raise InternalError('Wait timeout %d '
                                            'seconds expired'
                                            % WAIT_WAIT_TIMEOUT)
                    try:
                        new_row = get_row_by_primary_key(etcd_client,
                                                         db,
                                                         tree.table,
                                                         primary_key,
                                                         wait=True,
                                                         wait_index=wait_index)
                        break
                    except KeyError:
                        wait_index += 1
                row = Row(eval_row(table_columns, new_row, tree),
                          etcd_index=new_row.etcd_index,
                          modified_index=new_row.modified_index)
                result_set.add_row(row)
        else:
            row = Row(eval_row(table_columns, table_row, tree),
                      etcd_index=etcd_index,
                      modified_index=etcd_index)
            result_set.add_row(row)

    return result_set

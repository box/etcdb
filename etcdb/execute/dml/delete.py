"""
Implement DELETE query.
"""
from etcdb.eval_expr import eval_expr
from etcdb.execute.dml.insert import get_table_columns
from etcdb.execute.dml.select import list_table, get_row_by_primary_key
from etcdb.lock import WriteLock


def _delete_key(etcd_client, key):
    etcd_client.delete(key)


def execute_delete(etcd_client, tree, db):
    """Execute DELETE query"""
    lock = WriteLock(etcd_client, db, tree.table)
    lock.acquire()
    try:

        affected_rows = 0
        table_columns = get_table_columns(etcd_client, db, tree.table)
        for primary_key in list_table(etcd_client, db, tree.table):

            table_row = get_row_by_primary_key(etcd_client, db, tree.table,
                                               primary_key)

            key = "/{db}/{tbl}/{pk}".format(
                db=db,
                tbl=tree.table,
                pk=primary_key
            )

            if tree.where:
                expr = tree.where
                if eval_expr((table_columns, table_row), expr)[1]:
                    _delete_key(etcd_client, key)
                affected_rows += 1
            else:
                _delete_key(etcd_client, key)
                affected_rows += 1
        return affected_rows

    finally:
        lock.release()

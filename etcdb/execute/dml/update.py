"""Implement UPDATE query."""
import json

from etcdb.eval_expr import eval_expr
from etcdb.execute.dml import get_exclusive_lock
from etcdb.execute.dml.insert import get_table_columns
from etcdb.execute.dml.select import list_table, get_row_by_primary_key


def _update_key(etcd_client, key, value):
    etcd_client.write(key, value)


def execute_update(etcd_client, tree, db):  # pylint: disable=too-many-locals
    """Execute UPDATE query"""
    lock = get_exclusive_lock(etcd_client, tree, db)
    try:

        affected_rows = 0
        table_columns = get_table_columns(etcd_client, db, tree.table)

        for primary_key in list_table(etcd_client, db, tree.table):

            table_row = get_row_by_primary_key(etcd_client, db, tree.table,
                                               primary_key)
            key = '/{db}/{tbl}/{pk}'.format(
                db=db,
                tbl=tree.table,
                pk=primary_key
            )

            row = {}
            for column in table_columns:
                index = table_columns.index(column)
                row[str(column)] = table_row[index]

            for update_fields in tree.expressions:
                field_name, field_expression = update_fields

                field_value = eval_expr(table_row, field_expression)
                # print('Update %s with %s' % (field_name, field_value[1]))
                row[field_name] = field_value[1]

            if tree.where:

                if eval_expr((table_columns, table_row), tree.where)[1]:
                    _update_key(etcd_client, key, json.dumps(row))
                    affected_rows += 1
            else:
                _update_key(etcd_client, key, json.dumps(row))
                affected_rows += 1
        return affected_rows

    finally:
        if tree.lock is None:
            lock.release()

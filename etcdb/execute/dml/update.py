"""Implement UPDATE query."""
import json

from etcdb import OperationalError
from etcdb.eval_expr import eval_expr
from etcdb.execute.dml.insert import get_table_columns
from etcdb.execute.dml.select import list_table, get_row_by_primary_key
from etcdb.lock import WriteLock


def _update_key(etcd_client, key, value):
    etcd_client.write(key, value)


def _get_lock(etcd_client, tree, db):
    lock = WriteLock(etcd_client, db, tree.table)
    if tree.lock is None:
        lock.acquire()
    else:
        valid_lock = False
        for write_lock in lock.writers():
            if write_lock.id == tree.lock:
                valid_lock = True

        if not valid_lock:
            raise OperationalError('Lock %s has no grant to update' % tree.lock)
    return lock


def execute_update(etcd_client, tree, db):  # pylint: disable=too-many-locals
    """Execute UPDATE query"""
    lock = _get_lock(etcd_client, tree, db)
    try:

        affected_rows = 0
        table_columns = get_table_columns(etcd_client, db, tree.table)

        for primary_key in list_table(etcd_client, db, tree.table):

            table_row = get_row_by_primary_key(etcd_client, db, tree.table,
                                               primary_key)
            key = '/{db}/{tbl}/{pk}'.format(db=db, tbl=tree.table, pk=primary_key)

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

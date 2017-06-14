import json

from etcdb.eval_expr import eval_expr
from etcdb.execute.dml.insert import get_table_columns
from etcdb.execute.dml.select import list_table, get_row_by_primary_key, \
    eval_row


def execute_update(etcd_client, tree, db):
    """Execute UPDATE query"""
    row_count = 0
    for primary_key in list_table(etcd_client, db, tree.table):



        table_row = get_row_by_primary_key(etcd_client, db, tree.table,
                                           primary_key)
        table_columns = get_table_columns(etcd_client, db, tree.table)
        if tree.where:
            expr = tree.where
            row = {}
            for column in table_columns:
                index = table_columns.index(column)
                row[str(column)] = table_row[index]
            print('In row: %r' % row)
            if eval_expr((table_columns, table_row), expr)[1]:
                for update_fields in tree.expressions:
                    field_name, field_expression = update_fields

                    field_value = eval_expr(table_row, field_expression)
                    print('Update %s with %s' % (field_name, field_value[1]))
                    row[field_name] = field_value[1]

                # row = eval_row(table_columns, table_row, tree, None)
                # print(row)
            print('Out row: %r' % row)
            etcd_client.write('/{db}/{tbl}/{pk}'
                              .format(db=db, tbl=tree.table, pk=primary_key),
                              json.dumps(row))
            row_count += 1
    return row_count

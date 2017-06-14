import pytest

from etcdb.eval_expr import EtcdbFunction, etcdb_count
from etcdb.execute.dml.select import eval_row, prepare_columns, group_result_set
from etcdb.resultset import ColumnSet, Column, Row, ResultSet
from etcdb.sqlparser.sql_tree import SQLTree


@pytest.mark.parametrize('expressions, cs', [
    (
        [
            (
                ('bool_primary',
                 ('predicate',
                  ('bit_expr',
                   ('simple_expr',
                    ('IDENTIFIER', 'id')
                    )
                   )
                  )
                 ),
                None),
            (
                ('bool_primary',
                 ('predicate',
                  ('bit_expr',
                   ('simple_expr',
                    ('IDENTIFIER', 'name')
                    )
                   )
                  )
                 ),
                None
            )
        ],
        ColumnSet().add(Column('id')).add(Column('name'))
    ),
    (
        [
            (
                ('bool_primary',
                 ('predicate',
                  ('bit_expr',
                   ('simple_expr',
                    ('IDENTIFIER', 'id')
                    )
                   )
                  )
                 ),
                None),
            (
                ('bool_primary',
                 ('predicate',
                  ('bit_expr',
                   ('simple_expr',
                    ('function_call', 'COUNT')
                    )
                   )
                  )
                 ),
                None
            )
        ],
        ColumnSet().add(Column('id')).add(Column('COUNT(*)'))
    )
])
def test_prepare_columns(expressions, cs):
    tree = SQLTree()
    tree.expressions = expressions
    actual_cs = prepare_columns(tree)
    print('Expected: %s' % cs)
    print('Actual: %s' % actual_cs)
    assert actual_cs == cs


@pytest.mark.parametrize('cs, row, expressions, result', [
    (
        ColumnSet().add(Column('id')).add(Column('name')),
        Row((1, 'aaa')),
        [
            (
                ('bool_primary',
                 ('predicate',
                  ('bit_expr',
                   ('simple_expr',
                    ('IDENTIFIER', 'id')
                    )
                   )
                  )
                 ),
                None),
            (
                ('bool_primary',
                 ('predicate',
                  ('bit_expr',
                   ('simple_expr',
                    ('IDENTIFIER', 'name')
                    )
                   )
                  )
                 ),
                None
            )
        ],
        (1, 'aaa')

    ),
    (
        ColumnSet().add(Column('id')).add(Column('COUNT(*)')),
        Row((1, 'aaa')),
        [
            (
                ('bool_primary',
                 ('predicate',
                  ('bit_expr',
                   ('simple_expr',
                    ('IDENTIFIER', 'id')
                    )
                   )
                  )
                 ),
                None),
            (
                ('bool_primary',
                 ('predicate',
                  ('bit_expr',
                   ('simple_expr',
                    ('function_call', 'COUNT')
                    )
                   )
                  )
                 ),
                '__count'
            )
        ],
        (1, EtcdbFunction(etcdb_count, group=True))

    ),
    (
        ColumnSet().add(Column('COUNT(*)')),
        None,
        [
            (
                ('bool_primary',
                 ('predicate',
                  ('bit_expr',
                   ('simple_expr',
                    ('function_call', 'COUNT')
                    )
                   )
                  )
                 ),
                '__count'
            )
        ],
        (EtcdbFunction(etcdb_count, group=True),)

    )

])
def test_eval_row(cs, row, expressions, result):
    tree = SQLTree()
    tree.expressions = expressions

    assert eval_row(cs, row, tree, ResultSet(cs)) == result


@pytest.mark.parametrize('rs, row, expressions, result', [
    (
        ResultSet(ColumnSet().add(Column(str(
            EtcdbFunction(etcdb_count, group=True)))), []),
        None,
        [
            (
                ('bool_primary',
                 ('predicate',
                  ('bit_expr',
                   ('simple_expr',
                    ('function_call', 'COUNT')
                    )
                   )
                  )
                 ),
                '__count'
            )
        ],
        ResultSet(
            ColumnSet().add(Column('__count')),
            [Row((0,))]
        )
    ),
    (
        ResultSet(ColumnSet().add(Column(str(
            EtcdbFunction(etcdb_count, group=True)))),
            [
                Row((5, )),
                Row((6, ))
            ]
        ),
        Row((6, )),
        [
            (
                ('bool_primary',
                 ('predicate',
                  ('bit_expr',
                   ('simple_expr',
                    ('function_call', 'COUNT')
                    )
                   )
                  )
                 ),
                '__count'
            )
        ],
        ResultSet(
            ColumnSet().add(Column('__count')),
            [Row((2,))]
        )
    )
])
def test_group_result_set(rs, row, expressions, result):
    tree = SQLTree()
    tree.expressions = expressions
    actual = group_result_set(etcdb_count, rs, row, tree, 0)
    print('Expected: %s' % result)
    print('Actual: %s' % actual)
    assert actual == result

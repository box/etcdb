import pytest

from etcdb.eval_expr import etcdb_count, eval_expr
from etcdb.resultset import ResultSet, ColumnSet, Column, Row
from etcdb.sqlparser.sql_tree import SQLTree


@pytest.mark.parametrize('rows, count', [
    (
        [],
        0
    ),
    (
        None,
        0
    ),
    (
        [Row(('1',))],
        1
    ),
    (
        [
            Row(('1',)),
            Row(('2',)),
            Row(('3',)),
        ],
        3
    )
])
def test_etcdb_count(rows, count):
    cs = ColumnSet().add(Column('id'))
    rs = ResultSet(cs, rows)
    assert etcdb_count(rs) == count


def test_expr_in_simple_expr():
    tree = SQLTree()
    tree.expressions = [
        (
            (
                'bool_primary',
                (
                    'predicate',
                    (
                        'bit_expr',
                        (
                            'simple_expr',
                            (
                                'expr',
                                (
                                    'bool_primary',
                                    (
                                        'predicate',
                                        (
                                            'bit_expr',
                                            (
                                                'simple_expr',
                                                (
                                                    'literal', '1'
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            ),
            'a'
        )
    ]

    assert eval_expr(None, tree.expressions[0][0]) == ('1', '1')

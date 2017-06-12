from etcdb.eval_expr import eval_identifier
from etcdb.resultset import ColumnSet, Column, Row


def test_eval_identifier():
    cs = ColumnSet()
    cs.add(Column('id'))
    cs.add(Column('name'))

    row = Row((5, 'aaa'))

    assert eval_identifier((cs, row), 'id')[1] == 5
    assert eval_identifier((cs, row), 'name')[1] == 'aaa'

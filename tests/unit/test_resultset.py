import pytest

from etcdb import ProgrammingError
from etcdb.resultset import Column, ColumnSet, Row, ResultSet


def test_column():
    col = Column('foo')
    assert col.name == 'foo'


def test_column_eq():
    col1 = Column('foo')
    col2 = Column('foo')
    assert col1 == col2


def test_column_ne():
    col1 = Column('foo')
    col2 = Column('bar')
    assert col1 != col2


def test_column_str():
    col1 = Column('foo')
    assert str(col1) == 'foo'


def test_column_set():
    cs = ColumnSet()
    col = Column('foo')
    assert isinstance(cs.add(col), ColumnSet)
    assert col in cs


def test_column_set_empty():
    cs = ColumnSet()
    assert cs.empty
    cs.add(Column('foo'))
    assert not cs.empty


def test_column_set_raise_if_wrong_type():
    with pytest.raises(ProgrammingError):
        cs = ColumnSet()
        # noinspection PyTypeChecker
        cs.add('foo')


def test_column_set_eq():
    cs1 = ColumnSet().add(Column('foo'))
    cs2 = ColumnSet().add(Column('foo'))
    assert cs1 == cs2


def test_column_set_str():
    cs = ColumnSet().add(Column('foo')).add(Column('bar'))
    assert str(cs) == '["foo", "bar"]'


def test_column_set_iterable():
    cs = ColumnSet().add(Column('foo'))
    cs.add(Column('bar'))
    assert cs.next() == Column('foo')
    assert cs.next() == Column('bar')
    with pytest.raises(StopIteration):
        cs.next()
    assert cs.next() == Column('foo')
    assert cs.next() == Column('bar')
    with pytest.raises(StopIteration):
        cs.next()


def test_column_set_getitem():
    cs = ColumnSet().add(Column('foo'))
    cs.add(Column('bar'))
    assert cs[0] == Column('foo')
    assert cs[1] == Column('bar')


def test_row_raises():
    with pytest.raises(ProgrammingError):
        # noinspection PyTypeChecker
        Row('foo')


def test_row_str():
    row = Row(('foo', 'bar'))
    assert str(row) == '["foo", "bar"]'


def test_row_eq():
    assert Row(('foo', 'bar')) == Row(('foo', 'bar'))


def test_row_iterable():
    row = Row(('foo_value1', 'bar_value1'))
    assert row.next() == 'foo_value1'
    assert row.next() == 'bar_value1'
    with pytest.raises(StopIteration):
        row.next()
    assert row.next() == 'foo_value1'
    assert row.next() == 'bar_value1'
    with pytest.raises(StopIteration):
        row.next()


@pytest.mark.parametrize('cols, rows', [
    (
        'cols',
        'bar'
    ),
    (
        ColumnSet(),
        'bar'
    )
])
def test_result_set_raises(cols, rows):
    with pytest.raises(ProgrammingError):
        ResultSet(cols, rows)


@pytest.mark.parametrize('cols, rows', [
    (
        ColumnSet().add(Column('foo')),
        None
    ),
    (
        ColumnSet().add(Column('foo')).add(Column('bar')),
        None
    ),
    (
        ColumnSet().add(Column('foo')).add(Column('bar')),
        [
            Row(('foo_value1', 'bar_value1')),
            Row(('foo_value2', 'bar_value2'))
         ]
    ),
])
def test_result_set_eq(cols, rows):
    rs1 = ResultSet(cols, rows)
    rs2 = ResultSet(cols, rows)
    assert rs1 == rs2


def test_result_set_empty_rows():
    rs = ResultSet(ColumnSet().add(Column('foo')), None)
    assert rs.rows == []


def test_result_set_str():
    cs = ColumnSet().add(Column('foo')).add(Column('bar'))
    rows = [
        Row(('foo_value1', 'bar_value1')),
        Row(('foo_value2', 'bar_value2'))
    ]
    rs = ResultSet(cs, rows)
    assert str(rs) == 'Columns: ["foo", "bar"]\n' \
                      'Rows: [["foo_value1", "bar_value1"], ' \
                      '["foo_value2", "bar_value2"]]'


def test_result_set_add_row():
    cs = ColumnSet().add(Column('foo')).add(Column('bar'))
    rows = [
        Row(('foo_value1', 'bar_value1')),
        Row(('foo_value2', 'bar_value2'))
    ]
    rs1 = ResultSet(cs, [Row(('foo_value1', 'bar_value1'))])
    rs1.add_row(Row(('foo_value2', 'bar_value2')))
    rs2 = ResultSet(cs, rows)
    print('Expected: \n%s' % rs2)
    print('Actual: \n%s' % rs1)
    assert rs1 == rs2


@pytest.mark.parametrize('cols, rows, n_rows, n_cols', [
    (
        ColumnSet().add(Column('foo')),
        None,
        0,
        1
    ),
    (
        ColumnSet().add(Column('foo')).add(Column('bar')),
        None,
        0,
        2
    ),
    (
        ColumnSet().add(Column('foo')).add(Column('bar')),
        [
            Row(('foo_value1', 'bar_value1')),
            Row(('foo_value2', 'bar_value2'))
        ],
        2,
        2
    ),
])
def test_result_set_eq(cols, rows, n_rows, n_cols):
    rs = ResultSet(cols, rows)
    assert rs.n_rows == n_rows
    assert rs.n_cols == n_cols


def test_result_set_iterable():
    rs = ResultSet(ColumnSet().add(Column('foo')).add(Column('bar')),
                   [
                       Row(('foo_value1', 'bar_value1')),
                       Row(('foo_value2', 'bar_value2'))
                   ])

    assert rs.next() == Row(('foo_value1', 'bar_value1'))
    assert rs.next() == Row(('foo_value2', 'bar_value2'))
    with pytest.raises(StopIteration):
        rs.next()

    assert rs.next() == Row(('foo_value1', 'bar_value1'))
    assert rs.next() == Row(('foo_value2', 'bar_value2'))
    with pytest.raises(StopIteration):
        rs.next()

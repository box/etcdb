import pytest

from etcdb.resultset import ResultSet, ColumnSet, Column


@pytest.mark.parametrize('query, args, query_out', [(
    'select 1',
    None,
    'select 1'
    ),
    (
        'select 1 where a = %s',
        (1, ),
        "select 1 where a = '1'"
    ),
    (
        'select 1 where a = %s and b = %s',
        (1, 'bbb'),
        "select 1 where a = '1' and b = 'bbb'"
    )
])
def test_morgify(cursor, query, args, query_out):
    assert cursor.morgify(query, args) == query_out


def test_n_rows(cursor):
    rs = ResultSet(ColumnSet().add(Column('foo')))
    cursor._result_set = rs
    assert cursor.n_rows == 0


def test_n_rows_none(cursor):
    assert cursor.n_rows == 0


def test_fetchone_none(cursor):
    assert cursor.fetchone() is None

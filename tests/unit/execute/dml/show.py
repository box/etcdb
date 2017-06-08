import mock
import pytest
from pyetcd import EtcdResult

from etcdb.execute.dml.show import show_databases
from etcdb.resultset import ResultSet, ColumnSet, Column, Row


@pytest.mark.parametrize('content, rows', [
    (
        '{"action":"get","node":{"dir":true,"nodes":[{"key":"/foo","dir":true,"modifiedIndex":2102,"createdIndex":2102},{"key":"/bar","dir":true,"modifiedIndex":2152,"createdIndex":2152}]}}',
        [
            Row(('foo',)),
            Row(('bar',))
        ]
    ),
    (
        '{"action":"get","node":{"dir":true}}',
        []
    )
])
def test_show_databases(content, rows):

    response = mock.Mock()
    response.content = content
    etcd_response = EtcdResult(response)

    etcd_client = mock.Mock()
    etcd_client.read.return_value = etcd_response

    cols = ColumnSet()
    cols.add(Column('Database'))

    # print(cols)

    rs = ResultSet(cols, rows)
    # noinspection PyTypeChecker
    result = show_databases(etcd_client)

    print("Expected: \n%s" % rs)
    print("Actual: \n%s" % result)
    # noinspection PyTypeChecker
    assert result == rs

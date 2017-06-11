import mock
import pytest
from pyetcd import EtcdResult, EtcdKeyNotFound

from etcdb import ProgrammingError
from etcdb.execute.dml.show import show_databases, show_tables, desc_table
from etcdb.resultset import ResultSet, ColumnSet, Column, Row
from etcdb.sqlparser.sql_tree import SQLTree


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


@pytest.mark.parametrize('content, rows', [
    (
        '{"action":"get","node":{"key":"/foo","dir":true,"nodes":[{"key":"/foo/bar","dir":true,"modifiedIndex":2183,"createdIndex":2183}],"modifiedIndex":2155,"createdIndex":2155}}',
        [
            Row(('bar',))
        ]
    ),
    (
        '{"action":"get","node":{"key":"/foo","dir":true,"modifiedIndex":2155,"createdIndex":2155}}',
        []
    )
])
def test_show_tables(content, rows):

    response = mock.Mock()
    response.content = content
    etcd_response = EtcdResult(response)

    etcd_client = mock.Mock()
    etcd_client.read.return_value = etcd_response

    cols = ColumnSet()
    cols.add(Column('Tables_in_foo'))

    # print(cols)

    rs = ResultSet(cols, rows)
    tree = SQLTree()
    tree.db = 'foo'
    tree.options['full'] = False
    # noinspection PyTypeChecker
    result = show_tables(etcd_client, tree, tree.db)

    print("Expected: \n%s" % rs)
    print("Actual: \n%s" % result)
    # noinspection PyTypeChecker
    assert result == rs


def test_desc_table_raises():
    etcd_client = mock.Mock()
    etcd_client.read.side_effect = EtcdKeyNotFound
    tree = SQLTree()
    with pytest.raises(ProgrammingError):
        # noinspection PyTypeChecker
        desc_table(etcd_client, tree, 'foo')

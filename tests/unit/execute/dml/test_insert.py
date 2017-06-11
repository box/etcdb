import json

import mock
import pytest
from pyetcd import EtcdResult, EtcdKeyNotFound

from etcdb import IntegrityError
from etcdb.execute.dml.insert import insert, get_pk_field
from etcdb.resultset import Column
from etcdb.sqlparser.sql_tree import SQLTree


@mock.patch('etcdb.execute.dml.insert.get_pk_field')
def test_insert_duplicate_raises(mock_get_pk_field):
    mock_get_pk_field.return_value = Column('id')

    etcd_client = mock.Mock()
    # etcd_client.read.side_effect =
    tree = SQLTree()

    # {"query_type": "INSERT",
    # "db": null,
    # "query":
    # "insert into bar(id, name) values(1, 'aaa')",
    # "table": "bar",
    # "expressions": [],
    # "success": true,
    # "fields": {"id": "1", "name": "aaa"},
    # "order": {"direction": "ASC", "by": null},
    # "limit": null,
    # "where": null,
    # "options": {}}
    tree.table = 'bar'
    tree.fields = {"id": "1", "name": "aaa"}

    with pytest.raises(IntegrityError):
        # noinspection PyTypeChecker
        insert(etcd_client, tree, 'foo')


@mock.patch('etcdb.execute.dml.insert.get_pk_field')
def test_insert(mock_get_pk_field):
    mock_get_pk_field.return_value = Column('id')

    etcd_client = mock.Mock()
    etcd_client.read.side_effect = EtcdKeyNotFound
    tree = SQLTree()
    tree.table = 'bar'
    tree.fields = {"id": "1", "name": "aaa"}
    # noinspection PyTypeChecker
    insert(etcd_client, tree, 'foo')
    etcd_client.write.assert_called_once_with('/foo/bar/1',
                                              json.dumps(tree.fields))


def test_get_pk_field():
    mock_response = mock.Mock()
    content = {
        "action": "get",
        "node": {
            "key": "/foo/bar/_fields",
            "value": '{"id": {"type": "INT", '
                     '"options": {"primary": true, "nullable": false}}, '
                     '"name": {"type": "VARCHAR", '
                     '"options": {"nullable": true}}}',
            "modifiedIndex": 2218,
            "createdIndex": 2218
        }
    }
    mock_response.content = json.dumps(content)
    etcd_response = EtcdResult(mock_response)

    etcd_client = mock.Mock()
    etcd_client.read.return_value = etcd_response
    assert get_pk_field(etcd_client, 'foo', 'bar') == Column('id')

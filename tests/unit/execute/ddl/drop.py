import mock
import pytest
from pyetcd import EtcdKeyNotFound

from etcdb import OperationalError
from etcdb.execute.ddl.drop import drop_database
from etcdb.sqlparser.sql_tree import SQLTree


def test_drop_database():
    client = mock.Mock()
    tree = SQLTree()
    tree.db = 'foo'
    drop_database(client, tree)
    client.rmdir.assert_called_once_with('/foo', recursive=True)


def test_drop_database_raises_operational_error():
    client = mock.Mock()
    client.rmdir.side_effect = EtcdKeyNotFound
    tree = SQLTree()
    tree.db = 'foo'
    with pytest.raises(OperationalError):
        drop_database(client, tree)

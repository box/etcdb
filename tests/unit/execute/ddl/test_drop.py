import mock
import pytest
from pyetcd import EtcdKeyNotFound

from etcdb import OperationalError
from etcdb.execute.ddl.drop import drop_database, drop_table
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


def test_drop_table_raises_error():

    etcd_client = mock.Mock()

    # Database is not selected
    with pytest.raises(OperationalError):
        drop_table(etcd_client, SQLTree())


def test_drop_table_raises_on_unknown_db():
    tree = SQLTree()
    tree.db = 'foo'
    tree.table = 'bar'

    etcd_client = mock.Mock()
    etcd_client.read.side_effect = EtcdKeyNotFound

    # unknown database
    with pytest.raises(OperationalError):
        drop_table(etcd_client, tree)


@pytest.mark.parametrize('db_sql_tree, db_arg', [
    (
        'foo',
        None,
    ),
    (
        None,
        'foo'
    )
])
def test_drop_table_calls_rmdir(db_sql_tree, db_arg):
    tree = SQLTree()
    tree.db = db_sql_tree
    tree.table = 'bar'

    etcd_client = mock.Mock()

    drop_table(etcd_client, tree, db=db_arg)

    etcd_client.rmdir.assert_called_once_with('/foo/bar', recursive=True)


def test_drop_table_if_exists():
    tree = SQLTree()
    tree.db = 'foo'
    tree.table = 'bar'
    tree.options['if_exists'] = True

    etcd_client = mock.Mock()
    etcd_client.rmdir.side_effect = EtcdKeyNotFound

    drop_table(etcd_client, tree, db='foo')

    etcd_client.rmdir.assert_called_once_with('/foo/bar', recursive=True)

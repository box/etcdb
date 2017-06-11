import json

import mock
import pytest
from pyetcd import EtcdNodeExist, EtcdKeyNotFound

from etcdb import ProgrammingError, OperationalError
from etcdb.execute.ddl.create import create_table
from etcdb.sqlparser.sql_tree import SQLTree


def test_create_table_raises_error():

    etcd_client = mock.Mock()

    # Database is not selected
    with pytest.raises(OperationalError):
        create_table(etcd_client, SQLTree())

    # primary key must be defined
    tree = SQLTree()
    tree.fields = {
                      u'id': {
                          u'options': {
                              u'nullable': True
                          },
                          u'type': u'INT'
                      }
                  }
    with pytest.raises(ProgrammingError):
        create_table(etcd_client, tree, db='foo')

    # primary key must be not NULL-able

    tree.fields = {
        u'id': {
            u'options': {
                u'primary': True,
                u'nullable': True
            },
            u'type': u'INT'
        }
    }
    with pytest.raises(ProgrammingError):
        create_table(etcd_client, tree, db='foo')


    # table exists

    etcd_client.mkdir.side_effect = EtcdNodeExist
    tree.fields = {
        u'id': {
            u'options': {
                u'primary': True,
                u'nullable': False
            },
            u'type': u'INT'
        }
    }
    with pytest.raises(OperationalError):
        create_table(etcd_client, tree, db='foo')


def test_create():
    etcd_client = mock.Mock()
    tree = SQLTree()
    tree.fields = {
        u'id': {
            u'options': {
                u'primary': True,
                u'nullable': False
            },
            u'type': u'INT'
        }
    }
    tree.table = 'bar'
    create_table(etcd_client, tree, db='foo')

    etcd_client.mkdir.assert_called_once_with('/foo/bar')
    etcd_client.write.assert_called_once_with('/foo/bar/_fields',
                                              json.dumps(tree.fields))


def test_create_raises_on_db_doesnt_exist():
    etcd_client = mock.Mock()
    etcd_client.read.side_effect = EtcdKeyNotFound
    tree = SQLTree()
    tree.fields = {
        u'id': {
            u'options': {
                u'primary': True,
                u'nullable': False
            },
            u'type': u'INT'
        }
    }
    tree.table = 'bar'
    with pytest.raises(OperationalError):
        create_table(etcd_client, tree, db='foo')

    etcd_client.read.assert_called_once_with('/foo')



    tree_values = {u'db': None,
                   u'expressions': [],
                   u'fields': {u'id': {u'options': {u'nullable': True}, u'type': u'INT'}},
                   u'limit': None,
                   u'options': {},
                   u'order': {u'by': None, u'direction': u'ASC'},
                   u'query': u'create table bar (id int)',
                   u'query_type': u'CREATE_TABLE',
                   u'success': True,
                   u'table': u'bar',
                   u'where': None}

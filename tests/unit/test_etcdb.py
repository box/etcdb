#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json

import mock
import pytest
import time
from pyetcd import EtcdResult, EtcdNodeExist, EtcdKeyNotFound
from pyetcd.client import Client

from etcdb import NotSupportedError, Time, Timestamp, DateFromTicks, TimeFromTicks, TimestampFromTicks, Binary, Date, \
    _split_version, ProgrammingError, OperationalError, LOCK_WAIT_TIMEOUT
from etcdb.connection import Connection
from etcdb.cursor import Cursor, ColInfo
from etcdb.etcddate import EtcdDate
from etcdb.etcdstring import EtcdString
from etcdb.etcdtime import EtcdTime
from etcdb.etcdtimestamp import EtcdTimestamp
from etcdb.sqlparser.sql_tree import SQLTree


def test_EtcdDate():
    d = EtcdDate(2016, 9, 11)
    assert d.year == 2016
    assert d.month == 9
    assert d.day == 11


def test_EtcdTime():
    t = EtcdTime(15, 51, 10)
    assert t.hour == 15
    assert t.minute == 51
    assert t.second == 10


def test_EtcdTimestamp():
    ts = EtcdTimestamp(2016, 9, 11, 15, 51, 10)
    assert ts.year == 2016
    assert ts.month == 9
    assert ts.day == 11
    assert ts.hour == 15
    assert ts.minute == 51
    assert ts.second == 10


def test_EtcdString():
    s = EtcdString('foo')
    assert s._string == 'foo'


def test_Date():
    d = Date(2016, 9, 11)
    assert isinstance(d, EtcdDate)
    assert d.year == 2016
    assert d.month == 9
    assert d.day == 11


def test_Time():
    t = Time(15, 53, 10)
    assert isinstance(t, EtcdTime)
    assert t.hour == 15
    assert t.minute == 53
    assert t.second == 10


def test_Timestamp():
    ts = Timestamp(2016, 9, 11, 15, 51, 10)
    assert isinstance(ts, EtcdTimestamp)
    assert ts.year == 2016
    assert ts.month == 9
    assert ts.day == 11
    assert ts.hour == 15
    assert ts.minute == 51
    assert ts.second == 10


def test_DateFromTicks(t_2016_9_21_23_10_3):
    d = DateFromTicks(t_2016_9_21_23_10_3)
    assert d.year == time.localtime(t_2016_9_21_23_10_3).tm_year
    assert d.month == time.localtime(t_2016_9_21_23_10_3).tm_mon
    assert d.day == time.localtime(t_2016_9_21_23_10_3).tm_mday


def test_TimeFromTicks(t_2016_9_21_23_10_3):
    t = TimeFromTicks(t_2016_9_21_23_10_3)
    assert t.hour == time.localtime(t_2016_9_21_23_10_3).tm_hour
    assert t.minute == time.localtime(t_2016_9_21_23_10_3).tm_min
    assert t.second == time.localtime(t_2016_9_21_23_10_3).tm_sec


def test_TimestampFromTicks(t_2016_9_21_23_10_3):
    d = TimestampFromTicks(t_2016_9_21_23_10_3)
    assert d.year == time.localtime(t_2016_9_21_23_10_3).tm_year
    assert d.month == time.localtime(t_2016_9_21_23_10_3).tm_mon
    assert d.day == time.localtime(t_2016_9_21_23_10_3).tm_mday
    assert d.hour == time.localtime(t_2016_9_21_23_10_3).tm_hour
    assert d.minute == time.localtime(t_2016_9_21_23_10_3).tm_min
    assert d.second == time.localtime(t_2016_9_21_23_10_3).tm_sec


def test_Binary():
    s = Binary('foo')
    assert isinstance(s, EtcdString)


def test_connection(etcdb_connection):
    assert isinstance(etcdb_connection._client, Client)


def test_connection_commit(etcdb_connection):
    with pytest.raises(NotSupportedError):
        etcdb_connection.commit()


def test_connection_rollback(etcdb_connection):
    with pytest.raises(NotSupportedError):
        etcdb_connection.rollback()


def test_connection_cursor(etcdb_connection):
    assert isinstance(etcdb_connection.cursor(), Cursor)


@mock.patch.object(Client, 'version')
def test_cursor_execute(mock_client, etcdb_connection):
    cursor = etcdb_connection.cursor()
    assert cursor.execute('SELECT VERSION()') == 1


def test_split_version():
    assert _split_version('a.b.c') == ('a', 'b', 'c')


@mock.patch.object(Client, 'version')
def test_cursor_fetchone(mock_client, etcdb_connection):
    mock_client.return_value = '2.3.7'
    cursor = etcdb_connection.cursor()
    cursor.execute('SELECT VERSION()')
    assert cursor.fetchone() == ('2.3.7', )


@pytest.mark.parametrize('kwargs,allowed_keys,kwargs_sanitized', [
    (
        {
            'host': '10.10.10.10',
            'port': 8888,
            'foo': 'bar'
        },
        ['host', 'port'],
        {'host': '10.10.10.10', 'port': 8888}
    )
])
def test_sanitize_kwargs(kwargs, allowed_keys, kwargs_sanitized):
    assert Connection._santize_pyetcd_kwargs(kwargs, allowed_keys) == kwargs_sanitized


@mock.patch.object(Client, 'mkdir')
def test_create_database(mock_client, etcdb_connection):
    cursor = etcdb_connection.cursor()
    cursor._execute_create_database('foo')
    mock_client.assert_called_once_with('/foo')


@pytest.mark.parametrize('db, payload,result', [
    (
        'foo',
        """
        {
            "action": "get",
            "node": {
                "createdIndex": 7,
                "dir": true,
                "key": "/foo",
                "modifiedIndex": 7,
                "nodes": [
                    {
                        "createdIndex": 8,
                        "dir": true,
                        "key": "/foo/tbl",
                        "modifiedIndex": 8
                    },
                    {
                        "createdIndex": 9,
                        "dir": true,
                        "key": "/foo/tbl1",
                        "modifiedIndex": 9
                    },
                    {
                        "createdIndex": 10,
                        "dir": true,
                        "key": "/foo/tbl2",
                        "modifiedIndex": 10
                    }
                ]
            }
        }
        """,
        (('Table',), (('tbl',), ('tbl1',), ('tbl2',)))
    ),
    (
        'test',
        """
        {
            "action": "get",
            "node": {
                "createdIndex": 7,
                "dir": true,
                "key": "/test",
                "modifiedIndex": 7,
                "nodes": [
                    {
                        "createdIndex": 8,
                        "dir": true,
                        "key": "/test/tbl",
                        "modifiedIndex": 8
                    },
                    {
                        "createdIndex": 9,
                        "dir": true,
                        "key": "/test/django_migrations",
                        "modifiedIndex": 9
                    },
                    {
                        "createdIndex": 10,
                        "dir": true,
                        "key": "/test/someverylongname",
                        "modifiedIndex": 10
                    }
                ]
            }
        }
        """,
        (('Table',), (('tbl',), ('django_migrations',), ('someverylongname',)))
    )
])
@mock.patch.object(Client, 'read')
def test_show_tables(mock_client, db, payload, result, cursor):
    response = mock.MagicMock()
    response.content = payload
    etcd_result = EtcdResult(response)
    tree = SQLTree()
    tree.db = db
    tree.options['full'] = False
    mock_client.return_value = etcd_result
    assert cursor._execute_show_tables(tree) == result
    mock_client.assert_called_once_with('/%s' % db)


@pytest.mark.parametrize('payload,result', [
    ("""
    {
        "action": "get",
        "node": {
            "createdIndex": 7,
            "dir": true,
            "key": "/foo",
            "modifiedIndex": 7,
            "nodes": [
                {
                    "createdIndex": 8,
                    "dir": true,
                    "key": "/foo/tbl",
                    "modifiedIndex": 8
                },
                {
                    "createdIndex": 9,
                    "dir": true,
                    "key": "/foo/tbl1",
                    "modifiedIndex": 9
                },
                {
                    "createdIndex": 10,
                    "dir": true,
                    "key": "/foo/tbl2",
                    "modifiedIndex": 10
                }
            ]
        }
    }
    """,
     (('Table', 'Type'), (('tbl', 'BASE TABLE'), ('tbl1', 'BASE TABLE'), ('tbl2', 'BASE TABLE')))
     ),
    (
        """{
        "action": "get",
        "node": {
            "createdIndex": 7,
            "dir": true,
            "key": "/foo",
            "modifiedIndex": 7
        }
    }""",
        (('Table', 'Type'), ())
    )
])
@mock.patch.object(Client, 'read')
def test_show_full_tables(mock_client, payload, result, etcdb_connection):
    cursor = etcdb_connection.cursor()
    assert cursor._db == 'foo'
    response = mock.MagicMock()
    response.content = payload
    etcd_result = EtcdResult(response)
    tree = SQLTree()
    tree.db = cursor._db
    tree.options['full'] = True
    mock_client.return_value = etcd_result
    assert cursor._execute_show_tables(tree) == result
    mock_client.assert_called_once_with('/foo')


@mock.patch.object(Client, 'read')
def test_show_tables_raises_exception_if_no_db(mock_client, etcdb_connection):
    cursor = etcdb_connection.cursor()
    cursor._db = None
    tree = SQLTree()
    with pytest.raises(OperationalError):
        cursor._execute_show_tables(tree)


@mock.patch.object(Client, 'write')
@mock.patch.object(Client, 'mkdir')
def test_create_table(mock_mkdir, mock_write, cursor):
    tree = SQLTree()
    tree.db = 'foo'
    tree.table = 'bar'
    tree.fields = {
        'id': {
            'type': 'INT',
            'options': {
                'nullable': False,
                'primary': True
            }
        }
    }
    cursor._execute_create_table(tree)
    mock_mkdir.assert_called_once_with('/foo/bar')
    mock_write.assert_called_once_with('/foo/bar/_fields', json.dumps(tree.fields))


@pytest.mark.parametrize('rows,result', [
    ((('information_schema',), ('mysql',), ('performance_schema',), ('sys',), ('test',)),
     [
         ('information_schema',),
         ('mysql',),
         ('performance_schema',),
         ('sys',),
         ('test',),
         None,
         None]),
    ((),
     [None])
])
def test_fetch_one(rows, result, etcdb_connection):
    cursor = etcdb_connection.cursor()
    cursor._rows = rows

    for i in xrange(len(result)):
        assert cursor.fetchone() == result[i]


@pytest.mark.parametrize('rows,result', [
    ((('information_schema',), ('mysql',), ('performance_schema',), ('sys',), ('test',)),
     [
         (('information_schema',), ('mysql',), ('performance_schema',), ('sys',), ('test',)),
         ()]),
    ((),
     [
         (),
         ()
     ])
])
def test_fetch_all(rows, result, etcdb_connection):
    cursor = etcdb_connection.cursor()
    cursor._rows = rows

    for i in xrange(len(result)):
        assert cursor.fetchall() == result[i]


@pytest.mark.parametrize('payload,result', [
    ("""{"action":"get","node":{"dir":true}}""", (('Database', ), ())),
    (
        """
        {
            "action": "get",
            "node": {
                "dir": true,
                "nodes": [
                    {
                        "createdIndex": 19,
                        "dir": true,
                        "key": "/foo",
                        "modifiedIndex": 19
                    }
                ]
            }
        }
        """,
        (('Database', ), ((u'foo',),))
     ),
    (
        """
        {
            "action": "get",
            "node": {
                "dir": true,
                "nodes": [
                    {
                        "createdIndex": 20,
                        "dir": true,
                        "key": "/foo",
                        "modifiedIndex": 20
                    },
                    {
                        "createdIndex": 19,
                        "dir": true,
                        "key": "/bar",
                        "modifiedIndex": 19
                    }
                ]
            }
        }
        """,
        (('Database', ),  ((u'foo',), (u'bar',),))
    )
])
@mock.patch.object(Client, 'read')
def test_show_databases(mock_client, payload, result, cursor):
    response = mock.MagicMock()
    response.content = payload
    etcd_result = EtcdResult(response)
    mock_client.return_value = etcd_result
    assert cursor._execute_show_databases() == result


@mock.patch.object(Client, 'mkdir')
def test_dbl_createdb(mock_client, etcdb_connection):
    mock_client.side_effect = EtcdNodeExist
    cursor = etcdb_connection.cursor()

    with pytest.raises(ProgrammingError):
        cursor._execute_create_database('foo')


@mock.patch.object(Client, 'mkdir')
def test_dbl_createtbl(mock_client, etcdb_connection):
    mock_client.side_effect = EtcdNodeExist
    cursor = etcdb_connection.cursor()
    tree = SQLTree()

    with pytest.raises(ProgrammingError):
        cursor._execute_create_table(tree)


def test_use_db(etcdb_connection):
    cursor = etcdb_connection.cursor()
    cursor.execute('USE foo')
    assert cursor._db == 'foo'


@mock.patch.object(Client, 'mkdir')
def test_create_table_raises_exception_if_no_db(mock_client, etcdb_connection):
    cursor = etcdb_connection.cursor()
    cursor._db = None
    tree = SQLTree()
    with pytest.raises(OperationalError):
        cursor._execute_create_table(tree)


@pytest.mark.parametrize('rows,n,result', [
    (
        (('information_schema',), ('mysql',), ('performance_schema',), ('sys',), ('test',)),
        2,
        (('information_schema',), ('mysql',), )
    ),
    (
        (),
        2,
        ()
     )
])
def test_fetch_many(rows, n, result, etcdb_connection):
    cursor = etcdb_connection.cursor()
    cursor._rows = rows

    assert cursor.fetchmany(n) == result


def test_select_variable(etcdb_connection):
    query = "SELECT @@SQL_MODE"
    cursor = etcdb_connection.cursor()
    cursor.execute(query)
    print(cursor._rows)
    assert cursor.fetchone() == ('STRICT_ALL_TABLES', )


def test_commit(cursor):
    # Commit does nothing
    cursor.execute("COMMIT")


@mock.patch.object(Client, 'rmdir')
def test_drop_database(mock_client, cursor):
    cursor.execute("DROP DATABASE foo")
    mock_client.assert_called_once_with('/foo', recursive=True)


def test_syntax_error_raises_exception(cursor):
    with pytest.raises(ProgrammingError):
        cursor.execute('foo')


@mock.patch.object(Client, 'read')
def test_desc_table(mock_client, cursor):
    etcd_result = mock.Mock()
    etcd_result.node = {
        'key': '/foo/bar/_fields',
        'value': '{"id": {"type": "INT", "options": {"nullable": true}}}'
    }
    mock_client.return_value = etcd_result
    cursor._db = 'foo'
    cursor.execute('DESC bar')
    mock_client.assert_called_once_with('/foo/bar/_fields')
    assert cursor.fetchone() == ('id', 'INT', 'YES', '', '', '', )


@mock.patch.object(Client, 'read')
def test_desc_table_raises_no_db(mock_client):
    conn = Connection()
    c = conn.cursor()

    with pytest.raises(OperationalError):
        c.execute('DESC bar')


@mock.patch.object(Client, 'read')
def test_desc_nonexisting_table_raises_error(mock_read, cursor):
    mock_read.side_effect = EtcdKeyNotFound
    with pytest.raises(ProgrammingError):
        cursor.execute('desc foo')


def test_create_table_must_define_pk(cursor):
    query = "CREATE TABLE t(id int)"
    with pytest.raises(ProgrammingError):
        cursor.execute(query)


def test_create_table_pk_must_be_not_null(cursor):
    query = "CREATE TABLE t(id int primary key)"
    with pytest.raises(ProgrammingError):
        cursor.execute(query)


@mock.patch.object(Client, 'read')
def test_get_pk(mock_read, cursor):
    etcd_result = mock.Mock()
    etcd_result.node = {
        'key': '/foo/bar/_fields',
        'value': '{"id": {"type": "INT", "options": {"nullable": false, "primary": true}}}'
    }
    mock_read.return_value = etcd_result
    assert cursor._get_pk('foo', 'bar') == {
        'id': {
            'type': 'INT',
            'options': {
                'nullable': False,
                'primary': True
            }
        }
    }


@mock.patch.object(Client, 'read')
def test_get_pk_name(mock_read, cursor):
    etcd_result = mock.Mock()
    etcd_result.node = {
        'key': '/foo/bar/_fields',
        'value': '{"id": {"type": "INT", "options": {"nullable": false, "primary": true}}}'
    }
    mock_read.return_value = etcd_result
    assert cursor._get_pk_name('foo', 'bar') == 'id'


@mock.patch.object(Client, 'write')
@mock.patch.object(Cursor, '_get_write_lock')
@mock.patch.object(Cursor, '_release_write_lock')
@mock.patch.object(Cursor, '_get_table_fields')
@mock.patch.object(Cursor, '_set_next_auto_inc')
def test_insert_table(mock_set_next_auto_inc,
                      mock_get_table_fields,
                      mock_release_write_lock,
                      mock_get_write_lock,
                      mock_write, cursor):
    mock_get_table_fields.return_value = json.loads("""
    {
        "id": {
            "options": {
                "auto_increment": true,
                "nullable": false,
                "primary": true
            },
            "type": "INT"
        },
        "name": {
            "options": {
                "nullable": true
            },
            "type": "VARCHAR"
        }
    }""")
    cursor._get_pk_name = mock.MagicMock()
    cursor._get_pk_name.return_value = 'id'

    cursor.execute("INSERT INTO t3 (id, name) VALUES(1, 'foo')")

    mock_write.assert_called_once_with('/foo/t3/1', '{"id": "1", "name": "foo"}')
    mock_get_write_lock.assert_called_once_with('foo', 't3')
    mock_release_write_lock.assert_called_once_with('foo', 't3')


@mock.patch.object(Client, 'write')
@mock.patch.object(Cursor, '_get_write_lock')
@mock.patch.object(Cursor, '_release_write_lock')
@mock.patch.object(Cursor, '_get_table_fields')
@mock.patch.object(Cursor, '_get_next_auto_inc')
@mock.patch.object(Cursor, '_set_next_auto_inc')
def test_insert_table_auto_incremented(mock_set_next_auto_inc,
                                       mock_get_next_auto_inc,
                                       mock_get_table_fields,
                                       mock_release_write_lock,
                                       mock_get_write_lock,
                                       mock_write, cursor):
    mock_get_table_fields.return_value = json.loads('{"id": {"type": "INT", "options": {"auto_increment": true, "primary": true, "nullable": false}}, "name": {"type": "VARCHAR", "options": {"nullable": true}}}')
    mock_get_next_auto_inc.return_value = 10
    cursor._get_pk_name = mock.MagicMock()
    cursor._get_pk_name.return_value = 'id'

    cursor.execute("INSERT INTO t3 (name) VALUES('foo')")

    mock_write.assert_called_once_with('/foo/t3/10', '{"id": "10", "name": "foo"}')
    # lock
    mock_get_write_lock.assert_called_once_with('foo', 't3')
    mock_release_write_lock.assert_called_once_with('foo', 't3')


@pytest.mark.parametrize('db,tbl,pk,payload,result', [
    (
        'foo', 'bar',
        {
            'id': {
                'type': 'INT',
                'options': {
                    'nullable': False,
                    'primary': True
                }
            }
        },
        """
        {
                "action": "get",
                "node": {
                    "dir": true,
                    "nodes": [
                        {
                            "createdIndex": 20,
                            "dir": true,
                            "key": "/foo/bar/1",
                            "modifiedIndex": 20
                        },
                        {
                            "createdIndex": 19,
                            "dir": true,
                            "key": "/foo/bar/2",
                            "modifiedIndex": 19
                        },
                        {
                            "createdIndex": 19,
                            "dir": true,
                            "key": "/foo/bar/3",
                            "modifiedIndex": 19
                        }
                    ]
                }
            }
        """,
        [1, 2, 3]
    ),
    (
        'foo', 'bar',
        {
            'id': {
                'type': 'INT',
                'options': {
                    'nullable': False,
                    'primary': True
                }
            }
        },
        """
        {
                "action": "get",
                "node": {
                    "dir": true
                }
            }
        """,
        []
    ),
    (
        'a', 't1',
        {
            'id': {
                'type': 'INT',
                'options': {
                    'nullable': False,
                    'primary': True
                }
            }
        },
        """
        {
            "action": "get",
            "node": {
                "createdIndex": 37,
                "dir": true,
                "key": "/a/t1",
                "modifiedIndex": 37,
                "nodes": [
                    {
                        "createdIndex": 39,
                        "key": "/a/t1/1",
                        "modifiedIndex": 39,
                        "value": "{\\"id\\": \\"1\\"}"
                    },
                    {
                        "createdIndex": 40,
                        "key": "/a/t1/2",
                        "modifiedIndex": 40,
                        "value": "{\\"id\\": \\"2\\"}"
                    },
                    {
                        "createdIndex": 41,
                        "key": "/a/t1/10",
                        "modifiedIndex": 41,
                        "value": "{\\"id\\": \\"10\\"}"
                    }
                ]
            }
        }
        """,
        [1, 2, 10]
    ),
    (
        'a', 't1',
        {
            'id': {
                'type': 'VARCHAR',
                'options': {
                    'nullable': False,
                    'primary': True
                }
            }
        },
        """
        {
            "action": "get",
            "node": {
                "createdIndex": 37,
                "dir": true,
                "key": "/a/t1",
                "modifiedIndex": 37,
                "nodes": [
                    {
                        "createdIndex": 39,
                        "key": "/a/t1/aaa",
                        "modifiedIndex": 39,
                        "value": "{\\"id\\": \\"aaa\\"}"
                    },
                    {
                        "createdIndex": 40,
                        "key": "/a/t1/ccc",
                        "modifiedIndex": 40,
                        "value": "{\\"id\\": \\"ccc\\"}"
                    },
                    {
                        "createdIndex": 41,
                        "key": "/a/t1/bbb",
                        "modifiedIndex": 41,
                        "value": "{\\"id\\": \\"bbb\\"}"
                    }
                ]
            }
        }
        """,
        ['aaa', 'bbb', 'ccc']
    )
])
@mock.patch.object(Client, 'read')
@mock.patch.object(Cursor, '_get_pk')
def test_get_pks_returns_pks(mock_get_pk, mock_read, db, tbl, pk, payload, result, cursor):
    response = mock.MagicMock()
    response.content = payload
    etcd_result = EtcdResult(response)
    mock_read.return_value = etcd_result
    mock_get_pk.return_value = pk
    assert cursor._get_pks(db, tbl) == result


@pytest.mark.parametrize('payload,result', [
    (
        '{"action":"get","node":{"key":"/a/t1/10","value":"{\\"id\\": \\"10\\"}","modifiedIndex":41,"createdIndex":41}}',
        ('10',)

    )
])
@mock.patch.object(Client, 'read')
@mock.patch.object(Cursor, '_get_read_lock')
@mock.patch.object(Cursor, '_release_read_lock')
def test_select_returns_records(mock_release_read_lock,
                                mock_get_read_lock,
                                mock_read, payload, result, cursor):
    cursor._get_pks = mock.Mock()
    cursor._get_pks.return_value = ['10']

    response = mock.MagicMock()
    response.content = payload
    etcd_result = EtcdResult(response)
    mock_read.return_value = etcd_result

    cursor.execute("SELECT id from bar")
    assert cursor.fetchone() == result


@pytest.mark.parametrize('column_names,rows,new_columns', [
    (
        ('Table', 'Type'),
        (
            ('t1', 'BASIC TABLE'),
            ('t1t1t1t1t1', 'BASIC TABLE')
        ),
        (ColInfo(name='Table', width=10), ColInfo(name='Type', width=11)),
    ),
    (
        ('Table', 'Type'),
        (),
        (ColInfo(name='Table', width=5), ColInfo(name='Type', width=4))
    ),
    (
        ('Table', 'Type'),
        None,
        (ColInfo(name='Table', width=5), ColInfo(name='Type', width=4))
    ),
    (
        ('Table', 'Type'),
        (
            ('t1', 'foo'),
            ('t1', 'bar')
        ),
        (ColInfo(name='Table', width=5), ColInfo(name='Type', width=4))
    )
])
def test_update_columns(cursor, column_names, rows, new_columns):
    columns = cursor._update_columns(column_names, rows)
    print(new_columns)

    for i in xrange(len(column_names)):
        assert columns[i].width == new_columns[i].width
        assert columns[i].name == new_columns[i].name


@pytest.mark.parametrize('payload,result', [
    (
        """
        {"action": "get", "node": {"createdIndex": 70, "modifiedIndex": 70, "value": "{\\"applied\\": \\"aaa\\", \\"app\\": \\"bbb\\", \\"id\\": \\"1\\", \\"name\\": \\"ccc\\"}", "key": "/test/django_migrations/1"}}
        """,
        (('bbb', 'ccc'), )

    )
])
@mock.patch.object(Client, 'read')
@mock.patch.object(Cursor, '_get_read_lock')
@mock.patch.object(Cursor, '_release_read_lock')
def test_select_from_django_migrations(mock_get_read_lock, mock_release_read_lock, mock_read, payload, result, cursor):
    cursor._get_pks = mock.Mock()
    cursor._get_pks.return_value = ['10']

    response = mock.MagicMock()
    response.content = payload
    etcd_result = EtcdResult(response)
    mock_read.return_value = etcd_result

    query = "SELECT `django_migrations`.`app`, `django_migrations`.`name` FROM `django_migrations`"
    cursor.execute(query)
    assert cursor._rows == result


@mock.patch('etcdb.cursor.time.time')
def test_get_lock_raises_lock_wait_timeout(mock_time, cursor):
    mock_time.side_effect = [1475852136, 1475852136 + LOCK_WAIT_TIMEOUT]
    with pytest.raises(OperationalError):
        cursor._get_meta_lock('foo', 'bar')


@mock.patch.object(Client, 'update_ttl')
@mock.patch.object(Client, 'compare_and_swap')
def test_get_meta_lock(mock_cas, mock_update_ttl, cursor):

    payload = '{"action":"set","node":{"key":"/foo/bar/_lock_meta","value":"","modifiedIndex":625,"createdIndex":625}}'

    response = mock.MagicMock()
    response.content = payload
    etcd_result = EtcdResult(response)
    mock_cas.return_value = etcd_result
    mock_update_ttl.side_effect = EtcdKeyNotFound

    response = cursor._get_meta_lock('foo', 'bar')

    mock_cas.assert_called_once_with('/foo/bar/_lock_meta',
                                     '',
                                     ttl=1,
                                     prev_exist=False)
    assert response.node['key'] == '/foo/bar/_lock_meta'
    assert response.node['value'] == ''


@mock.patch.object(Cursor, '_get_meta_lock')
@mock.patch.object(Cursor, '_release_meta_lock')
@mock.patch.object(Cursor, '_write_lock_set')
@mock.patch.object(Cursor, '_get_active_read_locks')
@mock.patch.object(Client, 'write')
@mock.patch('etcdb.cursor.uuid')
def test_get_read_lock(mock_uuid,
                       mock_write,
                       mock_get_active_read_locks,
                       mock_write_lock_set,
                       mock_release_meta_lock,
                       mock_get_meta_lock,
                       cursor):
    mock_uuid.uuid4.return_value = 'foo_id'
    mock_get_active_read_locks.return_value = []
    mock_write_lock_set.return_value = False

    cursor._get_read_lock('foo', 'bar')
    mock_write.assert_called_once_with('/foo/bar/_lock_read/foo_id', '', ttl=1)

    mock_get_meta_lock.assert_called_once_with('foo', 'bar')
    mock_release_meta_lock.assert_called_once_with('foo', 'bar')


@mock.patch.object(Cursor, '_get_meta_lock')
@mock.patch.object(Cursor, '_release_meta_lock')
@mock.patch.object(Client, 'read')
@mock.patch.object(Client, 'write')
@mock.patch('etcdb.cursor.uuid')
def test_get_read_lock_lock_free(mock_uuid, mock_write, mock_read,
                                 mock_release_meta_lock,
                                 mock_get_meta_lock,
                                 cursor):
    mock_read.side_effect = EtcdKeyNotFound
    mock_uuid.uuid4.return_value = 'lock_id'
    assert cursor._get_read_lock('foo', 'bar') == 'lock_id'
    mock_write.assert_called_once_with('/foo/bar/_lock_read/lock_id', '', ttl=cursor._timeout)


@mock.patch.object(Client, 'read')
def test_write_lock_set_false(mock_read, cursor):
    mock_read.side_effect = EtcdKeyNotFound
    assert not cursor._write_lock_set('foo', 'bar')
    mock_read.assert_called_once_with('/foo/bar/_lock_write')


@mock.patch.object(Client, 'read')
def test_write_lock_set_true(mock_read, cursor):
    assert cursor._write_lock_set('foo', 'bar')
    mock_read.assert_called_once_with('/foo/bar/_lock_write')


@mock.patch.object(Client, 'read')
def test_wait_until_write_lock_deleted(mock_read, cursor):
    mock_read.side_effect = [
        mock.MagicMock(),
        EtcdKeyNotFound
    ]
    cursor._wait_until_write_lock_deleted('foo', 'bar')
    mock_read.assert_called_with('/foo/bar/_lock_write', wait=True)
    assert mock_read.call_count == 2


@pytest.mark.parametrize('payload,active_read_locks', [
    (
        """
        {
            "action": "get",
            "node": {
                "createdIndex": 26468,
                "dir": true,
                "key": "/foo/t1/_lock_read",
                "modifiedIndex": 26468,
                "nodes": [
                    {
                        "createdIndex": 26468,
                        "key": "/foo/bar/_lock_read/xxx",
                        "modifiedIndex": 26468,
                        "value": ""
                    }
                ]
            }
        }
        """,
        ['xxx']
    ),
    (
        '{"action":"get","node":{"key":"/foo/t1/_lock_read","dir":true,"modifiedIndex":26471,"createdIndex":26471}}',
        []
    ),
    (
        """
        {
            "action": "get",
            "node": {
                "createdIndex": 26471,
                "dir": true,
                "key": "/foo/bar/_lock_read",
                "modifiedIndex": 26471,
                "nodes": [
                    {
                        "createdIndex": 133081,
                        "key": "/foo/bar/_lock_read/lock1",
                        "modifiedIndex": 133081,
                        "value": ""
                    },
                    {
                        "createdIndex": 133518,
                        "key": "/foo/bar/_lock_read/lock2",
                        "modifiedIndex": 133518,
                        "value": ""
                    }
                ]
            }
        }
        """,
        ['lock1', 'lock2']
    )
])
@mock.patch.object(Client, 'read')
def test_get_active_read_locks(mock_read, payload, active_read_locks, cursor):
    response = mock.MagicMock()
    response.content = payload
    etcd_result = EtcdResult(response)
    mock_read.return_value = etcd_result

    assert cursor._get_active_read_locks('foo', 'bar') == active_read_locks


@mock.patch.object(Client, 'read')
def test_get_active_read_locks_empty(mock_read, cursor):
    mock_read.side_effect = KeyError
    assert cursor._get_active_read_locks('foo', 'bar') == []


@mock.patch.object(Cursor, '_get_active_read_locks')
@mock.patch.object(Client, 'delete')
def test_release_read_lock(mock_delete,
                           mock_get_active_read_locks,
                           cursor):
    mock_get_active_read_locks.return_value = ['foo_id']
    cursor._release_read_lock('foo', 'bar', 'foo_id')
    mock_delete.assert_called_once_with('/foo/bar/_lock_read/foo_id')


@mock.patch.object(Cursor, '_get_meta_lock')
@mock.patch.object(Cursor, '_release_meta_lock')
@mock.patch.object(Cursor, '_write_lock_set')
@mock.patch.object(Cursor, '_ensure_no_read_lock')
@mock.patch.object(Client, 'write')
def test_get_write_lock(mock_write,
                        mock_ensure_no_read_lock,
                        mock_write_lock_set,
                        mock_release,
                        mock_get, cursor):
    mock_write_lock_set.return_value = False
    cursor._get_write_lock('foo', 'bar')
    mock_write.assert_called_once_with('/foo/bar/_lock_write', '', ttl=1)


@mock.patch.object(Cursor, '_get_active_read_locks')
def test_ensure_no_read_lock_no_active_reads(mock_get_active_read_locks, cursor):
    mock_get_active_read_locks.return_value = []
    cursor._ensure_no_read_lock('foo', 'bar')


@mock.patch.object(Cursor, '_get_active_read_locks')
@mock.patch.object(Cursor, '_wait_until_read_lock_released')
def test_ensure_no_read_lock_with_active_reads(mock_wait_until_read_lock_released,
                                               mock_get_active_read_locks, cursor):
    mock_get_active_read_locks.return_value = ['some lock']
    cursor._ensure_no_read_lock('foo', 'bar')
    mock_wait_until_read_lock_released.assert_called_once_with('foo', 'bar', 'some lock')


@mock.patch.object(Cursor, '_get_write_lock')
@mock.patch.object(Cursor, '_release_write_lock')
@mock.patch.object(Cursor, '_get_pk_name')
@mock.patch.object(Cursor, '_get_table_fields')
@mock.patch.object(Cursor, '_get_next_auto_inc')
@mock.patch.object(Client, 'write')
def test_lastrowid(mock_write,
                   mock_get_next_auto_inc,
                   mock_get_table_fields,
                   mock_get_pk_name,
                   mock_rwl, mock_gwl,
                   cursor):
    mock_get_next_auto_inc.return_value = 10
    mock_get_table_fields.return_value = json.loads("""
    {
        "id": {
            "options": {
                "auto_increment": true,
                "nullable": false,
                "primary": true
            },
            "type": "INT"
        },
        "name": {
            "options": {
                "nullable": true
            },
            "type": "VARCHAR"
        }
    }""")
    mock_get_pk_name.return_value = 'id'
    cursor.execute("INSERT INTO t1 (name) VALUES ('foo')")
    assert cursor.lastrowid == 10


@pytest.mark.parametrize('payload,rowcount', [
    (
        """
        {
            "action": "get",
            "node": {
                "createdIndex": 303785,
                "dir": true,
                "key": "/foo/bar",
                "modifiedIndex": 303785,
                "nodes": [
                    {
                        "createdIndex": 303794,
                        "key": "/foo/bar/1",
                        "modifiedIndex": 303794,
                        "value": "{\\"id\\": \\"1\\"}"
                    },
                    {
                        "createdIndex": 303807,
                        "key": "/foo/bar/2",
                        "modifiedIndex": 303807,
                        "value": "{\\"id\\": \\"2\\"}"
                    },
                    {
                        "createdIndex": 303819,
                        "key": "/foo/bar/3",
                        "modifiedIndex": 303819,
                        "value": "{\\"id\\": \\"3\\"}"
                    }
                ]
            }
        }
        """,
        3
    ),
    (
        """
        {
            "action": "get",
            "node": {
                "createdIndex": 303785,
                "dir": true,
                "key": "/foo/bar",
                "modifiedIndex": 303785
            }
        }
        """,
        0
    )
])
@mock.patch.object(Cursor, '_get_read_lock')
@mock.patch.object(Cursor, '_release_read_lock')
@mock.patch.object(Cursor, '_get_pks')
@mock.patch.object(Client, 'read')
def test_count_star(mock_read,
                    mock_get_pks,
                    mock_release_read_lock,
                    mock_get_read_lock,
                    payload, rowcount,
                    cursor):
    response = mock.MagicMock()
    response.content = payload
    etcd_result = EtcdResult(response)
    mock_read.return_value = etcd_result

    mock_get_pks.return_value = []
    query = "SELECT COUNT(*) AS `__count` FROM `foo_config`"
    assert cursor.execute(query) == 1
    assert cursor.fetchone()[0] == rowcount
    mock_read.assert_called_once_with('/foo/foo_config')


@pytest.mark.parametrize('payload,limit,result', [
    (
        """
        {
                "action": "get",
                "node": {
                    "dir": true,
                    "nodes": [
                        {
                            "createdIndex": 20,
                            "dir": true,
                            "key": "/foo/bar/1",
                            "modifiedIndex": 20
                        },
                        {
                            "createdIndex": 19,
                            "dir": true,
                            "key": "/foo/bar/2",
                            "modifiedIndex": 19
                        },
                        {
                            "createdIndex": 19,
                            "dir": true,
                            "key": "/foo/bar/3",
                            "modifiedIndex": 19
                        }
                    ]
                }
            }
        """,
        2,
        [1, 2, 3]
    ),
    (
        """
        {
                "action": "get",
                "node": {
                    "dir": true
                }
            }
        """,
        1,
        []
    ),
    (
        """
        {
            "action": "get",
            "node": {
                "createdIndex": 37,
                "dir": true,
                "key": "/foo/bar",
                "modifiedIndex": 37,
                "nodes": [
                    {
                        "createdIndex": 39,
                        "key": "/foo/bar/1",
                        "modifiedIndex": 39,
                        "value": "{\\"id\\": \\"1\\"}"
                    },
                    {
                        "createdIndex": 40,
                        "key": "/foo/bar/10",
                        "modifiedIndex": 40,
                        "value": "{\\"id\\": \\"10\\"}"
                    },
                    {
                        "createdIndex": 41,
                        "key": "/foo/bar/20",
                        "modifiedIndex": 41,
                        "value": "{\\"id\\": \\"20\\"}"
                    }
                ]
            }
        }
        """,
        2,
        [1, 10, 20]
    )
])
@mock.patch.object(Client, 'read')
@mock.patch.object(Cursor, '_get_pk')
def test_get_pks_with_limit(mock_get_pk, mock_read, payload, limit, result, cursor):
    response = mock.MagicMock()
    response.content = payload
    etcd_result = EtcdResult(response)
    mock_read.return_value = etcd_result
    tree = SQLTree()
    tree.limit = limit

    mock_get_pk.return_value = {
        'id': {
            'type': 'INT',
            'options': {
                'nullable': False,
                'primary': True
            }
        }
    }
    assert cursor._get_pks('foo', 'bar', tree=tree) == result


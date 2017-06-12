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
from etcdb.eval_expr import eval_identifier
from etcdb.resultset import ResultSet, ColumnSet, Row, Column
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


def test_split_version():
    assert _split_version('a.b.c') == ('a', 'b', 'c')


def test_cursor_fetchone(etcdb_connection):
    cursor = etcdb_connection.cursor()

    columns = ColumnSet().add(Column('VERSION()'))
    rs = ResultSet(columns)
    rs.add_row(Row(('2.3.7', )))
    cursor._result_set = rs

    row = cursor.fetchone()
    assert isinstance(row, tuple)
    assert row == ('2.3.7', )


@pytest.mark.parametrize('kwargs, kwargs_sanitized', [
    (
        {
            'host': '10.10.10.10',
            'port': 8888,
            'foo': 'bar'
        },
        {'host': '10.10.10.10', 'port': 8888}
    )
])
def test_sanitize_kwargs(kwargs, kwargs_sanitized):
    assert Connection._santize_pyetcd_kwargs(kwargs) == kwargs_sanitized


@pytest.mark.parametrize('query, db, payload, result', [
    (
        "SHOW TABLES",
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
        (
            ('tbl',), ('tbl1',), ('tbl2',)
        )
    ),
    (
        "SHOW TABLES",
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
        (
            ('tbl',), ('django_migrations',), ('someverylongname',)
        )
    ),
    (
        "SHOW FULL TABLES",
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
        }""",
        (('tbl', 'BASE TABLE'), ('tbl1', 'BASE TABLE'), ('tbl2', 'BASE TABLE'))

    )
])
@mock.patch.object(Client, 'read')
def test_show_tables(mock_client, query, db, payload, result, cursor):
    response = mock.Mock()
    response.content = payload
    etcd_result = EtcdResult(response)

    mock_client.return_value = etcd_result
    cursor.execute("USE %s" % db)
    cursor.execute(query)

    rows = ()
    while True:
        row = cursor.fetchone()
        if row:
            assert isinstance(row, tuple)
            rows += (row, )
        else:
            break
    assert result == rows


@pytest.mark.parametrize('rows, result', [
    (
        [
            Row(('information_schema',)),
            Row(('mysql',)),
            Row(('performance_schema',)),
            Row(('sys',)),
            Row(('test',))
        ],
        (
            ('information_schema',),
            ('mysql',),
            ('performance_schema',),
            ('sys',),
            ('test',)
        ),
    ),
    (
        [],
        ()
    )
])
def test_fetch_all(rows, result, etcdb_connection):
    cursor = etcdb_connection.cursor()
    rs = ResultSet(ColumnSet().add(Column('Tables')),
                   rows
                   )
    cursor._result_set = rs

    # for i in xrange(len(result)):
    assert cursor.fetchall() == result
    assert cursor.fetchall() == ()


@pytest.mark.parametrize('rows,n,result', [
    (
        [
            Row(('information_schema',)),
            Row(('mysql',)),
            Row(('performance_schema',)),
            Row(('sys',)),
            Row(('test',))
        ],
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
    rs = ResultSet(ColumnSet().add(Column('Tables')),
                   rows
                   )
    cursor._result_set = rs

    assert cursor.fetchmany(n) == result


# @mock.patch('etcdb.cursor.time.time')
# def test_get_lock_raises_lock_wait_timeout(mock_time, cursor):
#     mock_time.side_effect = [1475852136, 1475852136 + LOCK_WAIT_TIMEOUT]
#     with pytest.raises(OperationalError):
#         cursor._get_meta_lock('foo', 'bar')
#
#
# @mock.patch.object(Client, 'update_ttl')
# @mock.patch.object(Client, 'compare_and_swap')
# def test_get_meta_lock(mock_cas, mock_update_ttl, cursor):
#
#     payload = '{"action":"set","node":{"key":"/foo/bar/_lock_meta","value":"","modifiedIndex":625,"createdIndex":625}}'
#
#     response = mock.MagicMock()
#     response.content = payload
#     etcd_result = EtcdResult(response)
#     mock_cas.return_value = etcd_result
#     mock_update_ttl.side_effect = EtcdKeyNotFound
#
#     response = cursor._get_meta_lock('foo', 'bar')
#
#     mock_cas.assert_called_once_with('/foo/bar/_lock_meta',
#                                      '',
#                                      ttl=1,
#                                      prev_exist=False)
#     assert response.node['key'] == '/foo/bar/_lock_meta'
#     assert response.node['value'] == ''
#
#
# @mock.patch.object(Cursor, '_get_meta_lock')
# @mock.patch.object(Cursor, '_release_meta_lock')
# @mock.patch.object(Cursor, '_write_lock_set')
# @mock.patch.object(Cursor, '_get_active_read_locks')
# @mock.patch.object(Client, 'write')
# @mock.patch('etcdb.cursor.uuid')
# def test_get_read_lock(mock_uuid,
#                        mock_write,
#                        mock_get_active_read_locks,
#                        mock_write_lock_set,
#                        mock_release_meta_lock,
#                        mock_get_meta_lock,
#                        cursor):
#     mock_uuid.uuid4.return_value = 'foo_id'
#     mock_get_active_read_locks.return_value = []
#     mock_write_lock_set.return_value = False
#
#     cursor._get_read_lock('foo', 'bar')
#     mock_write.assert_called_once_with('/foo/bar/_lock_read/foo_id', '', ttl=1)
#
#     mock_get_meta_lock.assert_called_once_with('foo', 'bar')
#     mock_release_meta_lock.assert_called_once_with('foo', 'bar')
#
#
# @mock.patch.object(Cursor, '_get_meta_lock')
# @mock.patch.object(Cursor, '_release_meta_lock')
# @mock.patch.object(Client, 'read')
# @mock.patch.object(Client, 'write')
# @mock.patch('etcdb.cursor.uuid')
# def test_get_read_lock_lock_free(mock_uuid, mock_write, mock_read,
#                                  mock_release_meta_lock,
#                                  mock_get_meta_lock,
#                                  cursor):
#     mock_read.side_effect = EtcdKeyNotFound
#     mock_uuid.uuid4.return_value = 'lock_id'
#     assert cursor._get_read_lock('foo', 'bar') == 'lock_id'
#     mock_write.assert_called_once_with('/foo/bar/_lock_read/lock_id', '', ttl=cursor._timeout)
#
#
# @mock.patch.object(Client, 'read')
# def test_write_lock_set_false(mock_read, cursor):
#     mock_read.side_effect = EtcdKeyNotFound
#     assert not cursor._write_lock_set('foo', 'bar')
#     mock_read.assert_called_once_with('/foo/bar/_lock_write')
#
#
# @mock.patch.object(Client, 'read')
# def test_write_lock_set_true(mock_read, cursor):
#     assert cursor._write_lock_set('foo', 'bar')
#     mock_read.assert_called_once_with('/foo/bar/_lock_write')
#
#
# @mock.patch.object(Client, 'read')
# def test_wait_until_write_lock_deleted(mock_read, cursor):
#     mock_read.side_effect = [
#         mock.MagicMock(),
#         EtcdKeyNotFound
#     ]
#     cursor._wait_until_write_lock_deleted('foo', 'bar')
#     mock_read.assert_called_with('/foo/bar/_lock_write', wait=True)
#     assert mock_read.call_count == 2
#
#
# @pytest.mark.parametrize('payload,active_read_locks', [
#     (
#         """
#         {
#             "action": "get",
#             "node": {
#                 "createdIndex": 26468,
#                 "dir": true,
#                 "key": "/foo/t1/_lock_read",
#                 "modifiedIndex": 26468,
#                 "nodes": [
#                     {
#                         "createdIndex": 26468,
#                         "key": "/foo/bar/_lock_read/xxx",
#                         "modifiedIndex": 26468,
#                         "value": ""
#                     }
#                 ]
#             }
#         }
#         """,
#         ['xxx']
#     ),
#     (
#         '{"action":"get","node":{"key":"/foo/t1/_lock_read","dir":true,"modifiedIndex":26471,"createdIndex":26471}}',
#         []
#     ),
#     (
#         """
#         {
#             "action": "get",
#             "node": {
#                 "createdIndex": 26471,
#                 "dir": true,
#                 "key": "/foo/bar/_lock_read",
#                 "modifiedIndex": 26471,
#                 "nodes": [
#                     {
#                         "createdIndex": 133081,
#                         "key": "/foo/bar/_lock_read/lock1",
#                         "modifiedIndex": 133081,
#                         "value": ""
#                     },
#                     {
#                         "createdIndex": 133518,
#                         "key": "/foo/bar/_lock_read/lock2",
#                         "modifiedIndex": 133518,
#                         "value": ""
#                     }
#                 ]
#             }
#         }
#         """,
#         ['lock1', 'lock2']
#     )
# ])
# @mock.patch.object(Client, 'read')
# def test_get_active_read_locks(mock_read, payload, active_read_locks, cursor):
#     response = mock.MagicMock()
#     response.content = payload
#     etcd_result = EtcdResult(response)
#     mock_read.return_value = etcd_result
#
#     assert cursor._get_active_read_locks('foo', 'bar') == active_read_locks
#
#
# @mock.patch.object(Client, 'read')
# def test_get_active_read_locks_empty(mock_read, cursor):
#     mock_read.side_effect = KeyError
#     assert cursor._get_active_read_locks('foo', 'bar') == []
#
#
# @mock.patch.object(Cursor, '_get_active_read_locks')
# @mock.patch.object(Client, 'delete')
# def test_release_read_lock(mock_delete,
#                            mock_get_active_read_locks,
#                            cursor):
#     mock_get_active_read_locks.return_value = ['foo_id']
#     cursor._release_read_lock('foo', 'bar', 'foo_id')
#     mock_delete.assert_called_once_with('/foo/bar/_lock_read/foo_id')
#
#
# @mock.patch.object(Cursor, '_get_meta_lock')
# @mock.patch.object(Cursor, '_release_meta_lock')
# @mock.patch.object(Cursor, '_write_lock_set')
# @mock.patch.object(Cursor, '_ensure_no_read_lock')
# @mock.patch.object(Client, 'write')
# def test_get_write_lock(mock_write,
#                         mock_ensure_no_read_lock,
#                         mock_write_lock_set,
#                         mock_release,
#                         mock_get, cursor):
#     mock_write_lock_set.return_value = False
#     cursor._get_write_lock('foo', 'bar')
#     mock_write.assert_called_once_with('/foo/bar/_lock_write', '', ttl=1)
#
#
# @mock.patch.object(Cursor, '_get_active_read_locks')
# def test_ensure_no_read_lock_no_active_reads(mock_get_active_read_locks, cursor):
#     mock_get_active_read_locks.return_value = []
#     cursor._ensure_no_read_lock('foo', 'bar')
#
#
# @mock.patch.object(Cursor, '_get_active_read_locks')
# @mock.patch.object(Cursor, '_wait_until_read_lock_released')
# def test_ensure_no_read_lock_with_active_reads(mock_wait_until_read_lock_released,
#                                                mock_get_active_read_locks, cursor):
#     mock_get_active_read_locks.return_value = ['some lock']
#     cursor._ensure_no_read_lock('foo', 'bar')
#     mock_wait_until_read_lock_released.assert_called_once_with('foo', 'bar', 'some lock')
#
#
# @mock.patch.object(Cursor, '_get_write_lock')
# @mock.patch.object(Cursor, '_release_write_lock')
# @mock.patch.object(Cursor, '_get_pk_name')
# @mock.patch.object(Cursor, '_get_table_fields')
# @mock.patch.object(Cursor, '_get_next_auto_inc')
# @mock.patch.object(Client, 'write')
# def test_lastrowid(mock_write,
#                    mock_get_next_auto_inc,
#                    mock_get_table_fields,
#                    mock_get_pk_name,
#                    mock_rwl, mock_gwl,
#                    cursor):
#     mock_get_next_auto_inc.return_value = 10
#     mock_get_table_fields.return_value = json.loads("""
#     {
#         "id": {
#             "options": {
#                 "auto_increment": true,
#                 "nullable": false,
#                 "primary": true
#             },
#             "type": "INT"
#         },
#         "name": {
#             "options": {
#                 "nullable": true
#             },
#             "type": "VARCHAR"
#         }
#     }""")
#     mock_get_pk_name.return_value = 'id'
#     cursor.execute("INSERT INTO t1 (name) VALUES ('foo')")
#     assert cursor.lastrowid == 10

import pytest

from etcdb import OperationalError
from etcdb.lock import WriteLock


def test_update(cursor):
    cursor.execute('CREATE TABLE t1(id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255))')
    cursor.execute("INSERT INTO t1(id, name) VALUES (1, 'aaa')")
    cursor.execute("INSERT INTO t1(id, name) VALUES (2, 'bbb')")
    cursor.execute("INSERT INTO t1(id, name) VALUES (3, 'ccc')")
    cursor.execute("SELECT id, name FROM t1")
    assert cursor.fetchall() == (
        ('1', 'aaa'),
        ('2', 'bbb'),
        ('3', 'ccc'),
    )
    cursor.execute("UPDATE t1 SET name = 'bbb' WHERE id = 3")
    cursor.execute("SELECT id, name FROM t1 WHERE id = 3")
    assert cursor.fetchall() == (
        ('3', 'bbb'),
    )


def test_update_wrong_lock_raises(cursor):
    cursor.execute('CREATE TABLE t1(id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255))')
    cursor.execute("INSERT INTO t1(id, name) VALUES (1, 'aaa')")
    with pytest.raises(OperationalError):
        cursor.execute("UPDATE t1 SET name = 'bbb' WHERE id = 1 USE LOCK 'foo'")


def test_update_with_lock(cursor, etcdb_connection):
    cursor.execute('CREATE TABLE t1(id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255))')
    cursor.execute("INSERT INTO t1(id, name) VALUES (1, 'aaa')")
    lock = WriteLock(etcdb_connection.client, 'foo', 't1')
    lock.acquire()
    cursor.execute("UPDATE t1 SET name = 'bbb' WHERE id = 1 USE LOCK '%s'" % lock.id)
    lock.release()
    cursor.execute("SELECT id, name FROM t1 WHERE id = 1")
    assert cursor.fetchall() == (
        ('1', 'bbb'),
    )


def test_update_doesnt_release_lock(cursor, etcdb_connection):
    cursor.execute('CREATE TABLE t1(id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255))')
    cursor.execute("INSERT INTO t1(id, name) VALUES (1, 'aaa')")
    lock = WriteLock(etcdb_connection.client, 'foo', 't1')
    lock.acquire(ttl=0)
    cursor.execute("UPDATE t1 SET name = 'bbb' WHERE id = 1 USE LOCK '%s'" % lock.id)
    with pytest.raises(OperationalError):
        cursor.execute("UPDATE t1 SET name = 'bbb' WHERE id = 1")

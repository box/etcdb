import pytest

from etcdb import OperationalError
from etcdb.lock import Lock, ReadLock, WriteLock


def test_readers(etcdb_connection):
    cur = etcdb_connection.cursor()
    cur.execute('CREATE TABLE bar(id int not null PRIMARY KEY)')
    lock = ReadLock(etcdb_connection.client, 'foo', 'bar')
    lock.acquire(ttl=0)
    readers = lock.readers()
    lock.release()
    assert len(readers) == 1
    readers = lock.readers()
    assert len(readers) == 0

    lock.acquire(ttl=0)
    l2 = ReadLock(etcdb_connection.client, 'foo', 'bar')
    l2.acquire(ttl=0)
    readers = lock.readers()
    assert len(readers) == 2


def test_writers(etcdb_connection):
    cur = etcdb_connection.cursor()
    cur.execute('CREATE TABLE bar(id int not null PRIMARY KEY)')
    lock = WriteLock(etcdb_connection.client, 'foo', 'bar')
    lock.acquire(ttl=0)
    writers = lock.writers()
    assert len(writers) == 1
    lock.release()
    writers = lock.writers()
    assert len(writers) == 0

    lock.acquire(ttl=0)
    l2 = WriteLock(etcdb_connection.client, 'foo', 'bar')
    with pytest.raises(OperationalError):
        l2.acquire()


def test_attributes(etcdb_connection):
    cur = etcdb_connection.cursor()
    cur.execute('CREATE TABLE bar(id int not null PRIMARY KEY)')
    lock = WriteLock(etcdb_connection.client, 'foo', 'bar')
    lock.acquire(author='author foo', reason='reason foo')
    assert lock.author == 'author foo'
    assert lock.reason == 'reason foo'
    assert type(lock.created_at) == int
    assert lock.created_at > 0

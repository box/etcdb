import pytest

from etcdb import OperationalError
from etcdb.lock import Lock, ReadLock, WriteLock


def test_readers(etcdb_connection):
    cur = etcdb_connection.cursor()
    cur.execute('CREATE TABLE bar(id int not null PRIMARY KEY)')
    l = ReadLock(etcdb_connection.client, 'foo', 'bar')
    l.acquire()
    readers = l.readers()
    l.release()
    assert len(readers) == 1
    readers = l.readers()
    assert len(readers) == 0

    l.acquire()
    l2 = ReadLock(etcdb_connection.client, 'foo', 'bar')
    l2.acquire()
    readers = l.readers()
    assert len(readers) == 2


def test_writers(etcdb_connection):
    cur = etcdb_connection.cursor()
    cur.execute('CREATE TABLE bar(id int not null PRIMARY KEY)')
    l = WriteLock(etcdb_connection.client, 'foo', 'bar')
    l.acquire()
    writers = l.writers()
    assert len(writers) == 1
    l.release()
    writers = l.writers()
    assert len(writers) == 0

    l.acquire()
    l2 = WriteLock(etcdb_connection.client, 'foo', 'bar')
    with pytest.raises(OperationalError):
        l2.acquire()

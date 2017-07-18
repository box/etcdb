from etcdb.lock import Lock, ReadLock


def test_readers(etcdb_connection):
    l = ReadLock(etcdb_connection.client, 'foo', 'bar')
    l.acquire()
    readers = l.readers()
    l.release()
    assert len(readers) == 0

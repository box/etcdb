from etcdb.lock import MetaLock


def test_init(etcdb_connection):
    lock = MetaLock(etcdb_connection.client, 'foo', 'bar')
    assert lock.id is None

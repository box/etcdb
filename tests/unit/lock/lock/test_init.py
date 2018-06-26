import pytest

from etcdb import InternalError
from etcdb.lock import Lock


def test_lock_sets_attributes(etcdb_connection):
    lock = Lock(etcdb_connection.client, 'foo', 'bar', lock_id='some_lock')
    assert lock._db == 'foo'
    assert lock._tbl == 'bar'
    assert lock._id == 'some_lock'
    assert lock._lock_prefix is None


def test_acquire_raises(etcdb_connection):
    l = Lock(etcdb_connection.client, 'foo', 'bar', lock_id='some_lock')
    with pytest.raises(InternalError):
        l.acquire()

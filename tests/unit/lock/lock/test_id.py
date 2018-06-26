import pytest

from etcdb.lock import Lock


@pytest.mark.parametrize('lock_id', [
    None, 'some_lock'
])
def test_id(etcdb_connection, lock_id):
    lock = Lock(etcdb_connection.client, 'foo', 'bar', lock_id=lock_id)
    assert lock.id == lock_id

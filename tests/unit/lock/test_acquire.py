# noinspection PyPackageRequirements
import mock
# noinspection PyPackageRequirements
import pytest
from pyetcd import EtcdNodeExist

from etcdb import LOCK_WAIT_TIMEOUT, OperationalError
from etcdb.lock import Lock


# noinspection PyUnresolvedReferences
@pytest.mark.parametrize('db, tbl, lock_prefix, lock_name, key', [
    (
        'foo', 'bar', 'some_prefix', 'some_lock',
        '/foo/bar/some_prefix/some_lock'
    ),
    (
        'foo', 'bar', 'some_prefix', None,
        '/foo/bar/some_prefix'
    )
])
@mock.patch.object(Lock, '_keep_key_alive')
def test_acquire(mock_keep_key_alive, etcdb_connection,
                 db, tbl, lock_prefix, lock_name,
                 key):
    etcdb_connection._client = mock.Mock()

    lock = Lock(etcdb_connection.client, db, tbl, lock_id=lock_name)
    lock._lock_prefix = lock_prefix

    lock.acquire()

    etcdb_connection.client.compare_and_swap.assert_called_once_with(
        key, '', ttl=1, prev_exist=False
    )
    mock_keep_key_alive.assert_called_once_with(key, 1)


# noinspection PyUnresolvedReferences
@mock.patch('etcdb.lock.time')
@mock.patch.object(Lock, '_keep_key_alive')
def test_acquire_after_2nd(mock_keep_key_alive, mock_time,
                           etcdb_connection):

    mock_time.time.side_effect = [100, 101, 102]
    etcdb_connection._client = mock.Mock()

    lock = Lock(etcdb_connection.client, 'foo', 'bar', lock_id='some_lock')
    lock._lock_prefix = 'some_prefix'

    etcdb_connection.client.compare_and_swap.side_effect = [
        EtcdNodeExist,
        None
    ]
    lock.acquire()

    assert etcdb_connection.client.compare_and_swap.call_count == 2
    assert mock_keep_key_alive.call_count == 1


# noinspection PyUnusedLocal,PyUnresolvedReferences
@mock.patch('etcdb.lock.time')
@mock.patch.object(Lock, '_keep_key_alive')
def test_acquire_raises_after_lock_wait(mock_keep_key_alive,
                                        mock_time,
                                        etcdb_connection):

    mock_time.time.side_effect = [100, 101, 200 + LOCK_WAIT_TIMEOUT]
    etcdb_connection._client = mock.Mock()
    etcdb_connection._client.timeout = 123

    lock = Lock(etcdb_connection.client, 'foo', 'bar', lock_id='some_lock')
    lock._lock_prefix = 'some_prefix'

    etcdb_connection.client.compare_and_swap.side_effect = [
        EtcdNodeExist,
        EtcdNodeExist
    ]
    with pytest.raises(OperationalError):
        lock.acquire()

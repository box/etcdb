import json

# noinspection PyPackageRequirements
import mock
# noinspection PyPackageRequirements
import pytest
from pyetcd import EtcdNodeExist

from etcdb import LOCK_WAIT_TIMEOUT, OperationalError
from etcdb.lock import Lock


@pytest.mark.parametrize('db, tbl, lock_prefix, lock_name, key, author, reason', [
    (
        'foo', 'bar', 'some_prefix', 'some_lock',
        '/foo/bar/some_prefix/some_lock',
        'foo-author', 'foo-reason',
    ),
    (
        'foo', 'bar', 'some_prefix', None,
        '/foo/bar/some_prefix',
        'bar-author', 'bar-reason',
    )
])
@mock.patch('etcdb.lock.time.time')
def test_acquire(mock_time, etcdb_connection,
                 db, tbl, lock_prefix, lock_name,
                 key, author, reason):
    etcdb_connection._client = mock.Mock()

    lock = Lock(etcdb_connection.client, db, tbl, lock_id=lock_name)
    lock._lock_prefix = lock_prefix

    mock_time.return_value = 1529965029.75682

    lock.acquire(timeout=100, ttl=120, author=author, reason=reason)

    key_value = json.dumps(
        {
            'author': author,
            'reason': reason,
            'created_at': 1529965029
        }
    )

    etcdb_connection.client.compare_and_swap.assert_called_once_with(
        key, key_value, ttl=120, prev_exist=False
    )


# noinspection PyUnresolvedReferences
@mock.patch('etcdb.lock.time')
def test_acquire_after_2nd(mock_time,
                           etcdb_connection):

    # We call time.time() twice in every
    mock_time.time.side_effect = [100, 100, 101, 101, 102, 102]
    etcdb_connection._client = mock.Mock()

    lock = Lock(etcdb_connection.client, 'foo', 'bar', lock_id='some_lock')
    lock._lock_prefix = 'some_prefix'

    etcdb_connection.client.compare_and_swap.side_effect = [
        EtcdNodeExist,
        None
    ]
    lock.acquire()

    assert etcdb_connection.client.compare_and_swap.call_count == 2


# noinspection PyUnusedLocal,PyUnresolvedReferences
@mock.patch('etcdb.lock.time')
def test_acquire_raises_after_lock_wait(mock_time,
                                        etcdb_connection):

    mock_time.time.side_effect = [100, 101, 102, 200 + LOCK_WAIT_TIMEOUT]
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


@mock.patch.object(Lock, '_value')
def test_author_none(mock_value, etcdb_connection):
    mock_value.return_value = None
    lock = Lock(etcdb_connection.client, 'foo', 'bar')
    assert lock.author is None
    assert lock.reason is None
    assert lock.created_at == 0

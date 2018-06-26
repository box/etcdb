import mock
import pytest
from pyetcd import EtcdKeyNotFound

from etcdb import InternalError
from etcdb.lock import Lock


def test_release_raises_internal(etcdb_connection):
    mock_delete = mock.Mock()
    mock_delete.side_effect = EtcdKeyNotFound
    etcdb_connection.client.delete = mock_delete

    lock = Lock(etcdb_connection.client, 'foo', 'bar', lock_id='some_lock')
    with pytest.raises(InternalError):
        lock.release()


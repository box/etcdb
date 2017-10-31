import mock
import pytest
from pyetcd import EtcdKeyNotFound

from etcdb import InternalError
from etcdb.lock import Lock


@mock.patch('etcdb.lock.active_children')
def test_release_raises_internal(mock_active_children, etcdb_connection):
    mock_delete = mock.Mock()
    mock_delete.side_effect = EtcdKeyNotFound
    etcdb_connection.client.delete = mock_delete

    lock = Lock(etcdb_connection.client, 'foo', 'bar', lock_id='some_lock')
    with pytest.raises(InternalError):
        lock.release()
    mock_active_children.assert_called_once_with()


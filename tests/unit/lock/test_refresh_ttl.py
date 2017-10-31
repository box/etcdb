import mock
import pytest

from etcdb.lock import Lock


@pytest.mark.timeout(10)
@mock.patch('etcdb.lock.getppid')
def test_refresh_ttl(mock_getppid, etcdb_connection):
    mock_getppid.return_value = 1
    etcdb_connection._client = mock.Mock()
    lock = Lock(etcdb_connection.client, 'foo', 'bar', lock_id='some_lock')
    lock._refresh_ttl('/foo', 1)


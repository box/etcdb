import mock

from etcdb.lock import ReadLock


@mock.patch('etcdb.lock.uuid.uuid4')
def test_init(mock_uuid4, etcdb_connection):
    mock_uuid4.return_value = 'foo'
    lock = ReadLock(etcdb_connection.client, 'foo', 'bar')
    assert lock.id == 'foo'

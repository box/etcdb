import json

import etcdb
import pytest
import requests
import time


@pytest.fixture
def cursor():
    # Clean database
    http_response = requests.get('http://10.0.1.10:2379/v2/keys/')
    content = http_response.content
    node = json.loads(content)['node']
    try:
        for n in node['nodes']:
            key = n['key']
            requests.delete('http://10.0.1.10:2379/v2/keys{key}?recursive=true'.format(key=key))
    except KeyError:
        pass

    connection = etcdb.connect(host='10.0.1.10', db='foo', timeout=1)
    return connection.cursor()


def test_select_version(cursor):
    cursor.execute('SELECT VERSION()')
    assert cursor.fetchone()[0] == '2.3.7'


def test_get_meta_lock(cursor):
    print(cursor._get_meta_lock('foo', 'bar'))
    print(cursor._release_meta_lock('foo', 'bar'))

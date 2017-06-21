import json

import pytest
import requests

import etcdb


@pytest.fixture
def cursor():
    # Clean database
    http_response = requests.get('http://127.0.0.1:2379/v2/keys/')
    content = http_response.content
    node = json.loads(content)['node']
    try:
        for n in node['nodes']:
            key = n['key']
            requests.delete('http://127.0.0.1:2379/v2/keys{key}?recursive=true'.format(key=key))
    except KeyError:
        pass

    connection = etcdb.connect(host='127.0.0.1', db='foo', timeout=1)
    cursor = connection.cursor()
    cursor.execute('CREATE DATABASE foo')
    return cursor

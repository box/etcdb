import json

import pytest
import requests

import etcdb
from etcdb import OperationalError
from etcdb.connection import Connection


@pytest.yield_fixture
def etcdb_connection():
    connection = Connection(db='foo', host='127.0.0.1')
    cur = connection.cursor()
    try:
        cur.execute('DROP DATABASE foo')
    except OperationalError:
        pass
    cur.execute('CREATE DATABASE foo')
    yield connection
    try:
        cur.execute('DROP DATABASE foo')
    except OperationalError:
        pass


@pytest.yield_fixture
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
    cur = connection.cursor()
    cur.execute('CREATE DATABASE foo')
    yield cur
    try:
        cur.execute('DROP DATABASE foo')
    except OperationalError:
        pass

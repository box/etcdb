import pytest

from etcdb.connection import Connection
from etcdb.sqlparser.parser import SQLParser


@pytest.fixture
def etcdb_connection():
    return Connection(db='foo', host='10.0.1.10')


@pytest.fixture
def cursor(etcdb_connection):
    return etcdb_connection.cursor()


@pytest.fixture
def parser():
    return SQLParser()


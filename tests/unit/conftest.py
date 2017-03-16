import pytest

from etcdb.connection import Connection
from etcdb.sqlparser.parser import SQLParser


@pytest.fixture
def t_2016_9_21_23_10_3():
    return 1474499403.0


@pytest.fixture
def etcdb_connection():
    return Connection(db='foo')


@pytest.fixture
def cursor(etcdb_connection):
    return etcdb_connection.cursor()


@pytest.fixture
def parser():
    return SQLParser()


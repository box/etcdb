import json
from pprint import pprint

from pyetcd import EtcdNodeExist, EtcdKeyNotFound

from etcdb import ProgrammingError, OperationalError


def create_database(etcd_client, tree):
    """
    Create database.

    :param etcd_client: Etcd client
    :type etcd_client: Client
    :param tree: Parsing tree
    :type tree: SQLTree
    """
    try:
        etcd_client.mkdir('/%s' % tree.db)
    except EtcdNodeExist:
        raise ProgrammingError("Can't create database '%s'; database exists"
                               % tree.db)


def create_table(etcd_client, tree, db=None):
    """
    Create table.

    :param etcd_client: Etcd client
    :type etcd_client: Client
    :param tree: Parsing tree
    :type tree: SQLTree
    :param db: Database name to use if not defined in the parsing tree.
    :type db: str
    :raise ProgrammingError: if database is not selected,
        or if primary key is not defined,
        or the primary key is NULL-able,
        or table exists.
    """
    if not db:
        db = tree.db

    if not db:
        raise ProgrammingError('No database selected')

    # Check if database exists
    try:
        etcd_client.read('/%s' % db)
    except EtcdKeyNotFound:
        raise OperationalError("Unknown database '%s'" % db)

    pk_field = None
    for field_name, value in tree.fields.iteritems():
        try:
            if value['options']['primary']:
                pk_field = field_name
        except KeyError:
            pass

    if not pk_field:
        raise ProgrammingError('Primary key must be defined')

    if tree.fields[pk_field]['options']['nullable']:
        raise ProgrammingError('Primary key must be NOT NULL')

    try:
        full_table_name = '/%s/%s' % (db, tree.table)
        etcd_client.mkdir(full_table_name)
        etcd_client.write(full_table_name + "/_fields",
                          json.dumps(tree.fields))
    except EtcdNodeExist:
        raise OperationalError("Table '%s' already exists" % tree.table)



def other():
    try:
        table_name = '/%s/%s' % (db, tree.table)
        self.connection.client.mkdir(table_name)
        self.connection.client.write(table_name + "/_fields", json.dumps(tree.fields))
    except EtcdNodeExist as err:
        raise ProgrammingError("Failed to create table: %s" % err)


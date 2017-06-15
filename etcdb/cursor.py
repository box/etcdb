import json

import time
import uuid
from multiprocessing import Process

from multiprocessing import active_children
from pyetcd import EtcdNodeExist, EtcdKeyNotFound

from etcdb import ProgrammingError, OperationalError, LOCK_WAIT_TIMEOUT
from etcdb.eval_expr import eval_expr
from etcdb.execute.ddl.create import create_database, create_table
from etcdb.execute.ddl.drop import drop_database, drop_table
from etcdb.execute.dml.delete import execute_delete
from etcdb.execute.dml.insert import insert
from etcdb.execute.dml.select import execute_select
from etcdb.execute.dml.show import show_databases, show_tables, desc_table
from etcdb.execute.dml.update import execute_update
from etcdb.execute.dml.use import use_database
from etcdb.log import LOG
from etcdb.sqlparser.parser import SQLParser, SQLParserError


class ColInfo(object):
    def __init__(self, name='', width=None):
        self.name = name
        if width and width > len(name):
            self.width = width
        else:
            self.width = len(self.name)

    def __repr__(self):
        return "ColInfo: name={name}, width={width}".format(name=self.name,
                                                            width=self.width)


class Cursor(object):
    """These objects represent a database cursor, which is used to manage the context of a fetch operation.
    Cursors created from the same connection are not isolated, i.e. , any changes done to the database
    by a cursor are immediately visible by the other cursors. Cursors created from different connections can
    or can not be isolated, depending on how the transaction support is implemented
    (see also the connection's .rollback () and .commit () methods). """

    description = None
    """This read-only attribute is a sequence of 7-item sequences.

    Each of these sequences contains information describing one result column:

        name
        type_code
        display_size
        internal_size
        precision
        scale
        null_ok

    The first two items ( name and type_code ) are mandatory, the other five are optional and are set to None
    if no meaningful values can be provided. """

    @property
    def rowcount(self):
        return self._rowcount

    """This read-only attribute specifies the number of rows that the last .execute*() produced
    (for DQL statements like SELECT ) or affected (for DML statements like UPDATE or INSERT )."""

    arraysize = 1
    """This read/write attribute specifies the number of rows to fetch at a time with .fetchmany().
    It defaults to 1 meaning to fetch a single row at a time. """

    connection = None
    """Etcd connection object"""

    # ColInfo = ColInfo

    _result_set = None

    def __init__(self, connection):
        self.connection = connection
        self._sql_parser = SQLParser()
        self._db = connection.db
        self._timeout = connection.timeout
        self.lastrowid = None
        self._rowcount = 0

    @property
    def n_cols(self):
        return self._result_set.n_cols

    @property
    def n_rows(self):
        try:
            return self._result_set.n_rows
        except AttributeError:
            return 0

    @property
    def result_set(self):
        return self._result_set

    @staticmethod
    def close():
        """Close the cursor now (rather than whenever __del__ is called). """
        pass

    @staticmethod
    def morgify(query, args):
        """Prepare query string that will be sent to parser

        :param query: Query text
        :param args: Tuple with query arguments
        :return: Query text
        :rtype: str
        """
        if args:
            query %= tuple(["'%s'" % a for a in args])
        return query

    def execute(self, query, args=None):
        """Prepare and execute a database operation (query or command).

        :param query: Query text.
        :type query: str
        :param args: Optional query arguments.
        :type args: tuple
        :raise ProgrammingError: if query can't be parsed."""

        query = self.morgify(query, args)

        LOG.debug('Executing: %s', query)

        try:
            tree = self._sql_parser.parse(query)
        except SQLParserError as err:
            raise ProgrammingError(err)

        if not self._db:
            self._db = tree.db

        self._result_set = None
        self._rowcount = 0

        if tree.query_type == "SHOW_DATABASES":
            self._result_set = show_databases(self.connection.client)

        elif tree.query_type == "CREATE_DATABASE":
            create_database(self.connection.client, tree)

        elif tree.query_type == "DROP_DATABASE":
            drop_database(self.connection.client, tree)

        elif tree.query_type == "USE_DATABASE":
            self._db = use_database(self.connection.client, tree)

        elif tree.query_type == "CREATE_TABLE":
            create_table(self.connection.client, tree, db=self._db)

        elif tree.query_type == "DROP_TABLE":
            drop_table(self.connection.client, tree, db=self._db)

        elif tree.query_type == "SHOW_TABLES":
            self._result_set = show_tables(self.connection.client, tree,
                                           db=self._db)
        elif tree.query_type == "DESC_TABLE":
            self._result_set = desc_table(self.connection.client, tree,
                                          db=self._db)
        elif tree.query_type == "INSERT":
            self._rowcount = insert(self.connection.client, tree,
                                    db=self._db)
        elif tree.query_type == 'SELECT':
            self._result_set = execute_select(self.connection.client, tree,
                                              db=self._db)
        elif tree.query_type == "UPDATE":
            self._rowcount = execute_update(self.connection.client, tree,
                                            db=self._db)
        elif tree.query_type == "DELETE":
            self._rowcount = execute_delete(self.connection.client, tree,
                                            db=self._db)
        if self._result_set is not None:
            self._rowcount = self._result_set.n_rows

    @staticmethod
    def executemany(operation, **kwargs):
        """Prepare a database operation (query or command) and then execute it against all parameter sequences
        or mappings found in the sequence seq_of_parameters . """
        pass

    def fetchone(self):
        """Fetch the next row of a query result set, returning a single sequence,
        or None when no more data is available."""
        try:
            return tuple(self._result_set.next())
        except (StopIteration, AttributeError):
            return None

    def fetchmany(self, n):
        """Fetch the next set of rows of a query result, returning a sequence of sequences (e.g. a list of tuples).
        An empty sequence is returned when no more rows are available. """
        rows = ()
        for i in xrange(n):
            row = self.fetchone()
            if row:
                rows += (row,)
        return rows

    def fetchall(self):
        """Fetch all (remaining) rows of a query result, returning them as a sequence of sequences
        (e.g. a list of tuples). Note that the cursor's arraysize attribute can affect
        the performance of this operation."""

        result = ()
        if len(self._result_set)> 0:
            for row in self._result_set:
                result += (tuple(row), )
            self._result_set.rows = []
        return result

    @staticmethod
    def setinputsizes(sizes):
        """This can be used before a call to .execute*() to predefine memory areas for the operation's parameters. """
        pass

    @staticmethod
    def setoutputsize(size):
        """Set a column buffer size for fetches of large columns (e.g. LONG s, BLOB s, etc.). The column is specified
        as an index into the result sequence. Not specifying the column will set the default size for all large columns
        in the cursor. """
        pass

    def _execute_select(self, tree):
        db = self._get_current_db(tree)
        tbl = tree.table
        rows = ()
        columns = ()
        lock_id = self._get_read_lock(db, tbl)
        try:
            function_exists = False
            variable_exists = False
            for expression in tree.expressions:
                columns += (expression['name'],)
                if expression['type'] == 'function':
                    function_exists = True
                if expression['type'] == 'variable':
                    variable_exists = True

            result_keys = self._get_pks(db, tbl, tree)

            if function_exists or variable_exists:
                result_keys.append(None)

            for pk in result_keys:
                row = self.get_table_row(tree, pk)
                if tree.where:
                    if eval_expr((columns, row), tree.where):
                        rows += (row,)
                else:
                    rows += (row,)

            if tree.order['by'] and tree.order['by'] in columns:
                pos = columns.index(tree.order['by'])

                def getKey(item):
                    return item[pos]

                reverse = False
                if tree.order['direction'] == 'DESC':
                    reverse = True

                rows = sorted(rows, reverse=reverse, key=getKey)

            if tree and tree.limit is not None:
                rows = rows[:tree.limit]
            return columns, rows
        finally:
            self._release_read_lock(db, tbl, lock_id)

    def _eval_function(self, name, db=None, tbl=None):
        if name == "VERSION":
            return self._eval_function_version()
        elif name == "COUNT":
            return self._eval_function_count(db, tbl)

    def _eval_variable(self, name):
        if name == "SQL_MODE":
            return self._eval_variable_sql_mode()

    def _eval_function_version(self):
        return self.connection.client.version()

    def _get_pks(self, db, table, tree=None):
        """
        Get list of primary key values for a given table

        :param db: database name
        :param table: table name
        :param tree: instance of SQLTree
        :return: list of values or empty list if table is empty
        """
        if not table:
            return []
        pks = []
        table_key = "/{db}/{tbl}".format(db=db, tbl=table)
        etcd_result = self.connection.client.read(table_key)
        pk = self._get_pk(db, table)
        pk_name = self._get_pk_name(db, table)
        pk_type = pk[pk_name]['type']
        try:
            nodes = etcd_result.node['nodes']

            for n in nodes:
                pk_key = n['key']
                pk = pk_key.replace(table_key + '/', '', 1)
                if pk_type in ['INT', 'INTEGER', 'SMALLINT', 'TINYINT']:
                    pk = int(pk)
                pks.append(pk)

            pks = sorted(pks)

        except KeyError:
            pass
        return pks

    def get_table_row(self, tree, pk):
        db = self._get_current_db(tree)
        table = tree.table

        if pk:
            key = "/{db}/{tbl}/{pk}".format(db=db,
                                            tbl=table,
                                            pk=pk)
            etcd_response = self.connection.client.read(key)
            full_row = json.loads(etcd_response.node['value'])
            row = ()
            for e in tree.expressions:
                field_name = e['name']
                field = None
                if e['type'] == 'function':
                    field = self._eval_function(e['name'], db=db, tbl=table)
                if e['type'] == 'variable':
                    field = self._eval_variable(e['name'])
                if e['type'] == 'field':
                    try:
                        field = full_row[field_name]
                    except KeyError:
                        raise ProgrammingError('Error: Field %s not found' % field_name)
                row += (field, )
            return row

        else:  # One row if pk is None
            row = ()
            for e in tree.expressions:
                field = None
                if e['type'] == 'function':
                    field = self._eval_function(e['name'], db=db, tbl=table)
                if e['type'] == 'variable':
                    field = self._eval_variable(e['name'])
                row += (field, )
            return row

    @staticmethod
    def _eval_variable_sql_mode():
        return "STRICT_ALL_TABLES"

    def _execute_drop_database(self, db):
        self.connection.client.rmdir('/%s' % db, recursive=True)


    def _get_table_fields(self, db, tbl):
        etcd_result = self.connection.client.read('/{db}/{tbl}/_fields'.format(db=db, tbl=tbl))
        value = etcd_result.node['value']
        return json.loads(value)

    def _get_pk(self, db, tbl):
        for f, v in self._get_table_fields(db, tbl).iteritems():
            try:
                if v['options']['primary']:
                    return {
                        f: v
                    }
            except KeyError:
                pass
        return None

    def _get_pk_name(self, db, tbl):
        return self._get_pk(db, tbl).keys()[0]

    def _execute_insert(self, tree):
        db = self._get_current_db(tree)
        table = tree.table

        self._get_write_lock(db, table)

        try:
            pk_field = self._get_pk_name(db, table)
            record = {}
            for field, v in self._get_table_fields(db, table).iteritems():
                field_options = v['options']
                try:
                    record[field] = tree.fields[field]
                except KeyError:
                    # value is not given
                    if 'auto_increment' in field_options:
                        self.lastrowid = self._get_next_auto_inc(db, table)
                        record[field] = str(self.lastrowid)
                    elif not field_options['nullable']:
                        try:
                            record[field] = field_options['default']
                        except KeyError:
                            record[field] = None
                            # Ignore this check for now
                            # raise ProgrammingError('Error: Field %s cannot be NULL and no default value is set')
                    else:
                        record[field] = None

            pk_value = record[pk_field]
            self.connection.client.write('/{db}/{tbl}/{pk}'.format(db=db,
                                                                   tbl=table,
                                                                   pk=pk_value), json.dumps(record, sort_keys=True))
            self._set_next_auto_inc(db, table)
        finally:
            self._release_write_lock(db, table)

    @staticmethod
    def _update_columns(columns_names, rows):
        """
        Take a tuple of column names and set their widths to maximums from rows

        :param columns_names: Tuple of column names
        :param rows: Tuple of records. A record is a tuple, too.
        :return: Tuple of ColInfo() instances where widths are large enough to fit any value from rows
        """
        widths = []
        columns = ()

        for col in columns_names:
            columns += (ColInfo(name=col), )

        try:
            for col in columns_names:
                widths.append(len(col))

            for row in rows:
                i = 0
                for _ in columns_names:
                    if len(row[i]) > widths[i]:
                        widths[i] = len(row[i])
                    i += 1

            i = 0
            for col in columns:
                col.width = widths[i]
                i += 1

        except TypeError:
            pass

        return columns

    def _get_meta_lock(self, db, tbl):
        """
        Set a meta lock

        :param db: database name
        :param tbl: table name
        """
        key = "/{db}/{tbl}/_lock_meta".format(
            db=db,
            tbl=tbl
        )
        expires = time.time() + LOCK_WAIT_TIMEOUT
        while time.time() < expires:
            try:
                response = self.connection.client.\
                    compare_and_swap(key, '',
                                     ttl=self._timeout,
                                     prev_exist=False)
                self._keep_key_alive(key, self._timeout)
                return response
            except EtcdNodeExist:
                pass

        raise OperationalError('Lock wait timeout')

    def _release_meta_lock(self, db, tbl):
        """
        Release a meta lock

        :param db: database name
        :param tbl: table name
        """
        key = "/{db}/{tbl}/_lock_meta".format(
            db=db,
            tbl=tbl
        )
        response = self.connection.client.delete(key)
        active_children()
        return response

    def _refresh_ttl(self, key, timeout):
        while True:
            try:
                self.connection.client.update_ttl(key, timeout)
            except (EtcdKeyNotFound, KeyboardInterrupt):
                break

    def _get_read_lock(self, db, tbl):
        """
        Get read lock on a table db.tbl.
        Read lock is shared i.e. if other clients are allowed to read from the table

        :param db: database name
        :param tbl: table name
        :return: string with id of the lock or None if db or tbl is empty
        """
        if not db or not tbl:
            return None
        self._get_meta_lock(db, tbl)
        lock_id = str(uuid.uuid4())

        if self._write_lock_set(db, tbl):
            self._wait_until_write_lock_deleted(db, tbl)

        read_lock_key = '/{db}/{tbl}/_lock_read/{lock_id}'.format(
            db=db,
            tbl=tbl,
            lock_id=lock_id
        )
        self.connection.client.write(read_lock_key, '', ttl=self._timeout)
        self._keep_key_alive(read_lock_key, self._timeout)

        self._release_meta_lock(db, tbl)

        return lock_id

    def _release_read_lock(self, db, tbl, lock_id):
        """
        Release lock previously set on a table

        :param db: database name
        :param tbl: table name
        :param lock_id: string with lock identifier
        """
        if not lock_id:
            return
        read_lock_key = '/{db}/{tbl}/_lock_read/{lock_id}'.format(
            db=db,
            tbl=tbl,
            lock_id=lock_id
        )
        self.connection.client.delete(read_lock_key)

    def _write_lock_set(self, db, tbl):
        """
        Check if write lock is set on a table

        :param db: database name
        :param tbl: table name
        :return: True or False
        """
        write_lock_key = '/{db}/{tbl}/_lock_write'.format(
            db=db,
            tbl=tbl
        )
        try:
            self.connection.client.read(write_lock_key)
            return True
        except EtcdKeyNotFound:
            return False

    def _wait_until_write_lock_deleted(self, db, tbl):
        """
        Wait until the write lock is unset

        :param db: database name
        :param tbl: table name
        """
        write_lock_key = '/{db}/{tbl}/_lock_write'.format(
            db=db,
            tbl=tbl
        )
        try:
            while True:
                self.connection.client.read(write_lock_key, wait=True)
        except EtcdKeyNotFound:
            pass

    def _get_active_read_locks(self, db, tbl):
        """
        Get list of active read locks on a table

        :param db: database name
        :param tbl: table name
        :return: list of locks or empty list
        """
        read_lock_key = '/{db}/{tbl}/_lock_read'.format(
            db=db,
            tbl=tbl
        )
        active_read_locks = []
        try:
            for lock in self.connection.client.read(read_lock_key).node['nodes']:
                lock_id = lock['key'].replace('/{db}/{tbl}/_lock_read/'.format(
                    db=db,
                    tbl=tbl
                ), '', 1)
                active_read_locks.append(lock_id)
        except (EtcdKeyNotFound, KeyError):
            pass
        return active_read_locks

    def _get_write_lock(self, db, tbl):
        """
        Set a write lock on a table

        :param db:
        :param tbl:
        """
        self._get_meta_lock(db, tbl)
        self._ensure_no_write_lock(db, tbl)
        self._ensure_no_read_lock(db, tbl)

        write_lock_key = '/{db}/{tbl}/_lock_write'.format(
            db=db,
            tbl=tbl
        )
        self.connection.client.write(write_lock_key, '', ttl=self._timeout)
        self._keep_key_alive(write_lock_key, self._timeout)
        self._release_meta_lock(db, tbl)

    def _release_write_lock(self, db, tbl):
        write_lock_key = '/{db}/{tbl}/_lock_write'.format(
            db=db,
            tbl=tbl
        )
        self.connection.client.delete(write_lock_key)

    def _ensure_no_write_lock(self, db, tbl):
        if self._write_lock_set(db, tbl):
            self._wait_until_write_lock_deleted(db, tbl)

    def _ensure_no_read_lock(self, db, tbl):
        for lock in self._get_active_read_locks(db, tbl):
            self._wait_until_read_lock_released(db, tbl, lock)

    def _wait_until_read_lock_released(self, db, tbl, lock_id):
        read_lock_key = '/{db}/{tbl}/_lock_read/{lock_id}'.format(
            db=db,
            tbl=tbl,
            lock_id=lock_id
        )
        try:
            while True:
                self.connection.client.read(read_lock_key)
                # TODO take into account modifiedIndex because race condition is possible
                # modified_index = response.node['modifiedIndex']
                self.connection.client.read(read_lock_key,
                                            params={},
                                            wait=True)
        except EtcdKeyNotFound:
            pass

    def _keep_key_alive(self, key, timeout):
        p = Process(target=self._refresh_ttl, args=(key, timeout))
        p.start()

    def _get_next_auto_inc(self, db, tbl):
        key = '/{db}/{tbl}/_auto_inc'.format(
            db=db,
            tbl=tbl,
        )
        try:
            etcd_result = self.connection.client.read(key)
            return int(etcd_result.node['value'])
        except EtcdKeyNotFound:
            return 1

    def _set_next_auto_inc(self, db, tbl):
        n = self._get_next_auto_inc(db, tbl)
        key = '/{db}/{tbl}/_auto_inc'.format(
            db=db,
            tbl=tbl,
        )
        self.connection.client.write(key, n + 1)

    def _execute_update(self, tree):
        return 1

    def _eval_function_count(self, db, tbl):
        key = '/{db}/{tbl}'.format(
            db=db,
            tbl=tbl,
        )
        etcd_result = self.connection.client.read(key)
        try:
            return int(len(etcd_result.node['nodes']))
        except KeyError:
            return 0

    def _execute_wait(self, tree):

        pk = tree.expressions[0]['args'][0]

        key = "/{db}/{tbl}/{pk}".format(db=self._get_current_db(tree),
                                        tbl=tree.table,
                                        pk=pk)
        etcd_response = self.connection.client.read(key, wait=True)
        full_row = json.loads(etcd_response.node['value'])

        columns = ()
        for col in full_row.keys():
            columns += (col, )

        row = ()
        for col in columns:
            row += (full_row[col], )

        rows = (row, )

        return columns, rows

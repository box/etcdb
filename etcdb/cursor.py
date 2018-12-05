from pprint import pprint

from pyetcd import EtcdKeyNotFound, EtcdRaftInternal

from etcdb import ProgrammingError, InternalError
from etcdb.execute.ddl.create import create_database, create_table
from etcdb.execute.ddl.drop import drop_database, drop_table
from etcdb.execute.dml.delete import execute_delete
from etcdb.execute.dml.insert import insert
from etcdb.execute.dml.select import execute_select
from etcdb.execute.dml.show import show_databases, show_tables, desc_table
from etcdb.execute.dml.update import execute_update
from etcdb.execute.dml.use import use_database
from etcdb.execute.dml.wait import execute_wait
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
    """These objects represent a database cursor, which is used to manage
    the context of a fetch operation.
    Cursors created from the same connection are not isolated, i.e. ,
    any changes done to the database
    by a cursor are immediately visible by the other cursors.
    Cursors created from different connections can
    or can not be isolated, depending on how
    the transaction support is implemented
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

    The first two items ( name and type_code ) are mandatory, the other five
    are optional and are set to None
    if no meaningful values can be provided. """

    @property
    def rowcount(self):
        return self._rowcount

    """This read-only attribute specifies the number of rows that
    the last .execute*() produced
    (for DQL statements like SELECT ) or affected
    (for DML statements like UPDATE or INSERT )."""

    arraysize = 1
    """This read/write attribute specifies the number of rows
    to fetch at a time with .fetchmany().
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
        :raise ProgrammingError: if query can't be parsed.
        :raise InternalError: If etcd is not ready to serve request
        """

        query = self.morgify(query, args)

        LOG.debug('Executing: %s', query)

        try:
            tree = self._sql_parser.parse(query)
        except SQLParserError as err:
            raise ProgrammingError('Error while parsing query: %s: %s'
                                   % (query, err))

        if not self._db:
            self._db = tree.db

        self._result_set = None
        self._rowcount = 0

        try:
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
            elif tree.query_type == 'WAIT':
                self._result_set = execute_wait(self.connection.client, tree,
                                                db=self._db)
            elif tree.query_type == "UPDATE":
                self._rowcount = execute_update(self.connection.client, tree,
                                                db=self._db)
            elif tree.query_type == "DELETE":
                self._rowcount = execute_delete(self.connection.client, tree,
                                                db=self._db)
        except EtcdRaftInternal as err:
            raise InternalError(err)

        if self._result_set is not None:
            self._rowcount = self._result_set.n_rows

    @staticmethod
    def executemany(operation, **kwargs):
        """Prepare a database operation (query or command) and then execute it
        against all parameter sequences
        or mappings found in the sequence seq_of_parameters . """
        pass

    def fetchone(self):
        """Fetch the next row of a query result set, returning
        a single sequence,
        or None when no more data is available."""
        try:
            return tuple(self._result_set.next())
        except (StopIteration, AttributeError):
            return None

    def fetchmany(self, n):
        """Fetch the next set of rows of a query result, returning
        a sequence of sequences (e.g. a list of tuples).
        An empty sequence is returned when no more rows are available. """
        rows = ()
        for i in xrange(n):
            row = self.fetchone()
            if row:
                rows += (row,)
        return rows

    def fetchall(self):
        """Fetch all (remaining) rows of a query result, returning them as
        a sequence of sequences
        (e.g. a list of tuples). Note that the cursor's arraysize attribute
        can affect the performance of this operation."""

        result = ()
        if len(self._result_set) > 0:
            for row in self._result_set:
                result += (tuple(row), )
            self._result_set.rows = []
        return result

    @staticmethod
    def setinputsizes(sizes):
        """This can be used before a call to .execute*() to predefine
        memory areas for the operation's parameters. """
        pass

    @staticmethod
    def setoutputsize(size):
        """Set a column buffer size for fetches of large columns (e.g. LONG s,
        BLOB s, etc.). The column is specified
        as an index into the result sequence. Not specifying
        the column will set the default size for all large columns
        in the cursor. """
        pass

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

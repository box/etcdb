"""Classes that represent query results"""
import json

from etcdb import ProgrammingError


class ColumnOptions(object):  # pylint: disable=too-few-public-methods
    """ColumnOptions represents column options like NULL-able or not"""

    def __init__(self, *options, **kwoptions):
        for dictionary in options:
            for key in dictionary:
                setattr(self, key, dictionary[key])

        for key in kwoptions:
            setattr(self, key, kwoptions[key])

    auto_increment = False
    primary = False
    nullable = None
    default = None
    unique = None


class Column(object):
    """
    Instantiate a Column

    :param colname: Column name.
    :type colname: str
    :param coltype: Column type
    :type coltype: str
    :param options: Column options
    :type options: ColumnOptions
    """
    def __init__(self, colname, coltype=None, options=None):
        self._colname = colname
        self._print_width = len(self._colname)
        self._coltype = coltype
        self._options = options

    @property
    def name(self):
        """Column name"""
        return self._colname

    @property
    def type(self):
        """Column type e.g. INT, VARCHAR, etc."""
        return self._coltype

    @property
    def auto_increment(self):
        """True if column is auto_incrementing."""
        return self._options.auto_increment

    @property
    def primary(self):
        """True if column is primary key."""
        return self._options.primary

    @property
    def nullable(self):
        """True if column is NULL-able."""
        return self._options.nullable

    @property
    def default(self):
        """Column default value."""
        return self._options.default

    @property
    def unique(self):
        """True if column is unique key."""
        return self._options.unique

    @property
    def print_width(self):
        """How many symbols client has to spare to print column value.
         A column name can be short, but its values may be longer.
         To align column and its values print_width is number of characters
         a client should allocate for the column name so it will be as lager
         as the largest columns length value."""
        return self._print_width

    def set_print_width(self, width):
        """Sets print_width."""
        self._print_width = width

    def __eq__(self, other):
        return self.name == other.name

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return self.name


class ColumnSet(object):
    """
    Instantiate a Column set

    :param columns: Optional dictionary with column definitions
    :type columns: dict
    """
    def __init__(self, columns=None):

        self._columns = []
        self._column_position = 0
        if columns:
            for colname, value in columns.iteritems():
                self._columns.append(Column(colname,
                                            coltype=value['type'],
                                            options=ColumnOptions(
                                                value['options']
                                            )))
        else:
            self._columns = []
            self._column_position = 0

    def add(self, column):
        """Add column to ColumnSet

        :param column: Column instance
        :type column: Column
        :return: Updated CoulmnSet instance
        :rtype: ColumnSet
        """
        if isinstance(column, Column):
            self._columns.append(column)
        else:
            raise ProgrammingError('%s must be Column type' % column)
        return self

    @property
    def empty(self):
        """True if there are no columns in the ColumnSet"""
        return not len(self._columns)

    @property
    def columns(self):
        """Returns list of Columns"""
        return self._columns

    @property
    def primary(self):
        """Return primary key column"""
        for col in self._columns:
            if col.primary:
                return col
        return None

    def __contains__(self, item):
        return item in self._columns

    def __eq__(self, other):
        return self.columns == other.columns

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return "[" + ', '.join(['"%s"' % str(x) for x in self._columns]) + "]"

    def __len__(self):
        return len(self._columns)

    def __iter__(self):
        return self

    def next(self):
        """Return next Column"""
        self._column_position += 1
        try:
            return self._columns[self._column_position - 1]
        except IndexError:
            self._column_position = 0
            raise StopIteration()

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._columns[key]
        else:
            for col in self._columns:
                if col == Column(key):
                    return col

    def index(self, column):
        """Find position of the given column in the set."""
        return self._columns.index(column)


class Row(object):
    """
    Row class

    :param row: Row values
    :type row: tuple
    """
    def __init__(self, row):
        if not isinstance(row, tuple):
            raise ProgrammingError('%s must be tuple')
        self._row = row
        self._field_position = 0

    @property
    def row(self):
        """
        :return: Return tuple with row values..
        :rtype: tuple
        """
        return self._row

    def __str__(self):
        return json.dumps(self._row)

    def __eq__(self, other):
        try:
            return self._row == other.row
        except AttributeError:
            return False

    def __iter__(self):
        return self

    def next(self):
        """Return next field in the row."""
        self._field_position += 1
        try:
            return self._row[self._field_position - 1]
        except IndexError:
            self._field_position = 0
            raise StopIteration()

    def __getitem__(self, key):
        return self._row[key]


class ResultSet(object):
    """
    Represents query result

    :param columns: Column set instance.
    :type columns: ColumnSet
    :param rows: List of Rows
    :type rows: list(Row)
    """
    def __init__(self, columns, rows=None):

        if not isinstance(columns, ColumnSet):
            raise ProgrammingError('%s must be ColumnSet' % columns)

        if columns.empty:
            raise ProgrammingError('columns must not be empty')
        self.columns = columns
        if rows:
            self.rows = rows
            for row in self.rows:
                self._update_print_width(row)
        else:
            self.rows = []

        self._pos = 0

    def __eq__(self, other):
        try:
            return all((self.columns == other.columns,
                        self.rows == other.rows))
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return "Columns: %s\nRows: %s" \
               % (
                   self.columns,
                   "[" + ', '.join(['%s' % str(x) for x in self.rows]) + "]"
               )

    def __iter__(self):
        return self

    def next(self):
        """Return next row in the result set."""
        self._pos += 1
        try:
            return self.rows[self._pos - 1]
        except IndexError:
            raise StopIteration()

    def __getitem__(self, key):
        return self.rows[key]

    def __len__(self):
        return len(self.rows)

    def rewind(self):
        """Move internal records pointer to the beginning of the result set.
        After this call .fetchone() will start returning rows again."""
        self._pos = 0

    @property
    def n_rows(self):
        """Return number of rows in the result set."""
        return len(self.rows)

    @property
    def n_cols(self):
        """Return number of columns in the result set."""
        return len(self.columns)

    def add_row(self, row):
        """
        Add row to result set

        :param row: Row instance
        :type row: Row
        :return: Updated result set
        :rtype: ResultSet
        """
        self.rows.append(row)
        self._update_print_width(row)
        # return self

    def _update_print_width(self, row):
        i = 0
        for field in row:
            if len(str(field)) > self.columns[i].print_width:
                self.columns[i].set_print_width(len(str(field)))
            i += 1

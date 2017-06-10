"""Classes that represent query results"""
import json

from etcdb import ProgrammingError


class Column(object):
    def __init__(self, colname):
        self._colname = colname
        self._print_width = len(self._colname)

    @property
    def name(self):
        return self._colname

    @property
    def print_width(self):
        return self._print_width

    def set_print_width(self, width):
        self._print_width = width

    def __eq__(self, other):
        return self.name == other.name

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return self.name


class ColumnSet(object):
    def __init__(self):
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
        return not len(self._columns)

    @property
    def columns(self):
        return self._columns

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
        self._column_position += 1
        try:
            return self._columns[self._column_position - 1]
        except IndexError:
            self._column_position = 0
            raise StopIteration()

    def __getitem__(self, key):
        return self._columns[key]


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
        self._field_position += 1
        try:
            return self._row[self._field_position - 1]
        except IndexError:
            self._field_position = 0
            raise StopIteration()


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
        return all((self.columns == other.columns,
                    self.rows == other.rows))

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
        self._pos += 1
        try:
            return self.rows[self._pos - 1]
        except IndexError:
            self._pos = 0
            raise StopIteration()

        #if self.i < self.n:
        #    i = self.i
        #    self.i += 1
        #    return i
        #else:
        #    raise StopIteration()

    @property
    def n_rows(self):
        return len(self.rows)

    @property
    def n_cols(self):
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
            if len(field) > self.columns[i].print_width:
                self.columns[i].set_print_width(len(field))
            i += 1
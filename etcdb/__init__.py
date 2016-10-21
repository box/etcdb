# -*- coding: utf-8 -*-
import time

from .etcddate import EtcdDate
from .etcdstring import EtcdString
from .etcdtime import EtcdTime
from .etcdtimestamp import EtcdTimestamp

__author__ = 'Box TechOps Database Team'
__email__ = 'oss@box.com'
__version__ = '1.0.1'


def _split_version(version):
    """
    Generate version tuple from string

    :param version: version string with two dots. E.g. 1.2.3
    :return: tuple with major, minor, patch versions. E.g. (1, 2, 3)
    """
    return tuple(version.split('.'))

version_info = _split_version(__version__)

apilevel = "1.0"
"""supported DB API level."""
threadsafety = 3
"""the level of thread safety. Threads may share the module, connections and cursors."""
paramstyle = "qmark"
"""the type of parameter marker formatting. Question mark style, e.g. ...WHERE name=?."""

NULL = None
LOCK_WAIT_TIMEOUT = 50


def enum(**enums):
    return type('Enum', (), enums)


EtcdTableLock = enum(read='read', write='write')

# Exceptions


class Error(StandardError):
    """Exception that is the base class of all other error exceptions."""


class Warning(StandardError):
    """Exception raised for important warnings like data truncations while inserting, etc."""


class InterfaceError(Error):
    """Exception raised for errors that are related to the database interface rather than the database itself."""


class DatabaseError(Error):
    """Exception raised for errors that are related to the database."""


class DataError(DatabaseError):
    """Exception raised for errors that are due to problems with the processed data like division by zero,
    numeric value out of range, etc."""


class OperationalError(DatabaseError):
    """Exception raised for errors that are related to the database's operation and not necessarily
    under the control of the programmer, e.g. an unexpected disconnect occurs, the data source name is not found,
    a transaction could not be processed, a memory allocation error occurred during processing, etc."""


class IntegrityError(DatabaseError):
    """Exception raised when the relational integrity of the database is affected, e.g. a foreign key check fails."""


class InternalError(DatabaseError):
    """Exception raised when the database encounters an internal error, e.g. the cursor is not valid anymore,
    the transaction is out of sync, etc. """


class ProgrammingError(DatabaseError):
    """Exception raised for programming errors, e.g. table not found or already exists, syntax error
    in the SQL statement, wrong number of parameters specified, etc."""


class NotSupportedError(DatabaseError):
    """Exception raised in case a method or database API was used which is not supported by the database,
    e.g. requesting a .rollback() on a connection that does not support transaction or has transactions turned off."""


def Timestamp(year, month, day, hour, minute, second):
    """
    This function constructs an object holding a time stamp value.

    :param year: See Date() and Time() arguments
    :param month:
    :param day:
    :param hour:
    :param minute:
    :param second:
    :return: EtcdTimestamp instance
    """
    return EtcdTimestamp(year, month, day, hour, minute, second)


def DateFromTicks(ticks):
    """
    This function constructs an object holding a time value from the given ticks value
    (number of seconds since the epoch; see the documentation of the standard Python time module for details).

    :param ticks: Seconds since Epoch
    :return: EtcdDate
    """
    return EtcdDate(*time.localtime(ticks)[:3])


def TimeFromTicks(ticks):
    """
    This function constructs an object holding a time value from the given ticks value
    (number of seconds since the epoch; see the documentation of the standard Python time module for details).

    :param ticks: Seconds since Epoch
    :return: EtcdTime
    """
    return EtcdTime(*time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    """This function constructs an object holding a time stamp value from the given ticks value
     (number of seconds since the epoch; see the documentation of the standard Python time module for details).

     :param ticks: Seconds since Epoch
     :return: EtcTimestamp
     """
    return EtcdTimestamp(*time.localtime(ticks)[:6])


def Date(year, month, day):
    """
    This function constructs an object holding a date value.

    :param year: Year, e.g. 2016
    :param month: Month, e.g. 9
    :param day: Day, e.g. 21
    :return: EtcdDate instance
    """
    return EtcdDate(year, month, day)


def Time(hour, minute, second):
    """
    This function constructs an object holding a time value.

    :param hour: Hour, e.g. 15
    :param minute: Minute, e.g. 53
    :param second: Second, e.g. 16
    :return: EtcdTime instance
    """
    return EtcdTime(hour, minute, second)


def Binary(string):
    """This function constructs an object capable of holding a binary (long) string value. """
    return EtcdString(string)


from .connection import Connect
connect = Connect

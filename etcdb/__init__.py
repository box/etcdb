# -*- coding: utf-8 -*-
"""
PEP-249 implementation for etcd
"""
import time

from enum import Enum

from .exception import (Error, Warning, InterfaceError, DatabaseError, DataError,  # pylint: disable=redefined-builtin
                        OperationalError, IntegrityError, InternalError,
                        ProgrammingError, NotSupportedError)

from .etcddate import EtcdDate
from .etcdstring import EtcdString
from .etcdtime import EtcdTime
from .etcdtimestamp import EtcdTimestamp

__author__ = 'Box TechOps Database Team'
__email__ = 'oss@box.com'
__version__ = '1.1.0'


def _split_version(version):
    """
    Generate version tuple from string

    :param version: version string with two dots. E.g. 1.2.3
    :return: tuple with major, minor, patch versions. E.g. (1, 2, 3)
    """
    return tuple(version.split('.'))


VERSION_INFO = _split_version(__version__)

apilevel = "1.0"  # pylint: disable=invalid-name
"""supported DB API level."""
threadsafety = 3  # pylint: disable=invalid-name
"""the level of thread safety. Threads may share the module,
connections and cursors."""
paramstyle = "qmark"  # pylint: disable=invalid-name
"""the type of parameter marker formatting.
Question mark style, e.g. ...WHERE name=?."""

NULL = None
LOCK_WAIT_TIMEOUT = 50
ETCDTABLELOCK = Enum('EtcdTableLock', 'read write')


def Timestamp(year, month, day, hour, minute, second):  # pylint: disable=invalid-name,too-many-arguments
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


def DateFromTicks(ticks):  # pylint: disable=invalid-name
    """
    This function constructs an object holding a time value from the given ticks value
    (number of seconds since the epoch; see the documentation of the standard Python time module for details).

    :param ticks: Seconds since Epoch
    :return: EtcdDate
    """
    return EtcdDate(*time.localtime(ticks)[:3])


def TimeFromTicks(ticks):  # pylint: disable=invalid-name
    """
    This function constructs an object holding a time value from the given ticks value
    (number of seconds since the epoch; see the documentation of the standard Python time module for details).

    :param ticks: Seconds since Epoch
    :return: EtcdTime
    """
    return EtcdTime(*time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks):  # pylint: disable=invalid-name
    """This function constructs an object holding a time stamp value from the given ticks value
     (number of seconds since the epoch; see the documentation of the standard Python time module for details).

     :param ticks: Seconds since Epoch
     :return: EtcTimestamp
     """
    return EtcdTimestamp(*time.localtime(ticks)[:6])


def Date(year, month, day):  # pylint: disable=invalid-name
    """
    This function constructs an object holding a date value.

    :param year: Year, e.g. 2016
    :param month: Month, e.g. 9
    :param day: Day, e.g. 21
    :return: EtcdDate instance
    """
    return EtcdDate(year, month, day)


def Time(hour, minute, second):  # pylint: disable=invalid-name
    """
    This function constructs an object holding a time value.

    :param hour: Hour, e.g. 15
    :param minute: Minute, e.g. 53
    :param second: Second, e.g. 16
    :return: EtcdTime instance
    """
    return EtcdTime(hour, minute, second)


def Binary(string):  # pylint: disable=invalid-name
    """This function constructs an object capable of holding a binary (long) string value. """
    return EtcdString(string)

from .connection import Connect  # pylint: disable=wrong-import-position
connect = Connect  # pylint: disable=invalid-name

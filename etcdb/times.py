from datetime import date, time, timedelta, datetime
from time import localtime

Date = date
Time = time
TimeDelta = timedelta
Timestamp = datetime

DateTimeDeltaType = timedelta
DateTimeType = datetime


def DateFromTicks(ticks):
    """Convert UNIX ticks into a date instance."""
    return date(*localtime(ticks)[:3])


def TimeFromTicks(ticks):
    """Convert UNIX ticks into a time instance."""
    return time(*localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    """Convert UNIX ticks into a datetime instance."""
    return datetime(*localtime(ticks)[:6])

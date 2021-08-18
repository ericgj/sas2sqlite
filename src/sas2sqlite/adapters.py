from calendar import timegm
from datetime import date, datetime, time
import sqlite3
from typing import Callable

import julian  # type: ignore


def store_time(time_type: str, time_format: str = "") -> None:
    if time_type == "seconds":
        sqlite3.register_adapter(time, time_to_seconds)
    elif time_type == "text":
        sqlite3.register_adapter(time, time_to_text(time_format))
    else:
        raise ValueError(f"Unknown time adapter: '{time_type}'")


def store_date(date_type: str, date_format: str = "") -> None:
    if date_type == "julian":
        sqlite3.register_adapter(date, date_to_julian)
    elif date_type == "posix":
        sqlite3.register_adapter(date, date_to_posix)
    elif date_type == "text":
        sqlite3.register_adapter(date, date_to_text(date_format))
    else:
        raise ValueError(f"Unknown date adapter: '{date_type}'")


def store_datetime(datetime_type: str, datetime_format: str = "") -> None:
    if datetime_type == "julian":
        sqlite3.register_adapter(datetime, datetime_to_julian)
    elif datetime_type == "posix":
        sqlite3.register_adapter(datetime, datetime_to_posix)
    elif datetime_type == "text":
        sqlite3.register_adapter(datetime, datetime_to_text(datetime_format))
    else:
        raise ValueError(f"Unknown datetime adapter: '{datetime_type}'")


def time_to_seconds(t: time) -> float:
    return (60 * 60 * t.hour) + (60 * t.minute) + t.second + t.microsecond


def time_to_text(format: str) -> Callable[[time], str]:
    def _time_to_text(t: time) -> str:
        return t.strftime(format)

    return _time_to_text


def date_to_posix(d: date) -> int:
    return datetime_to_posix(datetime(d.year, d.month, d.day))


def date_to_julian(d: date) -> float:
    return datetime_to_julian(datetime(d.year, d.month, d.day))


def date_to_text(format: str) -> Callable[[date], str]:
    def _date_to_text(d: date) -> str:
        return d.strftime(format)

    return _date_to_text


def datetime_to_posix(dt: datetime) -> int:
    return timegm(dt.utctimetuple())


def datetime_to_julian(dt: datetime) -> float:
    return float(julian.to_jd(dt))


def datetime_to_text(format: str) -> Callable[[datetime], str]:
    def _datetime_to_text(dt: datetime) -> str:
        return dt.strftime(format)

    return _datetime_to_text

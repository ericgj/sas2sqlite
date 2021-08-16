__version__ = "0.0.1"

from calendar import timegm
from datetime import date, datetime, time
import re
import sqlite3
from typing import Any, Optional, Iterable, Tuple, List, Callable

import julian  # type: ignore
from sas7bdat import SAS7BDAT  # type: ignore

SAS_TIME_FORMATS = re.compile(
    "(?:%s)"
    % "|".join(
        [
            r"hhmm\d*",
            r"hhmm\d+\.\d+",
            r"hour\d*",
            r"hour\d+\.\d+",
            r"mmss\d*",
            r"mmss\d+\.\d+",
            r"time\d*",
            r"time\d+\.\d+",
            r"timeampm\d*",
            r"timeampm\d+\.\d+",
            r"tod\d*",
            r"tod\d+\.\d+",
        ]
    )
)

SAS_DATE_FORMATS = re.compile(
    "(?:%s)"
    % "|".join(
        [
            r"date\d*",
            r"day\d*",
            r"ddmmyy\d*",
            r"ddmmyy\w\d*",
            r"julday\d*",
            r"julian\d*",
            r"monname\d*",
            r"month\d*",
            r"monyy\d*",
            r"mmddyy\d*",
            r"mmddyy\w\d*",
            r"mmyy\d*",
            r"mmyy\w\d*",
            r"qtr\d*",
            r"qtrr\d*",
            r"weekdate\d*",
            r"weekdatx\d*",
            r"weekday\d*",
            r"year\d*",
            r"yymm\d*",
            r"yymm\w\d*",
            r"yymmdd\d*",
            r"yymmdd\w\d*",
            r"yymon\d*",
            r"yyq\d*",
            r"yyq\w\d*",
            r"yyqr\d*",
            r"yyqr\w\d*",
        ]
    )
)

SAS_DATETIME_FORMATS = re.compile(
    "(?:%s)"
    % "|".join(
        [
            r"dateampm\d*",
            r"dateampm\d+\.\d+",
            r"datetime\d*",
            r"datetime\d+\.\d+",
            r"dtdate\d*",
            r"dtmonyy\d*",
            r"dtwkdatx\d*",
            r"dtyear\d*",
            r"dtyyqc\d*",
            r"mdyampm\d*",
            r"mdyampm\d+\.\d+",
            r"timeampm\d*",
            r"timeampm\d+\.\d+",
            r"tod\d*",
            r"tod\d+\.\d+",
        ]
    )
)


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


def store_time(time_type: str, time_format: str) -> None:
    if time_type == "seconds":
        sqlite3.register_adapter(time, time_to_seconds)
    elif time_type == "text":
        sqlite3.register_adapter(time, time_to_text(time_format))
    else:
        raise ValueError(f"Unknown time adapter: '{time_type}'")


def store_date(date_type: str, date_format: str) -> None:
    if date_type == "julian":
        sqlite3.register_adapter(date, date_to_julian)
    elif date_type == "posix":
        sqlite3.register_adapter(date, date_to_posix)
    elif date_type == "text":
        sqlite3.register_adapter(date, date_to_text(date_format))
    else:
        raise ValueError(f"Unknown date adapter: '{date_type}'")


def store_datetime(datetime_type: str, datetime_format: str) -> None:
    if datetime_type == "julian":
        sqlite3.register_adapter(datetime, datetime_to_julian)
    elif datetime_type == "posix":
        sqlite3.register_adapter(datetime, datetime_to_posix)
    elif datetime_type == "text":
        sqlite3.register_adapter(datetime, datetime_to_text(datetime_format))
    else:
        raise ValueError(f"Unknown datetime adapter: '{datetime_type}'")


def import_dataset(
    conn: sqlite3.Connection,
    dataset_file: str,
    *,
    table: Optional[str] = None,
    schema: Optional[str] = None,
    create_table: bool = False,
    store_time_as: str = "text",
    store_time_format: str = "%H:%M:%S",
    store_date_as: str = "text",
    store_date_format: str = "%Y-%m-%d",
    store_datetime_as: str = "text",
    store_datetime_format: str = "%Y-%m-%dT%H:%M:%S",
):
    saved_adapters = sqlite3.adapters.copy()
    store_time(store_time_as, store_time_format)
    store_date(store_date_as, store_date_format)
    store_datetime(store_datetime_as, store_datetime_format)

    try:
        with SAS7BDAT(dataset_file, skip_header=True) as r:
            cols = r.columns
            if table is None:
                table = r.header.properties.name.decode("utf-8")
            with conn:
                if create_table:
                    conn.execute(drop_table_sql(table, schema))
                    conn.execute(
                        create_table_sql(
                            table,
                            cols,
                            schema=schema,
                            store_time_as=store_time_as,
                            store_date_as=store_date_as,
                            store_datetime_as=store_datetime_as,
                        )
                    )

            with conn:
                for row in r:
                    conn.execute(*insert_sql(table, cols, row, schema))
    finally:
        sqlite3.adapters = saved_adapters


def drop_table_sql(table: str, schema: Optional[str] = None) -> str:
    if schema is None:
        return f"DROP TABLE IF EXISTS `{table}`;"
    else:
        return f"DROP TABLE IF EXISTS `{schema}`.`{table}`;"


def create_table_sql(
    table: str,
    cols: Iterable[Any],
    store_time_as: str,
    store_date_as: str,
    store_datetime_as: str,
    schema: Optional[str] = None,
) -> str:
    col_lines: List[str] = []
    for col in cols:
        if col.type.lower() == "string":
            col_lines.append(f"`{col.name.decode('utf-8')}` VARCHAR({col.length})")
        elif col.type.lower() == "number":
            format = None if col.format is None else col.format.lower()
            if not format is None and (
                (re.match(SAS_TIME_FORMATS, format) and store_time_as == "text")
                or (re.match(SAS_DATE_FORMATS, format) and store_date_as == "text")
                or (
                    re.match(SAS_DATETIME_FORMATS, format)
                    and store_datetime_as == "text"
                )
            ):
                col_lines.append(f"`{col.name.decode('utf-8')}` TEXT")
            else:
                col_lines.append(f"`{col.name.decode('utf-8')}` NUMERIC")
        else:
            raise ValueError(
                f"Unknown column type '{col.type}': column {col.name.decode('utf-8')}"
            )

    col_defs = ", ".join(col_lines)
    if schema is None:
        return f"CREATE TABLE `{table}` ({col_defs});"
    else:
        return f"CREATE TABLE `{schema}`.`{table}` ({col_defs});"


def insert_sql(
    table: str, cols: Iterable[Any], row: Iterable[Any], schema: Optional[str] = None
) -> Tuple[str, Iterable[Any]]:
    col_expr = ",".join([f"`{col.name.decode('utf-8')}`" for col in cols])
    val_expr = ",".join(["?" for col in cols])
    if schema is None:
        return (f"INSERT INTO `{table}` ({col_expr}) VALUES ({val_expr});", row)
    else:
        return (
            f"INSERT INTO `{schema}`.`{table}` ({col_expr}) VALUES ({val_expr});",
            row,
        )

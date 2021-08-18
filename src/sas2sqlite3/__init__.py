__version__ = "0.0.2"

import sqlite3
from typing import Any, Optional, Iterable, Tuple, List

from sas7bdat import SAS7BDAT  # type: ignore


def import_dataset(
    conn: sqlite3.Connection,
    dataset: SAS7BDAT,
    *,
    table: Optional[str] = None,
    create_table: bool = False,
    sqlite_time_type: str = "TEXT",
    sqlite_date_type: str = "TEXT",
    sqlite_datetime_type: str = "TEXT",
):
    saved_skip_header = dataset.skip_header
    dataset.skip_header = True  # always skip header row for our purposes

    try:
        with dataset as r:
            cols = r.columns
            if table is None:
                table = r.header.properties.name.decode("utf-8")
            with conn:
                if create_table:
                    conn.execute(drop_table_sql(table))
                    conn.execute(
                        create_table_sql(
                            table,
                            cols,
                            encoding=r.encoding,
                            sas_time_formats=r.TIME_FORMAT_STRINGS,
                            sas_date_formats=r.DATE_FORMAT_STRINGS,
                            sas_datetime_formats=r.DATE_TIME_FORMAT_STRINGS,
                            sqlite_time_type=sqlite_time_type,
                            sqlite_date_type=sqlite_date_type,
                            sqlite_datetime_type=sqlite_datetime_type,
                        )
                    )

            with conn:
                for row in r:
                    conn.execute(
                        *insert_sql(
                            table,
                            cols,
                            row,
                            encoding=r.encoding,
                        )
                    )
    finally:
        dataset.skip_header = saved_skip_header


def drop_table_sql(table: str) -> str:
    return f"DROP TABLE IF EXISTS `{table}`;"


def create_table_sql(
    table: str,
    cols: Iterable[Any],
    *,
    encoding: str,
    sas_time_formats: Iterable[str],
    sas_date_formats: Iterable[str],
    sas_datetime_formats: Iterable[str],
    sqlite_time_type: str,
    sqlite_date_type: str,
    sqlite_datetime_type: str,
) -> str:
    col_lines: List[str] = []
    for col in cols:
        if col.type.lower() == "string":
            col_lines.append(f"`{col.name.decode(encoding)}` VARCHAR({col.length})")
        elif col.type.lower() == "number":
            format = None if col.format is None else col.format.upper()
            if format is not None and format in sas_time_formats:
                col_lines.append(f"`{col.name.decode(encoding)}` {sqlite_time_type}")
            elif format is not None and format in sas_date_formats:
                col_lines.append(f"`{col.name.decode(encoding)}` {sqlite_date_type}")
            elif format is not None and format in sas_datetime_formats:
                col_lines.append(
                    f"`{col.name.decode(encoding)}` {sqlite_datetime_type}"
                )
            else:
                col_lines.append(f"`{col.name.decode(encoding)}` NUMERIC")
        else:
            raise ValueError(
                f"Unknown column type '{col.type}': column {col.name.decode(encoding)}"
            )

    col_defs = ", ".join(col_lines)
    return f"CREATE TABLE `{table}` ({col_defs});"


def insert_sql(
    table: str,
    cols: Iterable[Any],
    row: Iterable[Any],
    *,
    encoding: str,
) -> Tuple[str, Iterable[Any]]:
    col_expr = ",".join([f"`{col.name.decode(encoding)}`" for col in cols])
    val_expr = ",".join(["?" for col in cols])
    return (f"INSERT INTO `{table}` ({col_expr}) VALUES ({val_expr});", row)

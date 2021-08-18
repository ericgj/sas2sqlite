from argparse import ArgumentParser
import sqlite3
import sys
from typing import Tuple

from sas7bdat import SAS7BDAT  # type: ignore
from . import import_dataset
from . import adapters


def main(argv=sys.argv):
    cmd = ArgumentParser(description="Import sas7bdat files to sqlite3 dbase")
    cmd.add_argument("sas_file", help="source SAS file")
    cmd.add_argument("db_file", help="target sqlite3 database file")
    cmd.add_argument("-t", "--table", help="table name (defaults to SAS dataset name)")
    cmd.add_argument("--create-table", action="store_true", help="create table")
    cmd.add_argument(
        "--no-create-table",
        action="store_false",
        dest="create_table",
        help="do not create table",
    )
    cmd.add_argument(
        "--time",
        type=parse_time_args,
        default="%H:%M:%S",
        help='store time as "seconds", or strftime pattern',
    )
    cmd.add_argument(
        "--date",
        type=parse_date_args,
        default="%Y-%m-%d",
        help='store date as "julian", "posix", or strftime pattern',
    )
    cmd.add_argument(
        "--datetime",
        type=parse_datetime_args,
        default="%Y-%m-%dT%H:%M:%S",
        help='store datetime as "julian", "posix", or strftime pattern',
    )

    cmd.set_defaults(create_table=True)

    args = cmd.parse_args(argv[1:])

    adapters.store_time(*args.time)
    adapters.store_date(*args.date)
    adapters.store_datetime(*args.datetime)

    conn = sqlite3.connect(args.db_file)
    dataset = SAS7BDAT(args.sas_file, skip_header=True)

    import_dataset(
        conn,
        dataset,
        create_table=args.create_table,
        sqlite_time_type=sqlite_time_type(args.time[0]),
        sqlite_date_type=sqlite_date_type(args.date[0]),
        sqlite_datetime_type=sqlite_datetime_type(args.datetime[0]),
    )


def parse_time_args(s: str) -> Tuple[str, str]:
    if s == "seconds":
        return ("seconds", "")
    else:
        return ("text", s)


def parse_date_args(s: str) -> Tuple[str, str]:
    if s == "julian":
        return ("julian", "")
    elif s == "posix":
        return ("posix", "")
    else:
        return ("text", s)


def parse_datetime_args(s: str) -> Tuple[str, str]:
    if s == "julian":
        return ("julian", "")
    elif s == "posix":
        return ("posix", "")
    else:
        return ("text", s)


def sqlite_time_type(s: str) -> str:
    if s == "seconds":
        return "NUMERIC"
    if s == "text":
        return "TEXT"
    raise ValueError(f"No sqlite time adapter found for '{s}'")


def sqlite_date_type(s: str) -> str:
    if s == "julian":
        return "NUMERIC"
    if s == "posix":
        return "NUMERIC"
    if s == "text":
        return "TEXT"
    raise ValueError(f"No sqlite date adapter found for '{s}'")


def sqlite_datetime_type(s: str) -> str:
    if s == "julian":
        return "NUMERIC"
    if s == "posix":
        return "NUMERIC"
    if s == "text":
        return "TEXT"
    raise ValueError(f"No sqlite datetime adapter found for '{s}'")


if __name__ == "__main__":
    main()

import json
import os.path
import os
import re
import sqlite3
from typing import Any, Optional, List, Dict

from sas7bdat import SAS7BDAT  # type: ignore
import sas2sqlite

TEST_DIR = os.path.dirname(__file__)


def sas_fixtures(dir="fixtures"):
    for fname in os.listdir(os.path.join(TEST_DIR, dir)):
        _, ext = os.path.splitext(fname)
        if ext.lower() == ".sas7bdat":
            yield os.path.join(TEST_DIR, dir, fname)


def expected_fixtures(dir="fixtures"):
    for fname in os.listdir(os.path.join(TEST_DIR, dir)):
        _, ext = os.path.splitext(fname)
        if ext.lower() == ".json":
            exp = {}
            with open(os.path.join(TEST_DIR, dir, fname), "r") as f:
                exp = json.load(f)
            yield exp


def test_default():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    for sasfile in sas_fixtures("fixtures/default"):
        dataset = SAS7BDAT(sasfile)
        sas2sqlite.import_dataset(conn, dataset, create_table=True)

    for exp in expected_fixtures("fixtures/default"):
        assert_rows(
            conn,
            table=str(exp["table"]),
            exp_rows=list(exp["rows"]),
            schema=exp.get("schema"),
            desc=exp.get("desc"),
        )

        assert_col_types(
            conn,
            table=str(exp["table"]),
            exp_cols=list(exp["cols"]),
            schema=exp.get("schema"),
            desc=exp.get("desc"),
        )


def test_julian():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    for sasfile in sas_fixtures("fixtures/julian"):
        dataset = SAS7BDAT(sasfile)
        sas2sqlite.import_dataset(
            conn,
            dataset,
            create_table=True,
            store_date_as="julian",
            store_datetime_as="julian",
        )

    for exp in expected_fixtures("fixtures/julian"):
        assert_rows(
            conn,
            table=str(exp["table"]),
            exp_rows=list(exp["rows"]),
            schema=exp.get("schema"),
            desc=exp.get("desc"),
        )

        assert_col_types(
            conn,
            table=str(exp["table"]),
            exp_cols=list(exp["cols"]),
            schema=exp.get("schema"),
            desc=exp.get("desc"),
        )


def test_posix():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    for sasfile in sas_fixtures("fixtures/posix"):
        dataset = SAS7BDAT(sasfile)
        sas2sqlite.import_dataset(
            conn,
            dataset,
            create_table=True,
            store_date_as="posix",
            store_datetime_as="posix",
        )

    for exp in expected_fixtures("fixtures/posix"):
        assert_rows(
            conn,
            table=str(exp["table"]),
            exp_rows=list(exp["rows"]),
            schema=exp.get("schema"),
            desc=exp.get("desc"),
        )

        assert_col_types(
            conn,
            table=str(exp["table"]),
            exp_cols=list(exp["cols"]),
            schema=exp.get("schema"),
            desc=exp.get("desc"),
        )


def test_seconds_from_midnight():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    for sasfile in sas_fixtures("fixtures/seconds"):
        dataset = SAS7BDAT(sasfile)
        sas2sqlite.import_dataset(
            conn,
            dataset,
            create_table=True,
            store_time_as="seconds",
        )

    for exp in expected_fixtures("fixtures/seconds"):
        assert_rows(
            conn,
            table=str(exp["table"]),
            exp_rows=list(exp["rows"]),
            schema=exp.get("schema"),
            desc=exp.get("desc"),
        )

        assert_col_types(
            conn,
            table=str(exp["table"]),
            exp_cols=list(exp["cols"]),
            schema=exp.get("schema"),
            desc=exp.get("desc"),
        )


def assert_rows(
    conn: sqlite3.Connection,
    table: str,
    exp_rows: List[Any],
    schema: Optional[str] = None,
    desc: Optional[str] = None,
):
    if desc is None:
        desc = table
    if schema is None:
        c = conn.execute(f"SELECT * FROM `{table}` ORDER BY ROWID;")
    else:
        c = conn.execute(f"SELECT * FROM `{schema}`.`{table}` ORDER BY ROWID;")
    act_rows = c.fetchall()
    assert len(act_rows) == len(exp_rows)
    for (i, (act, exp)) in enumerate(zip(act_rows, exp_rows)):
        for k in exp:
            a = act[k]
            e = exp[k]
            assert (
                a == e
            ), f"row {i+1}, column {k}: expected {repr(e)}, was {repr(a)} ({desc})"


def assert_col_types(
    conn: sqlite3.Connection,
    table: str,
    exp_cols: List[Dict[str, str]],
    schema: Optional[str] = None,
    desc: Optional[str] = None,
):
    if desc is None:
        desc = table

    # Note: I don't know how to get schema info from a foreign schema (attached db)
    # So schema is ignored

    c = conn.execute(
        "SELECT sql FROM sqlite_master WHERE LOWER(`name`) = ? LIMIT 1",
        (table.lower(),),
    )
    r = c.fetchone()
    assert r is not None, f"Did not find table '{table}'"
    sql = r[0]
    for (i, exp) in enumerate(exp_cols):
        for k in exp:
            exp_type = exp[k]
            assert matches_col_type(
                k, exp_type, sql
            ), f"row {i+1}, column {k}: expected '{exp_type}' ({desc})"


def matches_col_type(col_name, exp_type, sql):
    return re.search(f"`{col_name}` {exp_type}", sql) is not None

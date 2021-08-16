import json
import os.path
import os
import sqlite3
from typing import Any, Optional, List

import sas2sqlite  # type: ignore

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
        sas2sqlite.import_dataset(conn, sasfile, create_table=True)

    for exp in expected_fixtures("fixtures/default"):
        assert_rows(
            conn,
            table=str(exp["table"]),
            exp_rows=list(exp["rows"]),
            schema=exp.get("schema"),
            desc=exp.get("desc"),
        )

        # TODO: assert sqlite3 column types


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

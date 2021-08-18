import json
import os.path
import os
import re
import shutil
import sqlite3
import subprocess
from typing import Any, Optional, List, Dict

TEST_DIR = os.path.dirname(__file__)
OUTPUT_DIR = os.path.join(TEST_DIR, "output")


def test_default():
    _run_test("default", [])


def test_julian():
    _run_test("julian", ["--date", "julian", "--datetime", "julian"])


def test_posix():
    _run_test("posix", ["--date", "posix", "--datetime", "posix"])


def test_seconds_from_midnight():
    _run_test("seconds", ["--time", "seconds"])


def _run_test(name, args):
    init_output(name)
    dbfile = output_db_file(name)
    for sasfile in sas_fixtures(f"fixtures/{name}"):
        run_command([sasfile, dbfile] + args)

    conn = sqlite3.connect(dbfile)
    conn.row_factory = sqlite3.Row

    for exp in expected_fixtures(f"fixtures/{name}"):
        assert_rows(
            conn,
            table=str(exp["table"]),
            exp_rows=list(exp["rows"]),
            desc=exp.get("desc"),
        )

        assert_col_types(
            conn,
            table=str(exp["table"]),
            exp_cols=list(exp["cols"]),
            desc=exp.get("desc"),
        )


def init_output(dir):
    output_dir = os.path.join(OUTPUT_DIR, dir)
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)


def output_db_file(dir, name="test.db"):
    return os.path.join(OUTPUT_DIR, dir, name)


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


def run_command(args):
    subprocess.run(["sas2sqlite"] + args, check=True)


def assert_rows(
    conn: sqlite3.Connection,
    table: str,
    exp_rows: List[Any],
    desc: Optional[str] = None,
):
    if desc is None:
        desc = table
    c = conn.execute(f"SELECT * FROM `{table}` ORDER BY ROWID;")
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
    desc: Optional[str] = None,
):
    if desc is None:
        desc = table

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

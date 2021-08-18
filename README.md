# sas2sqlite3

Import sas7bdat files to sqlite3 dbase. Uses the [sas7bdat][sas7bdat] SAS 
dataset file parser.

Command-line and python library interface with minimal requirements.


## CLI usage

Create and import data into `some_dataset` table in `some_sqlite.db`

```sh
sas2sqlite some_dataset.sas7bdat some_sqlite.db
```

Import data into existing `some_table` table in `some_sqlite.db`

```sh
sas2sqlite some_dataset.sas7bdat some_sqlite.db --table some_table --no-create-table
```

Import data storing dates and datetimes as Julian days, and times as `HH:MM`:

```sh
sas2sqlite some_dataset.sas7bdat some_sqlite.db \
    --time "%H:%M" --date julian --datetime julian
```


## Install

_Requires python 3.6+ and pip._

```sh
pip install sas2sqlite3
```

_Note: the package has a '3' at the end. The executable does not._


## Other tools

- [sas2db][sas2db]: Import SAS dataset and XPT files to any database supported
  by [SQL Alchemy][SQLAlchemy]. Uses [pandas][pandas] SAS parser under the hood.


[sas7bdat]: https://pypi.org/project/sas7bdat
[sas2db]: https://pypi.org/project/sas2db
[SQLAlchemy]: https://www.sqlalchemy.org
[pandas]: https://pandas.pydata.org


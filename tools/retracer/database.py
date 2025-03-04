# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)


def create_database(file: Path, force: bool) -> sqlite3.Connection:
    if file.is_file():
        logger.debug(f"Database file already exists {file}")
        if force:
            logger.debug(f"Deleting file {file}")
            file.unlink()
        else:
            raise Exception(
                "Refusing to delete existing database, use -f/--force to overwrite"
            )
    if file.is_dir():
        raise Exception("Database path is a directory, aborting.")

    logger.debug(f"Creating database file {file}")
    con = sqlite3.connect(file)
    with con:
        con.execute("drop table if exists syscall_events")
        con.execute(
            """
            create table syscall_events(
                id integer primary key autoincrement,
                pid integer,
                unix_time real,
                syscall text,
                args text,
                return_code integer,
                duration real
            )
            """
        )
        con.execute(
            """
            create index syscall_events_syscall_pid_time_idx on syscall_events(
                syscall, pid, unix_time
            );
            """
        )
        con.execute(
            """
             create index syscall_events_time_idx on syscall_events(unix_time);
            """
        )
    return con


def open_database(file: Path) -> sqlite3.Connection:
    if not file.is_file():
        raise Exception(f"Database not found at {file}.")
    con = sqlite3.connect(file)
    try:
        con.execute("select 1 from syscall_events limit 0")
        # This should probably also use 'pragma info_table(syscall_events) to validate the schema
    except:
        raise Exception(
            "Table 'syscall_events' is missing. Did you forget to run parse-strace command first?"
        )
    return con

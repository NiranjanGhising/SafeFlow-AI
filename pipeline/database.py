"""SQLite connection and transaction utilities for SafeFlow AI."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from collections.abc import Generator

from pipeline.config import get_database_path


def create_connection() -> sqlite3.Connection:
    """Create and configure a SQLite database connection."""

    database_path = get_database_path()

    connection = sqlite3.connect(
        database=database_path,
        timeout=30,
    )

    connection.row_factory = sqlite3.Row

    connection.execute(
        "PRAGMA foreign_keys = ON;"
    )

    connection.execute(
        "PRAGMA journal_mode = WAL;"
    )

    connection.execute(
        "PRAGMA busy_timeout = 30000;"
    )

    return connection


@contextmanager
def database_transaction() -> Generator[
    sqlite3.Connection,
    None,
    None,
]:
    """Provide a transaction that commits or rolls back automatically."""

    connection = create_connection()

    try:
        connection.execute("BEGIN;")

        yield connection

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()
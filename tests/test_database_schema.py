"""Tests for the SafeFlow AI analytical database schema."""

from __future__ import annotations

import sqlite3
import unittest
import uuid
from datetime import datetime, timezone

from pipeline.database import create_connection
from pipeline.schema import (
    get_expected_tables,
    initialize_database,
)


class DatabaseSchemaTests(unittest.TestCase):
    """Verify database tables, constraints, and relationships."""

    @classmethod
    def setUpClass(cls) -> None:
        """Initialize the database schema once before all tests."""

        initialize_database()

    def setUp(self) -> None:
        """Create a fresh database connection before each test."""

        self.connection = create_connection()

    def tearDown(self) -> None:
        """Roll back test changes and close the connection."""

        self.connection.rollback()
        self.connection.close()

    def test_all_expected_tables_exist(self) -> None:
        """Verify that all required SafeFlow AI tables exist."""

        rows = self.connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name NOT LIKE 'sqlite_%';
            """
        ).fetchall()

        actual_tables = {row["name"] for row in rows}

        self.assertTrue(
            get_expected_tables().issubset(actual_tables)
        )

    def test_foreign_keys_are_enabled(self) -> None:
        """Verify that SQLite foreign-key enforcement is enabled."""

        foreign_keys_enabled = self.connection.execute(
            "PRAGMA foreign_keys;"
        ).fetchone()[0]

        self.assertEqual(foreign_keys_enabled, 1)

    def test_fact_table_has_required_foreign_keys(self) -> None:
        """Verify the fact table's required relationships."""

        rows = self.connection.execute(
            "PRAGMA foreign_key_list(fact_safety_event);"
        ).fetchall()

        relationships = {
            (
                row["from"],
                row["table"],
                row["to"],
            )
            for row in rows
        }

        expected_relationships = {
            (
                "pipeline_run_id",
                "pipeline_audit",
                "pipeline_run_id",
            ),
            (
                "date_key",
                "dim_date",
                "date_key",
            ),
            (
                "category_key",
                "dim_category",
                "category_key",
            ),
            (
                "action_key",
                "dim_action",
                "action_key",
            ),
        }

        self.assertTrue(
            expected_relationships.issubset(relationships)
        )

    def test_invalid_pipeline_status_is_rejected(self) -> None:
        """Verify that an unsupported pipeline status is rejected."""

        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                """
                INSERT INTO pipeline_audit (
                    pipeline_run_id,
                    start_time,
                    status
                )
                VALUES (?, ?, ?);
                """,
                (
                    str(uuid.uuid4()),
                    datetime.now(timezone.utc).isoformat(),
                    "UNKNOWN",
                ),
            )

    def test_negative_source_count_is_rejected(self) -> None:
        """Verify that pipeline counts cannot be negative."""

        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                """
                INSERT INTO pipeline_audit (
                    pipeline_run_id,
                    start_time,
                    source_count,
                    status
                )
                VALUES (?, ?, ?, ?);
                """,
                (
                    str(uuid.uuid4()),
                    datetime.now(timezone.utc).isoformat(),
                    -1,
                    "RUNNING",
                ),
            )

    def test_invalid_action_is_rejected(self) -> None:
        """Verify that unsupported safety actions are rejected."""

        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                """
                INSERT INTO dim_action (
                    action_name,
                    severity_rank,
                    risk_level
                )
                VALUES (?, ?, ?);
                """,
                (
                    "delete",
                    4,
                    "Critical",
                ),
            )

    def test_invalid_action_severity_rank_is_rejected(self) -> None:
        """Verify that action severity ranks must be between 1 and 4."""

        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                """
                INSERT INTO dim_action (
                    action_name,
                    severity_rank,
                    risk_level
                )
                VALUES (?, ?, ?);
                """,
                (
                    "allow",
                    8,
                    "Low",
                ),
            )

    def test_staging_requires_existing_pipeline_run(self) -> None:
        """Verify that staging rows require a valid pipeline run."""

        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                """
                INSERT INTO stg_safety_events (
                    pipeline_run_id,
                    request_id,
                    source_position,
                    extracted_at,
                    raw_record
                )
                VALUES (?, ?, ?, ?, ?);
                """,
                (
                    "nonexistent-run",
                    str(uuid.uuid4()),
                    0,
                    datetime.now(timezone.utc).isoformat(),
                    "{}",
                ),
            )

    def test_duplicate_request_in_same_run_is_rejected(self) -> None:
        """Verify uniqueness of request IDs within one pipeline run."""

        pipeline_run_id = str(uuid.uuid4())
        request_id = str(uuid.uuid4())
        current_time = datetime.now(timezone.utc).isoformat()

        self.connection.execute(
            """
            INSERT INTO pipeline_audit (
                pipeline_run_id,
                start_time,
                status
            )
            VALUES (?, ?, ?);
            """,
            (
                pipeline_run_id,
                current_time,
                "RUNNING",
            ),
        )

        staging_values = (
            pipeline_run_id,
            request_id,
            0,
            current_time,
            "{}",
        )

        self.connection.execute(
            """
            INSERT INTO stg_safety_events (
                pipeline_run_id,
                request_id,
                source_position,
                extracted_at,
                raw_record
            )
            VALUES (?, ?, ?, ?, ?);
            """,
            staging_values,
        )

        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                """
                INSERT INTO stg_safety_events (
                    pipeline_run_id,
                    request_id,
                    source_position,
                    extracted_at,
                    raw_record
                )
                VALUES (?, ?, ?, ?, ?);
                """,
                staging_values,
            )

    def test_duplicate_fact_request_id_is_rejected(self) -> None:
        """Verify that one request cannot appear twice in the fact table."""

        pipeline_run_id = str(uuid.uuid4())
        request_id = str(uuid.uuid4())
        current_time = datetime.now(timezone.utc).isoformat()

        self.connection.execute(
            """
            INSERT INTO pipeline_audit (
                pipeline_run_id,
                start_time,
                status
            )
            VALUES (?, ?, ?);
            """,
            (
                pipeline_run_id,
                current_time,
                "RUNNING",
            ),
        )

        self.connection.execute(
            """
            INSERT INTO dim_date (
                date_key,
                full_date,
                day,
                month,
                month_name,
                quarter,
                year,
                day_name
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                20261007,
                "2026-10-07",
                7,
                10,
                "October",
                4,
                2026,
                "Wednesday",
            ),
        )

        category_cursor = self.connection.execute(
            """
            INSERT INTO dim_category (
                category_name,
                category_description,
                is_attack_category
            )
            VALUES (?, ?, ?);
            """,
            (
                f"test-category-{uuid.uuid4()}",
                "Temporary test category",
                1,
            ),
        )

        category_key = category_cursor.lastrowid

        action_row = self.connection.execute(
            """
            SELECT action_key
            FROM dim_action
            WHERE action_name = ?;
            """,
            (
                "allow",
            ),
        ).fetchone()

        self.assertIsNotNone(action_row,msg="The seeded 'allow' action was not found.",)

        action_key = action_row["action_key"]

        fact_values = (
            request_id,
            pipeline_run_id,
            20261007,
            category_key,
            action_key,
            "attack",
            "fixture-test",
            "Test prompt",
            "Test output",
            "test-category",
            0.5,
            0,
            0,
            1,
            1,
            1,
            0,
            0,
            0,
            1.0,
            "Fast",
            current_time,
            current_time,
        )

        insert_fact_sql = """
            INSERT INTO fact_safety_event (
                request_id,
                pipeline_run_id,
                date_key,
                category_key,
                action_key,
                corpus,
                fixture_id,
                prompt,
                final_output,
                pre_gen_category,
                pre_gen_confidence,
                pre_gen_blocked,
                terminated_early,
                post_gen_checked,
                is_attack,
                was_allowed,
                was_warned,
                was_redacted,
                was_blocked,
                latency_ms,
                latency_band,
                event_timestamp,
                loaded_at
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            );
        """

        self.connection.execute(
            insert_fact_sql,
            fact_values,
        )

        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                insert_fact_sql,
                fact_values,
            )


if __name__ == "__main__":
    unittest.main()
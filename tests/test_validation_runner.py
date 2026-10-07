"""Tests for staged validation and quarantine handling."""

from __future__ import annotations

import json
import unittest
import uuid
from copy import deepcopy

from pipeline.audit import start_pipeline_run
from pipeline.database import create_connection, database_transaction
from pipeline.extract import extract_source_events
from pipeline.seed import seed_reference_data
from pipeline.staging import stage_events
from pipeline.validation_runner import validate_staged_run


class ValidationRunnerTests(unittest.TestCase):
    """Verify VALID and REJECTED staging outcomes."""

    @classmethod
    def setUpClass(cls) -> None:
        """Initialize database tables and reference data."""

        seed_reference_data()
        extracted = extract_source_events()

        cls.valid_template = {
            key: value
            for key, value in extracted.events[0].items()
            if key != "_source_position"
        }

    def setUp(self) -> None:
        """Create one isolated pipeline run for each test."""

        self.pipeline_run = start_pipeline_run()

    def tearDown(self) -> None:
        """Remove records created by the current test."""

        with database_transaction() as connection:
            connection.execute(
                """
                DELETE FROM rejected_safety_events
                WHERE pipeline_run_id = ?;
                """,
                (self.pipeline_run.pipeline_run_id,),
            )

            connection.execute(
                """
                DELETE FROM stg_safety_events
                WHERE pipeline_run_id = ?;
                """,
                (self.pipeline_run.pipeline_run_id,),
            )

            connection.execute(
                """
                DELETE FROM pipeline_audit
                WHERE pipeline_run_id = ?;
                """,
                (self.pipeline_run.pipeline_run_id,),
            )

    def build_event(
        self,
        *,
        source_position: int,
    ) -> dict[str, object]:
        """Create a valid event with a unique request identifier."""

        event = deepcopy(self.valid_template)
        event["request_id"] = str(uuid.uuid4())
        event["_source_position"] = source_position

        return event

    def test_valid_staged_record_becomes_valid(self) -> None:
        """Verify that a valid event receives VALID status."""

        event = self.build_event(source_position=0)

        staged_count, duplicate_count = stage_events(
            self.pipeline_run,
            [event],
        )

        self.assertEqual(staged_count, 1)
        self.assertEqual(duplicate_count, 0)

        summary = validate_staged_run(
            self.pipeline_run.pipeline_run_id
        )

        self.assertEqual(summary.pending_count, 1)
        self.assertEqual(summary.valid_count, 1)
        self.assertEqual(summary.rejected_count, 0)

        connection = create_connection()

        try:
            row = connection.execute(
                """
                SELECT
                    processing_status,
                    validation_errors
                FROM stg_safety_events
                WHERE pipeline_run_id = ?;
                """,
                (self.pipeline_run.pipeline_run_id,),
            ).fetchone()

            self.assertIsNotNone(row)
            self.assertEqual(row["processing_status"], "VALID")
            self.assertIsNone(row["validation_errors"])

        finally:
            connection.close()

    def test_invalid_action_is_quarantined(self) -> None:
        """Verify that an unsupported action is rejected and preserved."""

        event = self.build_event(source_position=0)
        event["final_action"] = "delete"

        stage_events(
            self.pipeline_run,
            [event],
        )

        summary = validate_staged_run(
            self.pipeline_run.pipeline_run_id
        )

        self.assertEqual(summary.valid_count, 0)
        self.assertEqual(summary.rejected_count, 1)

        connection = create_connection()

        try:
            staging_row = connection.execute(
                """
                SELECT
                    processing_status,
                    validation_errors
                FROM stg_safety_events
                WHERE pipeline_run_id = ?;
                """,
                (self.pipeline_run.pipeline_run_id,),
            ).fetchone()

            rejected_row = connection.execute(
                """
                SELECT
                    request_id,
                    error_reason,
                    raw_record
                FROM rejected_safety_events
                WHERE pipeline_run_id = ?;
                """,
                (self.pipeline_run.pipeline_run_id,),
            ).fetchone()

            self.assertIsNotNone(staging_row)
            self.assertEqual(
                staging_row["processing_status"],
                "REJECTED",
            )

            staging_errors = json.loads(
                staging_row["validation_errors"]
            )

            self.assertIn(
                "Unsupported final_action: delete",
                staging_errors,
            )

            self.assertIsNotNone(rejected_row)
            self.assertEqual(
                rejected_row["request_id"],
                event["request_id"],
            )

            rejection_errors = json.loads(
                rejected_row["error_reason"]
            )

            self.assertIn(
                "Unsupported final_action: delete",
                rejection_errors,
            )

            quarantined_event = json.loads(
                rejected_row["raw_record"]
            )

            self.assertEqual(
                quarantined_event["final_action"],
                "delete",
            )

        finally:
            connection.close()

    def test_mixed_batch_returns_correct_counts(self) -> None:
        """Verify summary counts for valid and invalid events."""

        valid_event = self.build_event(source_position=0)
        invalid_event = self.build_event(source_position=1)
        invalid_event["latency_ms"] = -5

        stage_events(
            self.pipeline_run,
            [valid_event, invalid_event],
        )

        summary = validate_staged_run(
            self.pipeline_run.pipeline_run_id
        )

        self.assertEqual(summary.pending_count, 2)
        self.assertEqual(summary.valid_count, 1)
        self.assertEqual(summary.rejected_count, 1)

    def test_run_without_pending_records_is_rejected(self) -> None:
        """Verify that an empty validation run cannot be processed."""

        with self.assertRaises(ValueError):
            validate_staged_run(
                self.pipeline_run.pipeline_run_id
            )


if __name__ == "__main__":
    unittest.main()

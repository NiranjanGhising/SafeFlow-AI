"""Tests for SafeFlow AI incremental selection and raw staging."""

from __future__ import annotations

import json
import unittest
import uuid

from pipeline.audit import start_pipeline_run
from pipeline.database import database_transaction
from pipeline.seed import seed_reference_data
from pipeline.staging import (
    calculate_watermark_end,
    parse_watermark,
    select_incremental_events,
    stage_events,
)


class StagingTests(unittest.TestCase):
    """Verify incremental selection and raw-event staging."""

    @classmethod
    def setUpClass(cls) -> None:
        """Initialize the schema and reference data."""

        seed_reference_data()

    def setUp(self) -> None:
        """Create a separate pipeline run for each test."""

        self.pipeline_run = start_pipeline_run()

    def tearDown(self) -> None:
        """Remove records created by the current test."""

        with database_transaction() as connection:
            connection.execute(
                """
                DELETE FROM stg_safety_events
                WHERE pipeline_run_id = ?;
                """,
                (self.pipeline_run.pipeline_run_id,),
            )

            connection.execute(
                """
                DELETE FROM rejected_safety_events
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

    def test_none_watermark_becomes_negative_one(self) -> None:
        """Verify an empty watermark starts before the first event."""

        self.assertEqual(parse_watermark(None), -1)

    def test_numeric_watermark_is_parsed(self) -> None:
        """Verify a valid watermark becomes an integer."""

        self.assertEqual(parse_watermark("59"), 59)

    def test_invalid_watermark_is_rejected(self) -> None:
        """Verify nonnumeric watermarks are rejected."""

        with self.assertRaises(ValueError):
            parse_watermark("invalid")

    def test_watermark_below_negative_one_is_rejected(self) -> None:
        """Verify watermarks cannot be lower than -1."""

        with self.assertRaises(ValueError):
            parse_watermark("-2")

    def test_incremental_selection_uses_source_position(self) -> None:
        """Verify only records after the watermark are selected."""

        events = [
            {
                "request_id": "request-1",
                "_source_position": 0,
            },
            {
                "request_id": "request-2",
                "_source_position": 1,
            },
            {
                "request_id": "request-3",
                "_source_position": 2,
            },
        ]

        selected_events = select_incremental_events(
            events,
            watermark_start="0",
        )

        selected_positions = [
            event["_source_position"]
            for event in selected_events
        ]

        self.assertEqual(selected_positions, [1, 2])

    def test_no_watermark_selects_all_events(self) -> None:
        """Verify the initial run selects every source event."""

        events = [
            {
                "request_id": "request-1",
                "_source_position": 0,
            },
            {
                "request_id": "request-2",
                "_source_position": 1,
            },
        ]

        selected_events = select_incremental_events(
            events,
            watermark_start=None,
        )

        self.assertEqual(len(selected_events), 2)

    def test_watermark_end_uses_highest_position(self) -> None:
        """Verify the proposed watermark uses the maximum position."""

        events = [
            {
                "request_id": "request-1",
                "_source_position": 4,
            },
            {
                "request_id": "request-2",
                "_source_position": 9,
            },
            {
                "request_id": "request-3",
                "_source_position": 7,
            },
        ]

        watermark_end = calculate_watermark_end(
            events,
            watermark_start="3",
        )

        self.assertEqual(watermark_end, "9")

    def test_empty_batch_keeps_existing_watermark(self) -> None:
        """Verify an empty batch does not change the watermark."""

        watermark_end = calculate_watermark_end(
            events=[],
            watermark_start="59",
        )

        self.assertEqual(watermark_end, "59")

    def test_event_is_staged_as_raw_json(self) -> None:
        """Verify raw event data is preserved in staging."""

        request_id = str(uuid.uuid4())

        events = [
            {
                "request_id": request_id,
                "category": "benign",
                "corpus": "benign",
                "prompt": "Explain Python lists.",
                "final_action": "allow",
                "latency_ms": 1.5,
                "_source_position": 0,
            }
        ]

        staged_count, duplicate_count = stage_events(
            self.pipeline_run,
            events,
        )

        self.assertEqual(staged_count, 1)
        self.assertEqual(duplicate_count, 0)

        from pipeline.database import create_connection

        connection = create_connection()

        try:
            row = connection.execute(
                """
                SELECT
                    request_id,
                    source_position,
                    processing_status,
                    raw_record
                FROM stg_safety_events
                WHERE pipeline_run_id = ?;
                """,
                (self.pipeline_run.pipeline_run_id,),
            ).fetchone()

            self.assertIsNotNone(row)
            self.assertEqual(row["request_id"], request_id)
            self.assertEqual(row["source_position"], 0)
            self.assertEqual(
                row["processing_status"],
                "PENDING",
            )

            raw_record = json.loads(row["raw_record"])

            self.assertEqual(
                raw_record["category"],
                "benign",
            )
            self.assertEqual(
                raw_record["final_action"],
                "allow",
            )
            self.assertNotIn(
                "_source_position",
                raw_record,
            )

        finally:
            connection.close()

    def test_duplicate_event_in_same_run_is_counted(self) -> None:
        """Verify duplicate staging records are counted safely."""

        request_id = str(uuid.uuid4())

        event = {
            "request_id": request_id,
            "category": "benign",
            "corpus": "benign",
            "prompt": "Test prompt",
            "final_action": "allow",
            "latency_ms": 1.0,
            "_source_position": 0,
        }

        first_staged, first_duplicates = stage_events(
            self.pipeline_run,
            [event],
        )

        second_staged, second_duplicates = stage_events(
            self.pipeline_run,
            [event],
        )

        self.assertEqual(first_staged, 1)
        self.assertEqual(first_duplicates, 0)
        self.assertEqual(second_staged, 0)
        self.assertEqual(second_duplicates, 1)


if __name__ == "__main__":
    unittest.main()
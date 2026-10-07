"""Tests for SafeFlow AI transformation and warehouse loading."""

from __future__ import annotations

import json
import unittest
import uuid
from copy import deepcopy
from datetime import datetime, timezone

from pipeline.audit import start_pipeline_run
from pipeline.database import create_connection, database_transaction
from pipeline.extract import extract_source_events
from pipeline.load import load_validated_run
from pipeline.seed import seed_reference_data
from pipeline.staging import stage_events
from pipeline.transform import (
    build_date_attributes,
    classify_latency,
    transform_safety_event,
)
from pipeline.validation_runner import validate_staged_run


class TransformAndLoadTests(unittest.TestCase):
    """Verify analytical transformation and idempotent warehouse loading."""

    @classmethod
    def setUpClass(cls) -> None:
        seed_reference_data()
        extracted = extract_source_events()
        cls.template = {
            key: value
            for key, value in extracted.events[0].items()
            if key != "_source_position"
        }

    def setUp(self) -> None:
        self.pipeline_run = start_pipeline_run()
        self.request_ids: list[str] = []

    def tearDown(self) -> None:
        with database_transaction() as connection:
            for request_id in self.request_ids:
                connection.execute(
                    "DELETE FROM fact_safety_event WHERE request_id = ?;",
                    (request_id,),
                )

            connection.execute(
                "DELETE FROM rejected_safety_events WHERE pipeline_run_id = ?;",
                (self.pipeline_run.pipeline_run_id,),
            )
            connection.execute(
                "DELETE FROM stg_safety_events WHERE pipeline_run_id = ?;",
                (self.pipeline_run.pipeline_run_id,),
            )
            connection.execute(
                "DELETE FROM pipeline_audit WHERE pipeline_run_id = ?;",
                (self.pipeline_run.pipeline_run_id,),
            )

    def build_event(
        self,
        *,
        source_position: int = 0,
        final_action: str | None = None,
    ) -> dict[str, object]:
        event = deepcopy(self.template)
        request_id = str(uuid.uuid4())
        event["request_id"] = request_id
        event["_source_position"] = source_position

        if final_action is not None:
            event["final_action"] = final_action

        self.request_ids.append(request_id)
        return event

    def test_latency_bands(self) -> None:
        self.assertEqual(classify_latency(0.5), "Fast")
        self.assertEqual(classify_latency(5.0), "Moderate")
        self.assertEqual(classify_latency(20.0), "Slow")

    def test_date_attributes(self) -> None:
        attributes = build_date_attributes(
            "2026-10-07T03:30:00+00:00"
        )

        self.assertEqual(attributes["date_key"], 20261007)
        self.assertEqual(attributes["full_date"], "2026-10-07")
        self.assertEqual(attributes["quarter"], 4)
        self.assertEqual(attributes["month_name"], "October")
        self.assertEqual(attributes["day_name"], "Wednesday")

    def test_timestamp_requires_timezone(self) -> None:
        with self.assertRaises(ValueError):
            build_date_attributes("2026-10-07T03:30:00")

    def test_action_flags_are_mutually_exclusive(self) -> None:
        event = self.build_event()
        event.pop("_source_position")

        transformed = transform_safety_event(
            event,
            event_timestamp=datetime.now(timezone.utc).isoformat(),
        )

        action_total = sum(
            [
                transformed.was_allowed,
                transformed.was_warned,
                transformed.was_redacted,
                transformed.was_blocked,
            ]
        )

        self.assertEqual(action_total, 1)

    def test_valid_record_loads_into_fact_table(self) -> None:
        event = self.build_event()
        stage_events(self.pipeline_run, [event])
        validate_staged_run(self.pipeline_run.pipeline_run_id)

        summary = load_validated_run(
            self.pipeline_run.pipeline_run_id
        )

        self.assertEqual(summary.valid_count, 1)
        self.assertEqual(summary.loaded_count, 1)
        self.assertEqual(summary.duplicate_count, 0)

        connection = create_connection()
        try:
            fact = connection.execute(
                """
                SELECT request_id, is_attack, latency_band
                FROM fact_safety_event
                WHERE request_id = ?;
                """,
                (event["request_id"],),
            ).fetchone()

            staging = connection.execute(
                """
                SELECT processing_status
                FROM stg_safety_events
                WHERE pipeline_run_id = ?;
                """,
                (self.pipeline_run.pipeline_run_id,),
            ).fetchone()

            self.assertIsNotNone(fact)
            self.assertEqual(fact["request_id"], event["request_id"])
            self.assertEqual(staging["processing_status"], "LOADED")
        finally:
            connection.close()

    def test_existing_request_becomes_duplicate(self) -> None:
        event = self.build_event()
        raw_event = {
            key: value
            for key, value in event.items()
            if key != "_source_position"
        }

        first_run = self.pipeline_run
        stage_events(first_run, [event])
        validate_staged_run(first_run.pipeline_run_id)
        first_summary = load_validated_run(first_run.pipeline_run_id)
        self.assertEqual(first_summary.loaded_count, 1)

        second_run = start_pipeline_run()
        try:
            duplicate_event = dict(raw_event)
            duplicate_event["_source_position"] = 1
            stage_events(second_run, [duplicate_event])
            validate_staged_run(second_run.pipeline_run_id)

            second_summary = load_validated_run(
                second_run.pipeline_run_id
            )

            self.assertEqual(second_summary.loaded_count, 0)
            self.assertEqual(second_summary.duplicate_count, 1)

            connection = create_connection()
            try:
                status = connection.execute(
                    """
                    SELECT processing_status
                    FROM stg_safety_events
                    WHERE pipeline_run_id = ?;
                    """,
                    (second_run.pipeline_run_id,),
                ).fetchone()["processing_status"]

                self.assertEqual(status, "DUPLICATE")
            finally:
                connection.close()
        finally:
            with database_transaction() as connection:
                connection.execute(
                    "DELETE FROM stg_safety_events WHERE pipeline_run_id = ?;",
                    (second_run.pipeline_run_id,),
                )
                connection.execute(
                    "DELETE FROM pipeline_audit WHERE pipeline_run_id = ?;",
                    (second_run.pipeline_run_id,),
                )


if __name__ == "__main__":
    unittest.main()

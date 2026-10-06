"""Tests for SafeFlow AI pipeline-audit management."""

from __future__ import annotations

import unittest

from pipeline.audit import (
    complete_pipeline_run,
    fail_pipeline_run,
    get_pipeline_run,
    start_pipeline_run,
    update_source_count,
)
from pipeline.seed import seed_reference_data


class PipelineAuditTests(unittest.TestCase):
    """Verify pipeline-run audit management."""

    @classmethod
    def setUpClass(cls) -> None:
        seed_reference_data()

    def test_start_pipeline_run_creates_running_record(
        self,
    ) -> None:
        """Verify that starting a run creates a RUNNING audit."""

        pipeline_run = start_pipeline_run()

        audit_record = get_pipeline_run(
            pipeline_run.pipeline_run_id
        )

        self.assertIsNotNone(audit_record)
        self.assertEqual(
            audit_record["status"],
            "RUNNING",
        )
        self.assertIsNotNone(
            audit_record["start_time"]
        )
        self.assertIsNone(
            audit_record["end_time"]
        )

    def test_source_count_can_be_updated(self) -> None:
        """Verify source-count recording."""

        pipeline_run = start_pipeline_run()

        update_source_count(
            pipeline_run.pipeline_run_id,
            60,
        )

        audit_record = get_pipeline_run(
            pipeline_run.pipeline_run_id
        )

        self.assertEqual(
            audit_record["source_count"],
            60,
        )

    def test_negative_source_count_is_rejected(self) -> None:
        """Verify negative source counts are rejected."""

        pipeline_run = start_pipeline_run()

        with self.assertRaises(ValueError):
            update_source_count(
                pipeline_run.pipeline_run_id,
                -1,
            )

    def test_pipeline_run_can_complete_successfully(
        self,
    ) -> None:
        """Verify successful audit completion."""

        pipeline_run = start_pipeline_run()

        update_source_count(
            pipeline_run.pipeline_run_id,
            60,
        )

        complete_pipeline_run(
            pipeline_run.pipeline_run_id,
            status="SUCCESS",
            watermark_end="59",
            valid_count=60,
            loaded_count=60,
            unexplained_count=0,
        )

        audit_record = get_pipeline_run(
            pipeline_run.pipeline_run_id
        )

        self.assertEqual(
            audit_record["status"],
            "SUCCESS",
        )
        self.assertEqual(
            audit_record["watermark_end"],
            "59",
        )
        self.assertEqual(
            audit_record["loaded_count"],
            60,
        )
        self.assertEqual(
            audit_record["unexplained_count"],
            0,
        )
        self.assertIsNotNone(
            audit_record["end_time"]
        )

    def test_pipeline_run_can_fail(self) -> None:
        """Verify failure recording."""

        pipeline_run = start_pipeline_run()

        fail_pipeline_run(
            pipeline_run.pipeline_run_id,
            RuntimeError("Test pipeline failure"),
        )

        audit_record = get_pipeline_run(
            pipeline_run.pipeline_run_id
        )

        self.assertEqual(
            audit_record["status"],
            "FAILED",
        )
        self.assertEqual(
            audit_record["error_message"],
            "Test pipeline failure",
        )
        self.assertIsNotNone(
            audit_record["end_time"]
        )

    def test_completed_run_cannot_be_completed_again(
        self,
    ) -> None:
        """Verify that a completed run is immutable."""

        pipeline_run = start_pipeline_run()

        complete_pipeline_run(
            pipeline_run.pipeline_run_id,
            status="SUCCESS",
        )

        with self.assertRaises(ValueError):
            complete_pipeline_run(
                pipeline_run.pipeline_run_id,
                status="FAILED",
            )

    def test_unknown_final_status_is_rejected(self) -> None:
        """Verify that unsupported final statuses fail."""

        pipeline_run = start_pipeline_run()

        with self.assertRaises(ValueError):
            complete_pipeline_run(
                pipeline_run.pipeline_run_id,
                status="UNKNOWN",
            )

    def test_unknown_pipeline_run_is_rejected(self) -> None:
        """Verify that nonexistent runs cannot be updated."""

        with self.assertRaises(ValueError):
            update_source_count(
                "nonexistent-run",
                60,
            )


if __name__ == "__main__":
    unittest.main()
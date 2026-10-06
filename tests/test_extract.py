"""Tests for SafeFlow AI source extraction."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pipeline.extract import (
    extract_source_events,
    load_trace_payload,
)


class SourceExtractionTests(unittest.TestCase):
    """Verify trace-file reading and source-event extraction."""

    def create_temporary_trace(
        self,
        payload: object,
    ) -> Path:
        """Create a temporary JSON trace and return its path."""

        temporary_file = tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".json",
            delete=False,
        )

        with temporary_file:
            json.dump(payload, temporary_file)

        self.addCleanup(
            lambda: Path(temporary_file.name).unlink(
                missing_ok=True
            )
        )

        return Path(temporary_file.name)

    def test_real_trace_contains_sixty_events(self) -> None:
        """Verify that the generated demo contains 60 events."""

        result = extract_source_events()

        self.assertEqual(result.source_count, 60)
        self.assertEqual(len(result.events), 60)

    def test_source_positions_are_added(self) -> None:
        """Verify that source positions are assigned sequentially."""

        result = extract_source_events()

        positions = [
            event["_source_position"]
            for event in result.events
        ]

        self.assertEqual(
            positions,
            list(range(result.source_count)),
        )

    def test_extraction_does_not_remove_request_id(self) -> None:
        """Verify that request IDs remain available."""

        result = extract_source_events()

        for event in result.events:
            self.assertIn("request_id", event)
            self.assertTrue(event["request_id"])

    def test_missing_trace_file_is_rejected(self) -> None:
        """Verify that a nonexistent trace path raises an error."""

        missing_path = Path(
            "/tmp/safeflow_missing_trace_file.json"
        )

        with self.assertRaises(FileNotFoundError):
            load_trace_payload(missing_path)

    def test_trace_root_must_be_an_object(self) -> None:
        """Verify that the JSON root cannot be a list."""

        trace_path = self.create_temporary_trace(
            [
                {
                    "request_id": "request-1",
                }
            ]
        )

        with self.assertRaises(TypeError):
            load_trace_payload(trace_path)

    def test_traces_property_is_required(self) -> None:
        """Verify that the traces property must exist."""

        trace_path = self.create_temporary_trace(
            {
                "summary": {},
            }
        )

        with self.assertRaises(ValueError):
            load_trace_payload(trace_path)

    def test_traces_property_must_be_a_list(self) -> None:
        """Verify that traces must contain a list."""

        trace_path = self.create_temporary_trace(
            {
                "traces": {
                    "request_id": "request-1",
                }
            }
        )

        with self.assertRaises(TypeError):
            load_trace_payload(trace_path)

    def test_each_trace_must_be_an_object(self) -> None:
        """Verify that individual events cannot be strings."""

        trace_path = self.create_temporary_trace(
            {
                "traces": [
                    "invalid-event",
                ]
            }
        )

        with self.assertRaises(TypeError):
            extract_source_events(trace_path)


if __name__ == "__main__":
    unittest.main()
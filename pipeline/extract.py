"""Incremental extraction utilities for SafeFlow AI safety traces."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pipeline.config import get_trace_path


@dataclass(frozen=True)
class ExtractionResult:
    """Result of reading safety events from the source trace."""

    source_path: Path
    source_count: int
    events: list[dict[str, Any]]


def load_trace_payload(
    trace_path: Path | None = None,
) -> dict[str, Any]:
    """Read and validate the top-level safety-trace JSON document."""

    source_path = trace_path or get_trace_path()

    if not source_path.exists():
        raise FileNotFoundError(
            f"Safety trace file was not found: {source_path}"
        )

    if not source_path.is_file():
        raise ValueError(
            f"Safety trace path is not a file: {source_path}"
        )

    try:
        with source_path.open(
            mode="r",
            encoding="utf-8",
        ) as trace_file:
            payload = json.load(trace_file)

    except json.JSONDecodeError as error:
        raise ValueError(
            f"Safety trace contains invalid JSON: {error}"
        ) from error

    if not isinstance(payload, dict):
        raise TypeError(
            "The safety trace root must be a JSON object."
        )

    if "traces" not in payload:
        raise ValueError(
            "The safety trace is missing the required 'traces' property."
        )

    if not isinstance(payload["traces"], list):
        raise TypeError(
            "The safety trace 'traces' property must be a list."
        )

    return payload


def extract_source_events(
    trace_path: Path | None = None,
) -> ExtractionResult:
    """Extract source events without modifying the original trace."""

    source_path = trace_path or get_trace_path()
    payload = load_trace_payload(source_path)

    events: list[dict[str, Any]] = []

    for source_position, event in enumerate(payload["traces"]):
        if not isinstance(event, dict):
            raise TypeError(
                "Every safety trace event must be a JSON object. "
                f"Invalid event found at source position "
                f"{source_position}."
            )

        copied_event = dict(event)
        copied_event["_source_position"] = source_position

        events.append(copied_event)

    return ExtractionResult(
        source_path=source_path,
        source_count=len(events),
        events=events,
    )


def print_source_summary(
    extraction_result: ExtractionResult,
) -> None:
    """Print a concise source-extraction summary."""

    print("SafeFlow AI source extraction")
    print(
        f"Source file:   "
        f"{extraction_result.source_path}"
    )
    print(
        f"Source events: "
        f"{extraction_result.source_count}"
    )

    if extraction_result.events:
        first_event = extraction_result.events[0]

        print(
            f"First request: "
            f"{first_event.get('request_id', 'missing')}"
        )
        print(
            f"First category: "
            f"{first_event.get('category', 'missing')}"
        )


if __name__ == "__main__":
    result = extract_source_events()
    print_source_summary(result)
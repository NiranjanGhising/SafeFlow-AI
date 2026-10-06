"""Incremental selection and raw staging for SafeFlow AI."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pipeline.audit import (
    PipelineRun,
    start_pipeline_run,
    update_source_count,
)
from pipeline.database import database_transaction
from pipeline.extract import extract_source_events


@dataclass(frozen=True)
class StagingResult:
    """Summary of one raw-staging operation."""

    pipeline_run_id: str
    scanned_count: int
    incremental_count: int
    staged_count: int
    duplicate_count: int
    watermark_start: str | None
    proposed_watermark_end: str | None


def utc_now() -> str:
    """Return the current UTC timestamp."""

    return datetime.now(timezone.utc).isoformat()


def parse_watermark(
    watermark: str | None,
) -> int:
    """Convert the stored watermark to an integer source position."""

    if watermark is None:
        return -1

    try:
        parsed_watermark = int(watermark)

    except ValueError as error:
        raise ValueError(
            f"Invalid source-position watermark: {watermark}"
        ) from error

    if parsed_watermark < -1:
        raise ValueError(
            "Source-position watermark cannot be below -1."
        )

    return parsed_watermark


def select_incremental_events(
    events: list[dict[str, Any]],
    watermark_start: str | None,
) -> list[dict[str, Any]]:
    """Select events positioned after the last successful watermark."""

    last_processed_position = parse_watermark(watermark_start)

    incremental_events = [
        event
        for event in events
        if event["_source_position"] > last_processed_position
    ]

    return incremental_events


def calculate_watermark_end(
    events: list[dict[str, Any]],
    watermark_start: str | None,
) -> str | None:
    """Return the highest extracted source position."""

    if not events:
        return watermark_start

    highest_position = max(
        event["_source_position"]
        for event in events
    )

    return str(highest_position)


def stage_events(
    pipeline_run: PipelineRun,
    events: list[dict[str, Any]],
) -> tuple[int, int]:
    """Insert incremental events into the raw staging table."""

    staged_count = 0
    duplicate_count = 0
    extracted_at = utc_now()

    with database_transaction() as connection:
        for event in events:
            request_id = event.get("request_id")
            source_position = event["_source_position"]

            raw_event = {
                key: value
                for key, value in event.items()
                if key != "_source_position"
            }

            raw_record = json.dumps(
                raw_event,
                ensure_ascii=False,
                sort_keys=True,
            )

            try:
                connection.execute(
                    """
                    INSERT INTO stg_safety_events (
                        pipeline_run_id,
                        request_id,
                        source_position,
                        extracted_at,
                        raw_record,
                        processing_status
                    )
                    VALUES (?, ?, ?, ?, ?, ?);
                    """,
                    (
                        pipeline_run.pipeline_run_id,
                        str(request_id or ""),
                        source_position,
                        extracted_at,
                        raw_record,
                        "PENDING",
                    ),
                )

                staged_count += 1

            except Exception as error:
                error_message = str(error).lower()

                if (
                    "unique constraint failed"
                    in error_message
                ):
                    duplicate_count += 1
                    continue

                raise

    return staged_count, duplicate_count


def run_staging() -> StagingResult:
    """Start an audit run and stage unprocessed source events."""

    extraction_result = extract_source_events()
    pipeline_run = start_pipeline_run()

    incremental_events = select_incremental_events(
        extraction_result.events,
        pipeline_run.watermark_start,
    )

    update_source_count(
        pipeline_run.pipeline_run_id,
        len(incremental_events),
    )

    staged_count, duplicate_count = stage_events(
        pipeline_run,
        incremental_events,
    )

    proposed_watermark_end = calculate_watermark_end(
        incremental_events,
        pipeline_run.watermark_start,
    )

    return StagingResult(
        pipeline_run_id=pipeline_run.pipeline_run_id,
        scanned_count=extraction_result.source_count,
        incremental_count=len(incremental_events),
        staged_count=staged_count,
        duplicate_count=duplicate_count,
        watermark_start=pipeline_run.watermark_start,
        proposed_watermark_end=proposed_watermark_end,
    )


def print_staging_summary(
    staging_result: StagingResult,
) -> None:
    """Display a concise raw-staging summary."""

    print("SafeFlow AI raw staging")
    print(
        f"Pipeline run ID:       "
        f"{staging_result.pipeline_run_id}"
    )
    print(
        f"Source events scanned: "
        f"{staging_result.scanned_count}"
    )
    print(
        f"Incremental events:    "
        f"{staging_result.incremental_count}"
    )
    print(
        f"Events staged:         "
        f"{staging_result.staged_count}"
    )
    print(
        f"Duplicates detected:   "
        f"{staging_result.duplicate_count}"
    )
    print(
        f"Watermark start:       "
        f"{staging_result.watermark_start}"
    )
    print(
        f"Proposed watermark:    "
        f"{staging_result.proposed_watermark_end}"
    )
    print()
    print(
        "Watermark has not been committed yet. "
        "It will be updated only after validation, loading, "
        "and reconciliation succeed."
    )


if __name__ == "__main__":
    result = run_staging()
    print_staging_summary(result)
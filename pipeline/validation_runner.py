"""Validate staged SafeFlow AI events and quarantine invalid records."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone

from pipeline.database import create_connection, database_transaction
from pipeline.validate import validate_raw_record


@dataclass(frozen=True)
class StagedValidationSummary:
    """Summary of staged-record validation."""

    pipeline_run_id: str
    pending_count: int
    valid_count: int
    rejected_count: int


def utc_now() -> str:
    """Return the current UTC timestamp in ISO 8601 format."""

    return datetime.now(timezone.utc).isoformat()


def find_latest_staged_pipeline_run() -> str:
    """Return the latest running pipeline with pending staging records."""

    connection = create_connection()

    try:
        row = connection.execute(
            """
            SELECT pa.pipeline_run_id
            FROM pipeline_audit AS pa
            WHERE pa.status = 'RUNNING'
              AND EXISTS (
                  SELECT 1
                  FROM stg_safety_events AS stg
                  WHERE stg.pipeline_run_id = pa.pipeline_run_id
                    AND stg.processing_status = 'PENDING'
              )
            ORDER BY pa.start_time DESC
            LIMIT 1;
            """
        ).fetchone()

        if row is None:
            raise RuntimeError(
                "No RUNNING pipeline execution with PENDING "
                "staging records was found."
            )

        return str(row["pipeline_run_id"])

    finally:
        connection.close()


def get_pending_staging_records(
    pipeline_run_id: str,
) -> list[dict[str, object]]:
    """Return pending staging records for one pipeline run."""

    connection = create_connection()

    try:
        rows = connection.execute(
            """
            SELECT
                staging_id,
                pipeline_run_id,
                request_id,
                source_position,
                raw_record
            FROM stg_safety_events
            WHERE pipeline_run_id = ?
              AND processing_status = 'PENDING'
            ORDER BY source_position;
            """,
            (pipeline_run_id,),
        ).fetchall()

        return [dict(row) for row in rows]

    finally:
        connection.close()


def quarantine_record(
    connection: sqlite3.Connection,
    *,
    staging_record: dict[str, object],
    errors: list[str],
) -> None:
    """Store an invalid staging record in the quarantine table."""

    serialized_errors = json.dumps(errors, ensure_ascii=False)

    connection.execute(
        """
        INSERT INTO rejected_safety_events (
            pipeline_run_id,
            request_id,
            source_position,
            error_reason,
            raw_record,
            rejected_at
        )
        VALUES (?, ?, ?, ?, ?, ?);
        """,
        (
            staging_record["pipeline_run_id"],
            staging_record["request_id"],
            staging_record["source_position"],
            serialized_errors,
            staging_record["raw_record"],
            utc_now(),
        ),
    )


def mark_staging_record_valid(
    connection: sqlite3.Connection,
    staging_id: int,
) -> None:
    """Mark one pending staging record as valid."""

    cursor = connection.execute(
        """
        UPDATE stg_safety_events
        SET
            processing_status = 'VALID',
            validation_errors = NULL
        WHERE staging_id = ?
          AND processing_status = 'PENDING';
        """,
        (staging_id,),
    )

    if cursor.rowcount != 1:
        raise RuntimeError(
            "Unable to mark the staging record as VALID: "
            f"{staging_id}"
        )


def mark_staging_record_rejected(
    connection: sqlite3.Connection,
    *,
    staging_id: int,
    errors: list[str],
) -> None:
    """Mark one pending staging record as rejected."""

    serialized_errors = json.dumps(errors, ensure_ascii=False)

    cursor = connection.execute(
        """
        UPDATE stg_safety_events
        SET
            processing_status = 'REJECTED',
            validation_errors = ?
        WHERE staging_id = ?
          AND processing_status = 'PENDING';
        """,
        (serialized_errors, staging_id),
    )

    if cursor.rowcount != 1:
        raise RuntimeError(
            "Unable to mark the staging record as REJECTED: "
            f"{staging_id}"
        )


def validate_staged_run(
    pipeline_run_id: str,
) -> StagedValidationSummary:
    """Validate all pending staging records in one pipeline run."""

    pending_records = get_pending_staging_records(pipeline_run_id)

    if not pending_records:
        raise ValueError(
            "No PENDING staging records were found for pipeline "
            f"run: {pipeline_run_id}"
        )

    valid_count = 0
    rejected_count = 0

    with database_transaction() as connection:
        for staging_record in pending_records:
            raw_record = staging_record["raw_record"]

            if not isinstance(raw_record, str):
                validation_errors = [
                    "Staged raw_record must be a string."
                ]
            else:
                validation_result = validate_raw_record(raw_record)
                validation_errors = validation_result.errors

            if not validation_errors:
                mark_staging_record_valid(
                    connection,
                    int(staging_record["staging_id"]),
                )
                valid_count += 1
                continue

            quarantine_record(
                connection,
                staging_record=staging_record,
                errors=validation_errors,
            )

            mark_staging_record_rejected(
                connection,
                staging_id=int(staging_record["staging_id"]),
                errors=validation_errors,
            )

            rejected_count += 1

    return StagedValidationSummary(
        pipeline_run_id=pipeline_run_id,
        pending_count=len(pending_records),
        valid_count=valid_count,
        rejected_count=rejected_count,
    )


def print_validation_summary(
    summary: StagedValidationSummary,
) -> None:
    """Print the staged-validation result."""

    print("SafeFlow AI staged validation")
    print(f"Pipeline run ID:  {summary.pipeline_run_id}")
    print(f"Records checked:  {summary.pending_count}")
    print(f"Valid records:    {summary.valid_count}")
    print(f"Rejected records: {summary.rejected_count}")


def main() -> None:
    """Validate the latest staged pipeline run."""

    pipeline_run_id = find_latest_staged_pipeline_run()
    summary = validate_staged_run(pipeline_run_id)
    print_validation_summary(summary)


if __name__ == "__main__":
    main()

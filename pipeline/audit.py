"""Pipeline-audit management for SafeFlow AI."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from pipeline.database import (
    create_connection,
    database_transaction,
)
from pipeline.seed import PIPELINE_NAME


VALID_FINAL_STATUSES = {
    "SUCCESS",
    "FAILED",
}


def utc_now() -> str:
    """Return the current UTC timestamp in ISO 8601 format."""

    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class PipelineRun:
    """Basic information about a pipeline execution."""

    pipeline_run_id: str
    start_time: str
    watermark_start: str | None
    status: str


def get_current_watermark() -> str | None:
    """Return the last successful pipeline watermark."""

    connection = create_connection()

    try:
        row = connection.execute(
            """
            SELECT last_successful_watermark
            FROM pipeline_state
            WHERE pipeline_name = ?;
            """,
            (PIPELINE_NAME,),
        ).fetchone()

        if row is None:
            raise RuntimeError(
                "Pipeline state has not been initialized. "
                "Run 'python -m pipeline.seed' first."
            )

        return row["last_successful_watermark"]

    finally:
        connection.close()


def start_pipeline_run() -> PipelineRun:
    """Create and return a new RUNNING pipeline-audit record."""

    pipeline_run_id = str(uuid.uuid4())
    start_time = utc_now()
    watermark_start = get_current_watermark()

    with database_transaction() as connection:
        connection.execute(
            """
            INSERT INTO pipeline_audit (
                pipeline_run_id,
                start_time,
                watermark_start,
                status
            )
            VALUES (?, ?, ?, ?);
            """,
            (
                pipeline_run_id,
                start_time,
                watermark_start,
                "RUNNING",
            ),
        )

    return PipelineRun(
        pipeline_run_id=pipeline_run_id,
        start_time=start_time,
        watermark_start=watermark_start,
        status="RUNNING",
    )


def update_source_count(
    pipeline_run_id: str,
    source_count: int,
) -> None:
    """Record the number of source events found during extraction."""

    if source_count < 0:
        raise ValueError(
            "Source count cannot be negative."
        )

    with database_transaction() as connection:
        cursor = connection.execute(
            """
            UPDATE pipeline_audit
            SET source_count = ?
            WHERE pipeline_run_id = ?
              AND status = 'RUNNING';
            """,
            (
                source_count,
                pipeline_run_id,
            ),
        )

        if cursor.rowcount != 1:
            raise ValueError(
                "The pipeline run does not exist or is not RUNNING: "
                f"{pipeline_run_id}"
            )


def complete_pipeline_run(
    pipeline_run_id: str,
    *,
    status: str,
    watermark_end: str | None = None,
    valid_count: int = 0,
    rejected_count: int = 0,
    duplicate_count: int = 0,
    filtered_count: int = 0,
    loaded_count: int = 0,
    unexplained_count: int = 0,
    error_message: str | None = None,
) -> None:
    """Complete a pipeline run with final counts and status."""

    if status not in VALID_FINAL_STATUSES:
        raise ValueError(
            "Final pipeline status must be SUCCESS or FAILED."
        )

    counts = {
        "valid_count": valid_count,
        "rejected_count": rejected_count,
        "duplicate_count": duplicate_count,
        "filtered_count": filtered_count,
        "loaded_count": loaded_count,
        "unexplained_count": unexplained_count,
    }

    negative_counts = [
        name
        for name, value in counts.items()
        if value < 0
    ]

    if negative_counts:
        raise ValueError(
            "Pipeline counts cannot be negative: "
            + ", ".join(sorted(negative_counts))
        )

    end_time = utc_now()

    with database_transaction() as connection:
        cursor = connection.execute(
            """
            UPDATE pipeline_audit
            SET
                end_time = ?,
                watermark_end = ?,
                valid_count = ?,
                rejected_count = ?,
                duplicate_count = ?,
                filtered_count = ?,
                loaded_count = ?,
                unexplained_count = ?,
                status = ?,
                error_message = ?
            WHERE pipeline_run_id = ?
              AND status = 'RUNNING';
            """,
            (
                end_time,
                watermark_end,
                valid_count,
                rejected_count,
                duplicate_count,
                filtered_count,
                loaded_count,
                unexplained_count,
                status,
                error_message,
                pipeline_run_id,
            ),
        )

        if cursor.rowcount != 1:
            raise ValueError(
                "The pipeline run does not exist or has already "
                f"been completed: {pipeline_run_id}"
            )


def fail_pipeline_run(
    pipeline_run_id: str,
    error: Exception | str,
) -> None:
    """Mark a running pipeline execution as failed."""

    complete_pipeline_run(
        pipeline_run_id,
        status="FAILED",
        error_message=str(error),
    )


def get_pipeline_run(
    pipeline_run_id: str,
) -> dict[str, object] | None:
    """Return one pipeline-audit record."""

    connection = create_connection()

    try:
        row = connection.execute(
            """
            SELECT *
            FROM pipeline_audit
            WHERE pipeline_run_id = ?;
            """,
            (pipeline_run_id,),
        ).fetchone()

        if row is None:
            return None

        return dict(row)

    finally:
        connection.close()


if __name__ == "__main__":
    pipeline_run = start_pipeline_run()

    print("SafeFlow AI pipeline run started.")
    print(f"Pipeline run ID: {pipeline_run.pipeline_run_id}")
    print(f"Start time:      {pipeline_run.start_time}")
    print(f"Watermark:       {pipeline_run.watermark_start}")
    print(f"Status:          {pipeline_run.status}")
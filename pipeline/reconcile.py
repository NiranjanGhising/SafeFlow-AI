"""Reconcile SafeFlow AI pipeline counts and finalize successful runs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from pipeline.database import create_connection, database_transaction
from pipeline.seed import PIPELINE_NAME


@dataclass(frozen=True)
class ReconciliationResult:
    """Record-accounting result for one pipeline run."""

    pipeline_run_id: str
    source_count: int
    loaded_count: int
    rejected_count: int
    duplicate_count: int
    filtered_count: int
    valid_count: int
    unexplained_count: int
    watermark_start: str | None
    watermark_end: str | None
    passed: bool


def utc_now() -> str:
    """Return the current UTC timestamp in ISO 8601 format."""

    return datetime.now(timezone.utc).isoformat()


def get_pipeline_audit(
    pipeline_run_id: str,
) -> dict[str, object]:
    """Return one pipeline audit record."""

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
            raise ValueError(
                f"Pipeline run was not found: {pipeline_run_id}"
            )

        return dict(row)

    finally:
        connection.close()


def get_staging_status_counts(
    pipeline_run_id: str,
) -> dict[str, int]:
    """Count staging records by processing status."""

    connection = create_connection()

    try:
        rows = connection.execute(
            """
            SELECT processing_status, COUNT(*) AS total
            FROM stg_safety_events
            WHERE pipeline_run_id = ?
            GROUP BY processing_status;
            """,
            (pipeline_run_id,),
        ).fetchall()

        return {
            str(row["processing_status"]): int(row["total"])
            for row in rows
        }

    finally:
        connection.close()


def get_rejected_count(pipeline_run_id: str) -> int:
    """Count quarantined records for one pipeline run."""

    connection = create_connection()

    try:
        return int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM rejected_safety_events
                WHERE pipeline_run_id = ?;
                """,
                (pipeline_run_id,),
            ).fetchone()[0]
        )

    finally:
        connection.close()


def calculate_watermark_end(
    pipeline_run_id: str,
    watermark_start: str | None,
) -> str | None:
    """Return the highest source position processed by the run."""

    connection = create_connection()

    try:
        row = connection.execute(
            """
            SELECT MAX(source_position) AS max_position
            FROM stg_safety_events
            WHERE pipeline_run_id = ?;
            """,
            (pipeline_run_id,),
        ).fetchone()

        if row is None or row["max_position"] is None:
            return watermark_start

        return str(row["max_position"])

    finally:
        connection.close()


def reconcile_pipeline_run(
    pipeline_run_id: str,
) -> ReconciliationResult:
    """Account for every source record in one pipeline run."""

    audit = get_pipeline_audit(pipeline_run_id)

    if audit["status"] != "RUNNING":
        raise ValueError(
            "Only a RUNNING pipeline can be reconciled: "
            f"{pipeline_run_id}"
        )

    status_counts = get_staging_status_counts(pipeline_run_id)

    loaded_count = status_counts.get("LOADED", 0)
    duplicate_count = status_counts.get("DUPLICATE", 0)
    rejected_status_count = status_counts.get("REJECTED", 0)
    pending_count = status_counts.get("PENDING", 0)
    remaining_valid_count = status_counts.get("VALID", 0)
    rejected_count = get_rejected_count(pipeline_run_id)
    filtered_count = 0
    source_count = int(audit["source_count"])

    if rejected_count != rejected_status_count:
        raise RuntimeError(
            "Rejected staging count does not match quarantine count."
        )

    valid_count = loaded_count + duplicate_count
    accounted_count = (
        loaded_count
        + duplicate_count
        + rejected_count
        + filtered_count
    )
    unexplained_count = source_count - accounted_count

    if unexplained_count < 0:
        raise RuntimeError(
            "Accounted records exceed the source count."
        )

    passed = (
        unexplained_count == 0
        and pending_count == 0
        and remaining_valid_count == 0
    )

    watermark_start_value = audit["watermark_start"]
    watermark_start = (
        None
        if watermark_start_value is None
        else str(watermark_start_value)
    )
    watermark_end = calculate_watermark_end(
        pipeline_run_id,
        watermark_start,
    )

    return ReconciliationResult(
        pipeline_run_id=pipeline_run_id,
        source_count=source_count,
        loaded_count=loaded_count,
        rejected_count=rejected_count,
        duplicate_count=duplicate_count,
        filtered_count=filtered_count,
        valid_count=valid_count,
        unexplained_count=unexplained_count,
        watermark_start=watermark_start,
        watermark_end=watermark_end,
        passed=passed,
    )


def finalize_pipeline_run(
    result: ReconciliationResult,
) -> None:
    """Finalize audit data and commit watermark only after success."""

    final_status = "SUCCESS" if result.passed else "FAILED"
    error_message = (
        None
        if result.passed
        else "Pipeline reconciliation failed."
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
                result.watermark_end,
                result.valid_count,
                result.rejected_count,
                result.duplicate_count,
                result.filtered_count,
                result.loaded_count,
                result.unexplained_count,
                final_status,
                error_message,
                result.pipeline_run_id,
            ),
        )

        if cursor.rowcount != 1:
            raise RuntimeError(
                "Pipeline audit could not be finalized: "
                f"{result.pipeline_run_id}"
            )

        if result.passed:
            state_cursor = connection.execute(
                """
                UPDATE pipeline_state
                SET
                    last_successful_watermark = ?,
                    last_pipeline_run_id = ?,
                    updated_at = ?
                WHERE pipeline_name = ?;
                """,
                (
                    result.watermark_end,
                    result.pipeline_run_id,
                    end_time,
                    PIPELINE_NAME,
                ),
            )

            if state_cursor.rowcount != 1:
                raise RuntimeError(
                    "Pipeline state could not be updated."
                )


def print_reconciliation(result: ReconciliationResult) -> None:
    """Print a concise reconciliation summary."""

    print("SafeFlow AI reconciliation")
    print(f"Pipeline run ID:  {result.pipeline_run_id}")
    print(f"Source records:   {result.source_count}")
    print(f"Loaded records:   {result.loaded_count}")
    print(f"Rejected records: {result.rejected_count}")
    print(f"Duplicate records:{result.duplicate_count}")
    print(f"Filtered records: {result.filtered_count}")
    print(f"Unexplained:      {result.unexplained_count}")
    print(f"Watermark start:  {result.watermark_start}")
    print(f"Watermark end:    {result.watermark_end}")
    print(f"Result:           {'PASS' if result.passed else 'FAIL'}")


def find_latest_loadable_run() -> str:
    """Return the latest running pipeline containing processed staging rows."""

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
                    AND stg.processing_status IN (
                        'LOADED', 'DUPLICATE', 'REJECTED'
                    )
              )
            ORDER BY pa.start_time DESC
            LIMIT 1;
            """
        ).fetchone()

        if row is None:
            raise RuntimeError(
                "No RUNNING processed pipeline run was found."
            )

        return str(row["pipeline_run_id"])

    finally:
        connection.close()


def main() -> None:
    """Reconcile and finalize the latest processed pipeline run."""

    pipeline_run_id = find_latest_loadable_run()
    result = reconcile_pipeline_run(pipeline_run_id)
    finalize_pipeline_run(result)
    print_reconciliation(result)

    if not result.passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

"""Load validated SafeFlow AI events into the analytical warehouse."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone

from pipeline.database import create_connection, database_transaction
from pipeline.transform import TransformedSafetyEvent, transform_raw_record


@dataclass(frozen=True)
class WarehouseLoadSummary:
    """Summary of one warehouse-loading operation."""

    pipeline_run_id: str
    valid_count: int
    loaded_count: int
    duplicate_count: int


def utc_now() -> str:
    """Return the current UTC timestamp in ISO 8601 format."""

    return datetime.now(timezone.utc).isoformat()


def get_valid_staging_records(
    pipeline_run_id: str,
) -> list[dict[str, object]]:
    """Return validated staging records that have not been loaded."""

    connection = create_connection()

    try:
        rows = connection.execute(
            """
            SELECT
                staging_id,
                pipeline_run_id,
                request_id,
                extracted_at,
                raw_record
            FROM stg_safety_events
            WHERE pipeline_run_id = ?
              AND processing_status = 'VALID'
            ORDER BY source_position;
            """,
            (pipeline_run_id,),
        ).fetchall()

        return [dict(row) for row in rows]

    finally:
        connection.close()


def upsert_date_dimension(
    connection: sqlite3.Connection,
    event: TransformedSafetyEvent,
) -> None:
    """Insert the date dimension member when it does not exist."""

    connection.execute(
        """
        INSERT INTO dim_date (
            date_key,
            full_date,
            day,
            month,
            month_name,
            quarter,
            year,
            day_name
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(date_key) DO UPDATE SET
            full_date = excluded.full_date,
            day = excluded.day,
            month = excluded.month,
            month_name = excluded.month_name,
            quarter = excluded.quarter,
            year = excluded.year,
            day_name = excluded.day_name;
        """,
        (
            event.date_key,
            event.full_date,
            event.day,
            event.month,
            event.month_name,
            event.quarter,
            event.year,
            event.day_name,
        ),
    )


def get_category_key(
    connection: sqlite3.Connection,
    category_name: str,
) -> int:
    """Resolve a seeded category name to its surrogate key."""

    row = connection.execute(
        """
        SELECT category_key
        FROM dim_category
        WHERE category_name = ?;
        """,
        (category_name,),
    ).fetchone()

    if row is None:
        raise ValueError(
            f"Category was not found in dim_category: {category_name}"
        )

    return int(row["category_key"])


def get_action_key(
    connection: sqlite3.Connection,
    action_name: str,
) -> int:
    """Resolve a seeded action name to its surrogate key."""

    row = connection.execute(
        """
        SELECT action_key
        FROM dim_action
        WHERE action_name = ?;
        """,
        (action_name,),
    ).fetchone()

    if row is None:
        raise ValueError(
            f"Action was not found in dim_action: {action_name}"
        )

    return int(row["action_key"])


def insert_fact_event(
    connection: sqlite3.Connection,
    *,
    pipeline_run_id: str,
    event: TransformedSafetyEvent,
) -> bool:
    """Insert one fact event and return False when already loaded."""

    category_key = get_category_key(
        connection,
        event.category_name,
    )
    action_key = get_action_key(
        connection,
        event.action_name,
    )

    cursor = connection.execute(
        """
        INSERT INTO fact_safety_event (
            request_id,
            pipeline_run_id,
            date_key,
            category_key,
            action_key,
            corpus,
            fixture_id,
            prompt,
            final_output,
            pre_gen_category,
            pre_gen_confidence,
            pre_gen_blocked,
            terminated_early,
            post_gen_checked,
            is_attack,
            was_allowed,
            was_warned,
            was_redacted,
            was_blocked,
            latency_ms,
            latency_band,
            event_timestamp,
            loaded_at
        )
        VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT(request_id) DO NOTHING;
        """,
        (
            event.request_id,
            pipeline_run_id,
            event.date_key,
            category_key,
            action_key,
            event.corpus,
            event.fixture_id,
            event.prompt,
            event.final_output,
            event.pre_gen_category,
            event.pre_gen_confidence,
            event.pre_gen_blocked,
            event.terminated_early,
            event.post_gen_checked,
            event.is_attack,
            event.was_allowed,
            event.was_warned,
            event.was_redacted,
            event.was_blocked,
            event.latency_ms,
            event.latency_band,
            event.event_timestamp,
            utc_now(),
        ),
    )

    return cursor.rowcount == 1


def update_staging_status(
    connection: sqlite3.Connection,
    *,
    staging_id: int,
    status: str,
) -> None:
    """Mark a VALID staging record as LOADED or DUPLICATE."""

    if status not in {"LOADED", "DUPLICATE"}:
        raise ValueError(
            "Staging load status must be LOADED or DUPLICATE."
        )

    cursor = connection.execute(
        """
        UPDATE stg_safety_events
        SET processing_status = ?
        WHERE staging_id = ?
          AND processing_status = 'VALID';
        """,
        (status, staging_id),
    )

    if cursor.rowcount != 1:
        raise RuntimeError(
            "Unable to update staging load status for record: "
            f"{staging_id}"
        )


def load_validated_run(
    pipeline_run_id: str,
) -> WarehouseLoadSummary:
    """Transform and load all VALID records for one pipeline run."""

    records = get_valid_staging_records(pipeline_run_id)

    if not records:
        raise ValueError(
            "No VALID staging records were found for pipeline run: "
            f"{pipeline_run_id}"
        )

    loaded_count = 0
    duplicate_count = 0

    with database_transaction() as connection:
        for record in records:
            raw_record = record["raw_record"]
            extracted_at = record["extracted_at"]

            if not isinstance(raw_record, str):
                raise TypeError("Staged raw_record must be a string.")

            if not isinstance(extracted_at, str):
                raise TypeError("Staged extracted_at must be a string.")

            transformed = transform_raw_record(
                raw_record,
                event_timestamp=extracted_at,
            )

            upsert_date_dimension(connection, transformed)

            inserted = insert_fact_event(
                connection,
                pipeline_run_id=pipeline_run_id,
                event=transformed,
            )

            if inserted:
                status = "LOADED"
                loaded_count += 1
            else:
                status = "DUPLICATE"
                duplicate_count += 1

            update_staging_status(
                connection,
                staging_id=int(record["staging_id"]),
                status=status,
            )

    return WarehouseLoadSummary(
        pipeline_run_id=pipeline_run_id,
        valid_count=len(records),
        loaded_count=loaded_count,
        duplicate_count=duplicate_count,
    )


def find_latest_validated_pipeline_run() -> str:
    """Return the latest RUNNING pipeline with VALID staging records."""

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
                    AND stg.processing_status = 'VALID'
              )
            ORDER BY pa.start_time DESC
            LIMIT 1;
            """
        ).fetchone()

        if row is None:
            raise RuntimeError(
                "No RUNNING pipeline execution with VALID staging "
                "records was found."
            )

        return str(row["pipeline_run_id"])

    finally:
        connection.close()


def print_load_summary(summary: WarehouseLoadSummary) -> None:
    """Print warehouse-loading results."""

    print("SafeFlow AI warehouse loading")
    print(f"Pipeline run ID: {summary.pipeline_run_id}")
    print(f"Valid records:   {summary.valid_count}")
    print(f"Loaded records:  {summary.loaded_count}")
    print(f"Duplicates:      {summary.duplicate_count}")


def main() -> None:
    """Load the latest validated pipeline run."""

    pipeline_run_id = find_latest_validated_pipeline_run()
    summary = load_validated_run(pipeline_run_id)
    print_load_summary(summary)


if __name__ == "__main__":
    main()

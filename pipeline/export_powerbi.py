"""Export reconciled SafeFlow AI warehouse data for Power BI."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from pipeline.config import get_export_directory
from pipeline.database import create_connection


@dataclass(frozen=True)
class ExportDefinition:
    """One Power BI export definition."""

    file_name: str
    query: str


EXPORTS = [
    ExportDefinition(
        "fact_safety_events.csv",
        """
        SELECT *
        FROM fact_safety_event
        ORDER BY safety_event_key;
        """,
    ),
    ExportDefinition(
        "dim_action.csv",
        """
        SELECT *
        FROM dim_action
        ORDER BY severity_rank;
        """,
    ),
    ExportDefinition(
        "dim_category.csv",
        """
        SELECT *
        FROM dim_category
        ORDER BY category_key;
        """,
    ),
    ExportDefinition(
        "dim_date.csv",
        """
        SELECT *
        FROM dim_date
        ORDER BY date_key;
        """,
    ),
    ExportDefinition(
        "pipeline_audit.csv",
        """
        SELECT *
        FROM pipeline_audit
        ORDER BY start_time;
        """,
    ),
    ExportDefinition(
        "rejected_safety_events.csv",
        """
        SELECT *
        FROM rejected_safety_events
        ORDER BY rejection_id;
        """,
    ),
    ExportDefinition(
        "safety_events_flat.csv",
        """
        SELECT
            f.safety_event_key,
            f.request_id,
            f.pipeline_run_id,
            f.event_timestamp,
            d.full_date,
            d.day_name,
            d.month_name,
            d.quarter,
            d.year,
            c.category_name,
            c.is_attack_category,
            a.action_name,
            a.severity_rank,
            a.risk_level,
            f.corpus,
            f.fixture_id,
            f.prompt,
            f.final_output,
            f.pre_gen_category,
            f.pre_gen_confidence,
            f.pre_gen_blocked,
            f.terminated_early,
            f.post_gen_checked,
            f.is_attack,
            f.was_allowed,
            f.was_warned,
            f.was_redacted,
            f.was_blocked,
            f.latency_ms,
            f.latency_band,
            f.loaded_at
        FROM fact_safety_event AS f
        JOIN dim_date AS d
          ON d.date_key = f.date_key
        JOIN dim_category AS c
          ON c.category_key = f.category_key
        JOIN dim_action AS a
          ON a.action_key = f.action_key
        ORDER BY f.safety_event_key;
        """,
    ),
    ExportDefinition(
        "action_by_category.csv",
        """
        SELECT
            c.category_name,
            a.action_name,
            COUNT(*) AS request_count,
            ROUND(AVG(f.latency_ms), 3) AS average_latency_ms,
            SUM(f.terminated_early) AS early_termination_count
        FROM fact_safety_event AS f
        JOIN dim_category AS c
          ON c.category_key = f.category_key
        JOIN dim_action AS a
          ON a.action_key = f.action_key
        GROUP BY c.category_name, a.action_name
        ORDER BY c.category_name, a.severity_rank;
        """,
    ),
    ExportDefinition(
        "pipeline_reliability.csv",
        """
        SELECT
            pipeline_run_id,
            start_time,
            end_time,
            watermark_start,
            watermark_end,
            source_count,
            valid_count,
            rejected_count,
            duplicate_count,
            filtered_count,
            loaded_count,
            unexplained_count,
            status,
            error_message
        FROM pipeline_audit
        ORDER BY start_time;
        """,
    ),
]


def export_query(file_path: Path, query: str) -> int:
    """Export one SQLite query to CSV and return its row count."""

    connection = create_connection()

    try:
        cursor = connection.execute(query)
        column_names = [item[0] for item in cursor.description]
        rows = cursor.fetchall()
    finally:
        connection.close()

    file_path.parent.mkdir(parents=True, exist_ok=True)

    with file_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(column_names)
        writer.writerows([tuple(row) for row in rows])

    return len(rows)


def export_power_bi_data() -> dict[str, int]:
    """Create all Power BI-ready CSV exports."""

    export_directory = get_export_directory()
    results: dict[str, int] = {}

    for definition in EXPORTS:
        file_path = export_directory / definition.file_name
        results[definition.file_name] = export_query(
            file_path,
            definition.query,
        )

    return results


def main() -> None:
    """Export and report all dashboard datasets."""

    results = export_power_bi_data()
    print("SafeFlow AI Power BI exports")

    for file_name, row_count in results.items():
        print(f"{file_name}: {row_count} rows")


if __name__ == "__main__":
    main()

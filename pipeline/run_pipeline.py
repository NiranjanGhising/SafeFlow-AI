"""Single-command orchestration for the SafeFlow AI analytics pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from pipeline.audit import (
    PipelineRun,
    fail_pipeline_run,
    start_pipeline_run,
    update_source_count,
)
from pipeline.database import create_connection, database_transaction
from pipeline.extract import extract_source_events
from pipeline.load import load_validated_run
from pipeline.reconcile import (
    ReconciliationResult,
    finalize_pipeline_run,
    reconcile_pipeline_run,
)
from pipeline.seed import PIPELINE_NAME, seed_reference_data
from pipeline.staging import (
    calculate_watermark_end,
    select_incremental_events,
    stage_events,
)
from pipeline.validation_runner import validate_staged_run


@dataclass(frozen=True)
class PipelineExecutionSummary:
    """End-to-end result of one SafeFlow AI pipeline execution."""

    pipeline_run_id: str
    scanned_count: int
    incremental_count: int
    staged_count: int
    valid_count: int
    rejected_count: int
    loaded_count: int
    duplicate_count: int
    unexplained_count: int
    watermark_start: str | None
    watermark_end: str | None
    status: str


def utc_now() -> str:
    """Return the current UTC timestamp in ISO 8601 format."""

    return datetime.now(timezone.utc).isoformat()


def is_pipeline_run_running(pipeline_run_id: str) -> bool:
    """Return whether the supplied audit record is still RUNNING."""

    connection = create_connection()

    try:
        row = connection.execute(
            """
            SELECT status
            FROM pipeline_audit
            WHERE pipeline_run_id = ?;
            """,
            (pipeline_run_id,),
        ).fetchone()

        return row is not None and row["status"] == "RUNNING"

    finally:
        connection.close()


def finalize_empty_run(
    pipeline_run: PipelineRun,
) -> ReconciliationResult:
    """Finalize a successful run when no new events are available."""

    result = ReconciliationResult(
        pipeline_run_id=pipeline_run.pipeline_run_id,
        source_count=0,
        loaded_count=0,
        rejected_count=0,
        duplicate_count=0,
        filtered_count=0,
        valid_count=0,
        unexplained_count=0,
        watermark_start=pipeline_run.watermark_start,
        watermark_end=pipeline_run.watermark_start,
        passed=True,
    )

    finalize_pipeline_run(result)
    return result


def execute_pipeline() -> PipelineExecutionSummary:
    """Execute extraction through reconciliation as one controlled workflow."""

    seed_reference_data()
    extraction = extract_source_events()
    pipeline_run = start_pipeline_run()

    try:
        incremental_events = select_incremental_events(
            extraction.events,
            pipeline_run.watermark_start,
        )

        incremental_count = len(incremental_events)

        update_source_count(
            pipeline_run.pipeline_run_id,
            incremental_count,
        )

        if incremental_count == 0:
            result = finalize_empty_run(pipeline_run)

            return PipelineExecutionSummary(
                pipeline_run_id=pipeline_run.pipeline_run_id,
                scanned_count=extraction.source_count,
                incremental_count=0,
                staged_count=0,
                valid_count=0,
                rejected_count=0,
                loaded_count=0,
                duplicate_count=0,
                unexplained_count=0,
                watermark_start=result.watermark_start,
                watermark_end=result.watermark_end,
                status="SUCCESS",
            )

        staged_count, staging_duplicate_count = stage_events(
            pipeline_run,
            incremental_events,
        )

        proposed_watermark = calculate_watermark_end(
            incremental_events,
            pipeline_run.watermark_start,
        )

        if staging_duplicate_count != 0:
            raise RuntimeError(
                "Unexpected duplicates were found inside the new "
                "incremental staging batch."
            )

        if staged_count != incremental_count:
            raise RuntimeError(
                "Staged-record count does not match incremental count."
            )

        validation_summary = validate_staged_run(
            pipeline_run.pipeline_run_id
        )

        if (
            validation_summary.valid_count
            + validation_summary.rejected_count
            != incremental_count
        ):
            raise RuntimeError(
                "Validation counts do not reconcile with the "
                "incremental source count."
            )

        loaded_count = 0
        warehouse_duplicate_count = 0

        if validation_summary.valid_count > 0:
            load_summary = load_validated_run(
                pipeline_run.pipeline_run_id
            )
            loaded_count = load_summary.loaded_count
            warehouse_duplicate_count = load_summary.duplicate_count

        reconciliation = reconcile_pipeline_run(
            pipeline_run.pipeline_run_id
        )

        if reconciliation.watermark_end != proposed_watermark:
            raise RuntimeError(
                "Reconciliation watermark does not match the "
                "proposed extraction watermark."
            )

        finalize_pipeline_run(reconciliation)

        if not reconciliation.passed:
            raise RuntimeError(
                "Pipeline reconciliation failed."
            )

        return PipelineExecutionSummary(
            pipeline_run_id=pipeline_run.pipeline_run_id,
            scanned_count=extraction.source_count,
            incremental_count=incremental_count,
            staged_count=staged_count,
            valid_count=validation_summary.valid_count,
            rejected_count=validation_summary.rejected_count,
            loaded_count=loaded_count,
            duplicate_count=warehouse_duplicate_count,
            unexplained_count=reconciliation.unexplained_count,
            watermark_start=reconciliation.watermark_start,
            watermark_end=reconciliation.watermark_end,
            status="SUCCESS",
        )

    except Exception as error:
        if is_pipeline_run_running(
            pipeline_run.pipeline_run_id
        ):
            fail_pipeline_run(
                pipeline_run.pipeline_run_id,
                error,
            )

        raise


def print_execution_summary(
    summary: PipelineExecutionSummary,
) -> None:
    """Print the end-to-end pipeline execution summary."""

    print("SafeFlow AI end-to-end analytics pipeline")
    print(f"Pipeline run ID:   {summary.pipeline_run_id}")
    print(f"Events scanned:    {summary.scanned_count}")
    print(f"Incremental events:{summary.incremental_count}")
    print(f"Events staged:     {summary.staged_count}")
    print(f"Valid events:      {summary.valid_count}")
    print(f"Rejected events:   {summary.rejected_count}")
    print(f"Loaded events:     {summary.loaded_count}")
    print(f"Duplicate events:  {summary.duplicate_count}")
    print(f"Unexplained events:{summary.unexplained_count}")
    print(f"Watermark start:   {summary.watermark_start}")
    print(f"Watermark end:     {summary.watermark_end}")
    print(f"Status:            {summary.status}")


def main() -> None:
    """Run and report the complete analytics pipeline."""

    summary = execute_pipeline()
    print_execution_summary(summary)


if __name__ == "__main__":
    main()

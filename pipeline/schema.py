"""SQLite schema definition for the SafeFlow AI analytics pipeline."""

from __future__ import annotations

from pipeline.database import database_transaction


SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS pipeline_audit (
        pipeline_run_id TEXT PRIMARY KEY,
        start_time TEXT NOT NULL,
        end_time TEXT,
        watermark_start TEXT,
        watermark_end TEXT,
        source_count INTEGER NOT NULL DEFAULT 0
            CHECK (source_count >= 0),
        valid_count INTEGER NOT NULL DEFAULT 0
            CHECK (valid_count >= 0),
        rejected_count INTEGER NOT NULL DEFAULT 0
            CHECK (rejected_count >= 0),
        duplicate_count INTEGER NOT NULL DEFAULT 0
            CHECK (duplicate_count >= 0),
        filtered_count INTEGER NOT NULL DEFAULT 0
            CHECK (filtered_count >= 0),
        loaded_count INTEGER NOT NULL DEFAULT 0
            CHECK (loaded_count >= 0),
        unexplained_count INTEGER NOT NULL DEFAULT 0
            CHECK (unexplained_count >= 0),
        status TEXT NOT NULL
            CHECK (
                status IN (
                    'RUNNING',
                    'SUCCESS',
                    'FAILED'
                )
            ),
        error_message TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pipeline_state (
        pipeline_name TEXT PRIMARY KEY,
        last_successful_watermark TEXT,
        last_pipeline_run_id TEXT,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (last_pipeline_run_id)
            REFERENCES pipeline_audit(pipeline_run_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS stg_safety_events (
        staging_id INTEGER PRIMARY KEY AUTOINCREMENT,
        pipeline_run_id TEXT NOT NULL,
        request_id TEXT NOT NULL,
        source_position INTEGER NOT NULL
            CHECK (source_position >= 0),
        extracted_at TEXT NOT NULL,
        raw_record TEXT NOT NULL,
        processing_status TEXT NOT NULL DEFAULT 'PENDING'
            CHECK (
                processing_status IN (
                    'PENDING',
                    'VALID',
                    'REJECTED',
                    'LOADED',
                    'DUPLICATE'
                )
            ),
        validation_errors TEXT,
        UNIQUE (pipeline_run_id, request_id),
        FOREIGN KEY (pipeline_run_id)
            REFERENCES pipeline_audit(pipeline_run_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS rejected_safety_events (
        rejection_id INTEGER PRIMARY KEY AUTOINCREMENT,
        pipeline_run_id TEXT NOT NULL,
        request_id TEXT,
        source_position INTEGER,
        error_reason TEXT NOT NULL,
        raw_record TEXT NOT NULL,
        rejected_at TEXT NOT NULL,
        FOREIGN KEY (pipeline_run_id)
            REFERENCES pipeline_audit(pipeline_run_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_date (
        date_key INTEGER PRIMARY KEY,
        full_date TEXT NOT NULL UNIQUE,
        day INTEGER NOT NULL
            CHECK (day BETWEEN 1 AND 31),
        month INTEGER NOT NULL
            CHECK (month BETWEEN 1 AND 12),
        month_name TEXT NOT NULL,
        quarter INTEGER NOT NULL
            CHECK (quarter BETWEEN 1 AND 4),
        year INTEGER NOT NULL,
        day_name TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_category (
        category_key INTEGER PRIMARY KEY AUTOINCREMENT,
        category_name TEXT NOT NULL UNIQUE,
        category_description TEXT,
        is_attack_category INTEGER NOT NULL
            CHECK (is_attack_category IN (0, 1))
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_action (
        action_key INTEGER PRIMARY KEY AUTOINCREMENT,
        action_name TEXT NOT NULL UNIQUE
            CHECK (
                action_name IN (
                    'allow',
                    'warn',
                    'redact',
                    'block'
                )
            ),
        severity_rank INTEGER NOT NULL
            CHECK (severity_rank BETWEEN 1 AND 4),
        risk_level TEXT NOT NULL
            CHECK (
                risk_level IN (
                    'Low',
                    'Medium',
                    'High',
                    'Critical'
                )
            )
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_safety_event (
        safety_event_key INTEGER PRIMARY KEY AUTOINCREMENT,
        request_id TEXT NOT NULL UNIQUE,
        pipeline_run_id TEXT NOT NULL,
        date_key INTEGER NOT NULL,
        category_key INTEGER NOT NULL,
        action_key INTEGER NOT NULL,
        corpus TEXT NOT NULL
            CHECK (corpus IN ('attack', 'benign')),
        fixture_id TEXT,
        prompt TEXT NOT NULL,
        final_output TEXT,
        pre_gen_category TEXT,
        pre_gen_confidence REAL
            CHECK (
                pre_gen_confidence IS NULL
                OR pre_gen_confidence BETWEEN 0 AND 1
            ),
        pre_gen_blocked INTEGER NOT NULL DEFAULT 0
            CHECK (pre_gen_blocked IN (0, 1)),
        terminated_early INTEGER NOT NULL DEFAULT 0
            CHECK (terminated_early IN (0, 1)),
        post_gen_checked INTEGER NOT NULL DEFAULT 0
            CHECK (post_gen_checked IN (0, 1)),
        is_attack INTEGER NOT NULL
            CHECK (is_attack IN (0, 1)),
        was_allowed INTEGER NOT NULL
            CHECK (was_allowed IN (0, 1)),
        was_warned INTEGER NOT NULL
            CHECK (was_warned IN (0, 1)),
        was_redacted INTEGER NOT NULL
            CHECK (was_redacted IN (0, 1)),
        was_blocked INTEGER NOT NULL
            CHECK (was_blocked IN (0, 1)),
        latency_ms REAL NOT NULL
            CHECK (latency_ms >= 0),
        latency_band TEXT NOT NULL
            CHECK (
                latency_band IN (
                    'Fast',
                    'Moderate',
                    'Slow'
                )
            ),
        event_timestamp TEXT NOT NULL,
        loaded_at TEXT NOT NULL,
        FOREIGN KEY (pipeline_run_id)
            REFERENCES pipeline_audit(pipeline_run_id),
        FOREIGN KEY (date_key)
            REFERENCES dim_date(date_key),
        FOREIGN KEY (category_key)
            REFERENCES dim_category(category_key),
        FOREIGN KEY (action_key)
            REFERENCES dim_action(action_key)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_staging_pipeline_run
    ON stg_safety_events(pipeline_run_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_staging_processing_status
    ON stg_safety_events(processing_status);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_rejected_pipeline_run
    ON rejected_safety_events(pipeline_run_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fact_pipeline_run
    ON fact_safety_event(pipeline_run_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fact_date
    ON fact_safety_event(date_key);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fact_category
    ON fact_safety_event(category_key);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fact_action
    ON fact_safety_event(action_key);
    """,
]


def initialize_database() -> None:
    """Create all SafeFlow AI database tables and indexes."""

    with database_transaction() as connection:
        for statement in SCHEMA_STATEMENTS:
            connection.execute(statement)


def get_expected_tables() -> set[str]:
    """Return the set of tables required by SafeFlow AI."""

    return {
        "pipeline_audit",
        "pipeline_state",
        "stg_safety_events",
        "rejected_safety_events",
        "dim_date",
        "dim_category",
        "dim_action",
        "fact_safety_event",
    }


if __name__ == "__main__":
    initialize_database()
    print("SafeFlow AI database schema initialized successfully.")
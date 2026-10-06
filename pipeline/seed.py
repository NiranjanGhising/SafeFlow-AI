"""Seed stable reference data for the SafeFlow AI warehouse."""

from __future__ import annotations

from datetime import datetime, timezone

from pipeline.database import database_transaction
from pipeline.schema import initialize_database


ACTION_SEEDS = [
    {
        "action_name": "allow",
        "severity_rank": 1,
        "risk_level": "Low",
    },
    {
        "action_name": "warn",
        "severity_rank": 2,
        "risk_level": "Medium",
    },
    {
        "action_name": "redact",
        "severity_rank": 3,
        "risk_level": "High",
    },
    {
        "action_name": "block",
        "severity_rank": 4,
        "risk_level": "Critical",
    },
]


CATEGORY_SEEDS = [
    {
        "category_name": "benign",
        "category_description": (
            "A normal prompt that does not contain a recognized attack."
        ),
        "is_attack_category": 0,
    },
    {
        "category_name": "role-play",
        "category_description": (
            "An attempt to bypass safety controls using fictional roles, "
            "personas, or simulated scenarios."
        ),
        "is_attack_category": 1,
    },
    {
        "category_name": "instruction-override",
        "category_description": (
            "An attempt to replace or ignore the system's existing "
            "instructions."
        ),
        "is_attack_category": 1,
    },
    {
        "category_name": "context-smuggling",
        "category_description": (
            "An attempt to hide unauthorized instructions inside apparently "
            "legitimate contextual information."
        ),
        "is_attack_category": 1,
    },
    {
        "category_name": "multi-turn-ramp",
        "category_description": (
            "An attack that gradually increases risk across multiple "
            "interactions."
        ),
        "is_attack_category": 1,
    },
    {
        "category_name": "encoding-trick",
        "category_description": (
            "An attempt to hide instructions using encoding, obfuscation, "
            "or alternative representations."
        ),
        "is_attack_category": 1,
    },
    {
        "category_name": "prefix-injection",
        "category_description": (
            "An attempt to force the generated response to begin with "
            "attacker-controlled text."
        ),
        "is_attack_category": 1,
    },
]


PIPELINE_NAME = "safety_analytics_pipeline"


def seed_actions() -> None:
    """Insert or update the supported safety actions."""

    with database_transaction() as connection:
        for action in ACTION_SEEDS:
            connection.execute(
                """
                INSERT INTO dim_action (
                    action_name,
                    severity_rank,
                    risk_level
                )
                VALUES (?, ?, ?)
                ON CONFLICT(action_name)
                DO UPDATE SET
                    severity_rank = excluded.severity_rank,
                    risk_level = excluded.risk_level;
                """,
                (
                    action["action_name"],
                    action["severity_rank"],
                    action["risk_level"],
                ),
            )


def seed_categories() -> None:
    """Insert or update the supported safety categories."""

    with database_transaction() as connection:
        for category in CATEGORY_SEEDS:
            connection.execute(
                """
                INSERT INTO dim_category (
                    category_name,
                    category_description,
                    is_attack_category
                )
                VALUES (?, ?, ?)
                ON CONFLICT(category_name)
                DO UPDATE SET
                    category_description =
                        excluded.category_description,
                    is_attack_category =
                        excluded.is_attack_category;
                """,
                (
                    category["category_name"],
                    category["category_description"],
                    category["is_attack_category"],
                ),
            )


def seed_pipeline_state() -> None:
    """Create the initial incremental-processing state."""

    current_time = datetime.now(timezone.utc).isoformat()

    with database_transaction() as connection:
        connection.execute(
            """
            INSERT INTO pipeline_state (
                pipeline_name,
                last_successful_watermark,
                last_pipeline_run_id,
                updated_at
            )
            VALUES (?, ?, ?, ?)
            ON CONFLICT(pipeline_name)
            DO NOTHING;
            """,
            (
                PIPELINE_NAME,
                None,
                None,
                current_time,
            ),
        )


def seed_reference_data() -> None:
    """Initialize the schema and seed all reference data."""

    initialize_database()
    seed_actions()
    seed_categories()
    seed_pipeline_state()


def print_seed_summary() -> None:
    """Display the seeded reference-data counts."""

    from pipeline.database import create_connection

    connection = create_connection()

    try:
        action_count = connection.execute(
            "SELECT COUNT(*) FROM dim_action;"
        ).fetchone()[0]

        category_count = connection.execute(
            "SELECT COUNT(*) FROM dim_category;"
        ).fetchone()[0]

        state_count = connection.execute(
            "SELECT COUNT(*) FROM pipeline_state;"
        ).fetchone()[0]

        print("SafeFlow AI reference data seeded successfully.")
        print(f"Actions:         {action_count}")
        print(f"Categories:      {category_count}")
        print(f"Pipeline states: {state_count}")

    finally:
        connection.close()


if __name__ == "__main__":
    seed_reference_data()
    print_seed_summary()
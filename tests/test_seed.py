"""Tests for SafeFlow AI reference-data seeding."""

from __future__ import annotations

import unittest

from pipeline.database import create_connection
from pipeline.seed import (
    ACTION_SEEDS,
    CATEGORY_SEEDS,
    PIPELINE_NAME,
    seed_reference_data,
)


class SeedReferenceDataTests(unittest.TestCase):
    """Verify reference data and idempotent seeding."""

    @classmethod
    def setUpClass(cls) -> None:
        seed_reference_data()

    def setUp(self) -> None:
        self.connection = create_connection()

    def tearDown(self) -> None:
        self.connection.close()

    def test_all_actions_are_seeded(self) -> None:
        """Verify that all four supported actions exist."""

        rows = self.connection.execute(
            """
            SELECT
                action_name,
                severity_rank,
                risk_level
            FROM dim_action;
            """
        ).fetchall()

        actual_actions = {
            (
                row["action_name"],
                row["severity_rank"],
                row["risk_level"],
            )
            for row in rows
        }

        expected_actions = {
            (
                action["action_name"],
                action["severity_rank"],
                action["risk_level"],
            )
            for action in ACTION_SEEDS
        }

        self.assertEqual(actual_actions, expected_actions)

    def test_all_categories_are_seeded(self) -> None:
        """Verify that all supported categories exist."""

        rows = self.connection.execute(
            """
            SELECT
                category_name,
                is_attack_category
            FROM dim_category;
            """
        ).fetchall()

        actual_categories = {
            (
                row["category_name"],
                row["is_attack_category"],
            )
            for row in rows
        }

        expected_categories = {
            (
                category["category_name"],
                category["is_attack_category"],
            )
            for category in CATEGORY_SEEDS
        }

        self.assertEqual(
            actual_categories,
            expected_categories,
        )

    def test_benign_category_is_not_an_attack(self) -> None:
        """Verify that benign prompts are classified correctly."""

        row = self.connection.execute(
            """
            SELECT is_attack_category
            FROM dim_category
            WHERE category_name = 'benign';
            """
        ).fetchone()

        self.assertIsNotNone(row)
        self.assertEqual(row["is_attack_category"], 0)

    def test_attack_categories_are_marked_as_attacks(self) -> None:
        """Verify that all non-benign categories are attack categories."""

        rows = self.connection.execute(
            """
            SELECT
                category_name,
                is_attack_category
            FROM dim_category
            WHERE category_name != 'benign';
            """
        ).fetchall()

        self.assertGreater(len(rows), 0)

        for row in rows:
            self.assertEqual(
                row["is_attack_category"],
                1,
                msg=(
                    f"{row['category_name']} should be marked "
                    "as an attack category."
                ),
            )

    def test_pipeline_state_exists_and_is_valid(self) -> None:
        """Verify that the pipeline state exists and remains valid."""

        row = self.connection.execute(
            """
            SELECT
                pipeline_name,
                last_successful_watermark,
                last_pipeline_run_id,
                updated_at
            FROM pipeline_state
            WHERE pipeline_name = ?;
            """,
            (PIPELINE_NAME,),
        ).fetchone()

        self.assertIsNotNone(row)
        self.assertEqual(
            row["pipeline_name"],
            PIPELINE_NAME,
        )
        self.assertIsNotNone(row["updated_at"])

        watermark = row["last_successful_watermark"]
        pipeline_run_id = row["last_pipeline_run_id"]

        if watermark is None:
            self.assertIsNone(pipeline_run_id)
        else:
            self.assertGreaterEqual(
                int(watermark),
                0,
            )
            self.assertIsNotNone(pipeline_run_id)

            audit_row = self.connection.execute(
                """
                SELECT
                    status,
                    watermark_end
                FROM pipeline_audit
                WHERE pipeline_run_id = ?;
                """,
                (pipeline_run_id,),
            ).fetchone()

            self.assertIsNotNone(audit_row)
            self.assertEqual(
                audit_row["status"],
                "SUCCESS",
            )
            self.assertEqual(
                audit_row["watermark_end"],
                watermark,
            )

    def test_seeding_is_idempotent(self) -> None:
        """Verify repeated seeding does not create duplicates."""

        seed_reference_data()
        seed_reference_data()

        action_count = self.connection.execute(
            "SELECT COUNT(*) FROM dim_action;"
        ).fetchone()[0]

        category_count = self.connection.execute(
            "SELECT COUNT(*) FROM dim_category;"
        ).fetchone()[0]

        state_count = self.connection.execute(
            """
            SELECT COUNT(*)
            FROM pipeline_state
            WHERE pipeline_name = ?;
            """,
            (PIPELINE_NAME,),
        ).fetchone()[0]

        self.assertEqual(action_count, len(ACTION_SEEDS))
        self.assertEqual(category_count, len(CATEGORY_SEEDS))
        self.assertEqual(state_count, 1)


if __name__ == "__main__":
    unittest.main()
"""Configuration utilities for the SafeFlow AI analytics pipeline."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "settings.json"


def load_settings() -> dict[str, Any]:
    """Load and validate the SafeFlow AI project settings."""

    if not CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Configuration file was not found: {CONFIG_PATH}"
        )

    with CONFIG_PATH.open(
        mode="r",
        encoding="utf-8",
    ) as config_file:
        settings = json.load(config_file)

    required_settings = {
        "project_name",
        "raw_trace_path",
        "database_path",
        "export_directory",
        "allowed_actions",
        "recognized_categories",
        "incremental_key",
    }

    missing_settings = required_settings - settings.keys()

    if missing_settings:
        missing_names = ", ".join(sorted(missing_settings))

        raise ValueError(
            f"Missing required configuration settings: {missing_names}"
        )

    if not isinstance(settings["allowed_actions"], list):
        raise TypeError(
            "'allowed_actions' must be a list."
        )

    if not isinstance(settings["recognized_categories"], list):
        raise TypeError(
            "'recognized_categories' must be a list."
        )

    return settings


def resolve_project_path(relative_path: str) -> Path:
    """Convert a configured relative path into an absolute project path."""

    return PROJECT_ROOT / relative_path


def get_database_path() -> Path:
    """Return the absolute SQLite database path."""

    settings = load_settings()
    database_path = resolve_project_path(settings["database_path"])

    database_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    return database_path


def get_trace_path() -> Path:
    """Return the absolute safety trace path."""

    settings = load_settings()

    return resolve_project_path(settings["raw_trace_path"])


def get_export_directory() -> Path:
    """Return the dashboard export directory."""

    settings = load_settings()
    export_directory = resolve_project_path(
        settings["export_directory"]
    )

    export_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    return export_directory
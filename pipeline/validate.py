"""Schema and data-quality validation for SafeFlow AI events."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from pipeline.config import load_settings


@dataclass(frozen=True)
class ValidationResult:
    """Validation result for one safety event."""

    is_valid: bool
    errors: list[str]


REQUIRED_FIELDS = {
    "request_id",
    "prompt",
    "category",
    "corpus",
    "pre_gen",
    "during_gen",
    "final_action",
    "latency_ms",
}


def validate_required_fields(
    event: dict[str, Any],
) -> list[str]:
    """Validate required top-level """
    errors: list[str] = []

    for field_name in sorted(REQUIRED_FIELDS):
        if field_name not in event:
            errors.append(
                f"Missing required field: {field_name}"
            )

    return errors


def validate_request_id(
    event: dict[str, Any],
) -> list[str]:
    """Validate the request identifier."""

    request_id = event.get("request_id")

    if not isinstance(request_id, str):
        return [
            "request_id must be a string."
        ]

    if not request_id.strip():
        return [
            "request_id cannot be empty."
        ]

    return []


def validate_prompt(
    event: dict[str, Any],
) -> list[str]:
    """Validate the source prompt."""

    prompt = event.get("prompt")

    if not isinstance(prompt, str):
        return [
            "prompt must be a string."
        ]

    if not prompt.strip():
        return [
            "prompt cannot be empty."
        ]

    return []


def validate_category(
    event: dict[str, Any],
) -> list[str]:
    """Validate the safety-event category."""

    settings = load_settings()
    recognized_categories = set(
        settings["recognized_categories"]
    )

    category = event.get("category")

    if not isinstance(category, str):
        return [
            "category must be a string."
        ]

    if category not in recognized_categories:
        return [
            f"Unsupported category: {category}"
        ]

    return []


def validate_corpus(
    event: dict[str, Any],
) -> list[str]:
    """Validate the source corpus."""

    corpus = event.get("corpus")

    if corpus not in {
        "attack",
        "benign",
    }:
        return [
            "corpus must be either 'attack' or 'benign'."
        ]

    category = event.get("category")

    if corpus == "benign" and category != "benign":
        return [
            "A benign corpus event must use the benign category."
        ]

    if corpus == "attack" and category == "benign":
        return [
            "An attack corpus event cannot use the benign category."
        ]

    return []


def validate_final_action(
    event: dict[str, Any],
) -> list[str]:
    """Validate the final safety action."""

    settings = load_settings()
    allowed_actions = set(
        settings["allowed_actions"]
    )

    final_action = event.get("final_action")

    if not isinstance(final_action, str):
        return [
            "final_action must be a string."
        ]

    if final_action not in allowed_actions:
        return [
            f"Unsupported final_action: {final_action}"
        ]

    return []


def validate_latency(
    event: dict[str, Any],
) -> list[str]:
    """Validate event-processing latency."""

    latency_ms = event.get("latency_ms")

    if isinstance(latency_ms, bool):
        return [
            "latency_ms must be numeric."
        ]

    if not isinstance(
        latency_ms,
        (int, float),
    ):
        return [
            "latency_ms must be numeric."
        ]

    if latency_ms < 0:
        return [
            "latency_ms cannot be negative."
        ]

    return []


def validate_pre_generation(
    event: dict[str, Any],
) -> list[str]:
    """Validate the pre-generation checkpoint."""

    pre_gen = event.get("pre_gen")

    if not isinstance(pre_gen, dict):
        return [
            "pre_gen must be a JSON object."
        ]

    errors: list[str] = []

    category = pre_gen.get("category")

    if not isinstance(category, str):
        errors.append(
            "pre_gen.category must be a string."
        )

    confidence = pre_gen.get("confidence")

    if isinstance(confidence, bool) or not isinstance(
        confidence,
        (int, float),
    ):
        errors.append(
            "pre_gen.confidence must be numeric."
        )

    elif not 0 <= confidence <= 1:
        errors.append(
            "pre_gen.confidence must be between 0 and 1."
        )

    return errors


def validate_during_generation(
    event: dict[str, Any],
) -> list[str]:
    """Validate the during checkpoint."""

    during_gen = event.get("during_gen")

    if not isinstance(during_gen, dict):
        return [
            "during_gen must be a JSON object."
        ]

    errors: list[str] = []

    terminated_early = during_gen.get(
        "terminated_early"
    )

    if not isinstance(terminated_early, bool):
        errors.append(
            "during_gen.terminated_early must be Boolean."
        )

        return errors

    if terminated_early:
        termination_evidence = (
            during_gen.get("termination_reason")
            or during_gen.get("matched_pattern")
            or during_gen.get("reason")
            or during_gen.get("matched_phrase")
            or during_gen.get("matched")
        )

        if not termination_evidence:
            errors.append(
                "Early termination requires termination evidence."
            )

    return errors


def validate_checkpoint_consistency(
    event: dict[str, Any],
) -> list[str]:
    """Validate logical consistency across safety checkpoints."""

    errors: list[str] = []

    final_action = event.get("final_action")
    pre_gen = event.get("pre_gen")
    during_gen = event.get("during_gen")
    post_gen = event.get("post_gen")

    if not isinstance(pre_gen, dict):
        return errors

    if not isinstance(during_gen, dict):
        return errors

    pre_gen_action = pre_gen.get("action")

    terminated_early = during_gen.get(
        "terminated_early",
        False,
    )

    if (
        pre_gen_action == "block"
        and final_action != "block"
    ):
        errors.append(
            "A pre-generation block must produce a final block."
        )

    if (
        pre_gen_action == "block"
        and post_gen is not None
    ):
        errors.append(
            "A pre-generation blocked event must not have "
            "post-generation results."
        )

    if (
        final_action == "redact"
        and post_gen is None
        and not terminated_early
    ):
        errors.append(
            "A redacted event requires either post-generation "
            "results or an early streaming termination."
        )

    if (
        terminated_early
        and final_action == "allow"
    ):
        errors.append(
            "An early-terminated response cannot have a final "
            "action of allow."
        )

    return errors


def validate_safety_event(
    event: object,
) -> ValidationResult:
    """Run all schema and data-quality checks on one event."""

    if not isinstance(event, dict):
        return ValidationResult(
            is_valid=False,
            errors=[
                "Safety event must be a JSON object."
            ],
        )

    errors: list[str] = []

    errors.extend(
        validate_required_fields(event)
    )

    errors.extend(
        validate_request_id(event)
    )

    errors.extend(
        validate_prompt(event)
    )

    errors.extend(
        validate_category(event)
    )

    errors.extend(
        validate_corpus(event)
    )

    errors.extend(
        validate_final_action(event)
    )

    errors.extend(
        validate_latency(event)
    )

    errors.extend(
        validate_pre_generation(event)
    )

    errors.extend(
        validate_during_generation(event)
    )

    errors.extend(
        validate_checkpoint_consistency(event)
    )

    unique_errors = list(
        dict.fromkeys(errors)
    )

    return ValidationResult(
        is_valid=not unique_errors,
        errors=unique_errors,
    )


def validate_raw_record(
    raw_record: str,
) -> ValidationResult:
    """Parse & validate a staged raw JSON record."""

    try:
        event = json.loads(raw_record)

    except json.JSONDecodeError as error:
        return ValidationResult(
            is_valid=False,
            errors=[
                f"Invalid raw JSON: {error.msg}"
            ],
        )

    return validate_safety_event(event)
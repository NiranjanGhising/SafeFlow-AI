"""Transform validated SafeFlow AI events into warehouse-ready records."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class TransformedSafetyEvent:
    """Warehouse-ready representation of one validated safety event."""

    request_id: str
    corpus: str
    fixture_id: str | None
    prompt: str
    final_output: str | None
    category_name: str
    action_name: str
    pre_gen_category: str
    pre_gen_confidence: float
    pre_gen_blocked: int
    terminated_early: int
    post_gen_checked: int
    is_attack: int
    was_allowed: int
    was_warned: int
    was_redacted: int
    was_blocked: int
    latency_ms: float
    latency_band: str
    event_timestamp: str
    date_key: int
    full_date: str
    day: int
    month: int
    month_name: str
    quarter: int
    year: int
    day_name: str


def classify_latency(latency_ms: float) -> str:
    """Classify local safety-gate latency for dashboard grouping."""

    if latency_ms < 5:
        return "Fast"

    if latency_ms < 20:
        return "Moderate"

    return "Slow"


def parse_iso_timestamp(timestamp: str) -> datetime:
    """Parse an ISO 8601 timestamp and reject missing timezone data."""

    try:
        parsed_timestamp = datetime.fromisoformat(timestamp)
    except ValueError as error:
        raise ValueError(
            f"Invalid event timestamp: {timestamp}"
        ) from error

    if parsed_timestamp.tzinfo is None:
        raise ValueError(
            "Event timestamp must contain timezone information."
        )

    return parsed_timestamp


def build_date_attributes(
    event_timestamp: str,
) -> dict[str, int | str]:
    """Build date-dimension attributes from one event timestamp."""

    parsed_timestamp = parse_iso_timestamp(event_timestamp)
    event_date = parsed_timestamp.date()

    return {
        "date_key": int(event_date.strftime("%Y%m%d")),
        "full_date": event_date.isoformat(),
        "day": event_date.day,
        "month": event_date.month,
        "month_name": event_date.strftime("%B"),
        "quarter": ((event_date.month - 1) // 3) + 1,
        "year": event_date.year,
        "day_name": event_date.strftime("%A"),
    }


def transform_safety_event(
    event: dict[str, Any],
    *,
    event_timestamp: str,
) -> TransformedSafetyEvent:
    """Transform one validated source event for warehouse loading."""

    request_id = str(event["request_id"])
    corpus = str(event["corpus"])
    category_name = str(event["category"])
    action_name = str(event["final_action"])
    prompt = str(event["prompt"])
    latency_ms = float(event["latency_ms"])

    fixture_value = event.get("fixture_id")
    fixture_id = (
        None
        if fixture_value is None
        else str(fixture_value)
    )

    output_value = event.get("final_output")
    final_output = (
        None
        if output_value is None
        else str(output_value)
    )

    pre_gen = event["pre_gen"]
    during_gen = event["during_gen"]
    post_gen = event.get("post_gen")

    if not isinstance(pre_gen, dict):
        raise TypeError("pre_gen must be a dictionary.")

    if not isinstance(during_gen, dict):
        raise TypeError("during_gen must be a dictionary.")

    pre_gen_category = str(pre_gen["category"])
    pre_gen_confidence = float(pre_gen["confidence"])
    terminated_early = int(
        bool(during_gen["terminated_early"])
    )
    post_gen_checked = int(post_gen is not None)

    pre_gen_blocked = int(
        action_name == "block"
        and post_gen is None
        and terminated_early == 0
    )

    action_flags = {
        "allow": 0,
        "warn": 0,
        "redact": 0,
        "block": 0,
    }
    action_flags[action_name] = 1

    date_attributes = build_date_attributes(
        event_timestamp
    )

    return TransformedSafetyEvent(
        request_id=request_id,
        corpus=corpus,
        fixture_id=fixture_id,
        prompt=prompt,
        final_output=final_output,
        category_name=category_name,
        action_name=action_name,
        pre_gen_category=pre_gen_category,
        pre_gen_confidence=pre_gen_confidence,
        pre_gen_blocked=pre_gen_blocked,
        terminated_early=terminated_early,
        post_gen_checked=post_gen_checked,
        is_attack=int(corpus == "attack"),
        was_allowed=action_flags["allow"],
        was_warned=action_flags["warn"],
        was_redacted=action_flags["redact"],
        was_blocked=action_flags["block"],
        latency_ms=latency_ms,
        latency_band=classify_latency(latency_ms),
        event_timestamp=event_timestamp,
        date_key=int(date_attributes["date_key"]),
        full_date=str(date_attributes["full_date"]),
        day=int(date_attributes["day"]),
        month=int(date_attributes["month"]),
        month_name=str(date_attributes["month_name"]),
        quarter=int(date_attributes["quarter"]),
        year=int(date_attributes["year"]),
        day_name=str(date_attributes["day_name"]),
    )


def transform_raw_record(
    raw_record: str,
    *,
    event_timestamp: str,
) -> TransformedSafetyEvent:
    """Parse one validated raw JSON record and transform it."""

    try:
        event = json.loads(raw_record)
    except json.JSONDecodeError as error:
        raise ValueError(
            f"Unable to transform invalid raw JSON: {error.msg}"
        ) from error

    if not isinstance(event, dict):
        raise TypeError(
            "The raw safety event must be a JSON object."
        )

    return transform_safety_event(
        event,
        event_timestamp=event_timestamp,
    )

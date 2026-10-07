"""Append one live Safety Gate event to the SafeFlow AI trace file."""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from app.safety_gate import SafetyGate, trace_to_dict
from pipeline.config import get_trace_path, load_settings


def load_trace_document(trace_path: Path) -> dict[str, Any]:
    """Load and verify the existing trace document."""

    if not trace_path.exists():
        raise FileNotFoundError(
            f"Trace file does not exist: {trace_path}. Run python main.py first."
        )

    payload = json.loads(trace_path.read_text(encoding="utf-8"))

    if not isinstance(payload, dict):
        raise TypeError("Trace root must be a JSON object.")

    if not isinstance(payload.get("traces"), list):
        raise TypeError("Trace document must contain a traces list.")

    return payload


def infer_event_labels(
    *,
    pre_gen_category: str,
    requested_category: str | None,
) -> tuple[str, str]:
    """Return a consistent corpus and category for the new event."""

    recognized = set(load_settings()["recognized_categories"])

    if requested_category is not None:
        if requested_category not in recognized:
            raise ValueError(f"Unsupported category: {requested_category}")
        category = requested_category
    elif pre_gen_category in recognized:
        category = pre_gen_category
    else:
        category = "benign"

    corpus = "benign" if category == "benign" else "attack"
    return corpus, category


def rebuild_summary(traces: list[dict[str, Any]]) -> dict[str, Any]:
    """Recalculate the source-trace summary after appending an event."""

    action_counts: Counter[str] = Counter()
    per_category: dict[str, Counter[str]] = defaultdict(Counter)
    terminations = 0
    benign_blocks = 0
    total_latency = 0.0

    for event in traces:
        action = str(event.get("final_action", "unknown"))
        category = str(event.get("category", "unknown"))
        corpus = str(event.get("corpus", "unknown"))
        total_latency += float(event.get("latency_ms", 0.0))
        during_gen = event.get("during_gen")

        action_counts[action] += 1
        per_category[category][action] += 1

        if isinstance(during_gen, dict) and during_gen.get("terminated_early") is True:
            terminations += 1

        if corpus == "benign" and action == "block":
            benign_blocks += 1

    total_requests = len(traces)

    return {
        "total_requests": total_requests,
        "action_counts": dict(action_counts),
        "terminations": terminations,
        "benign_blocks": benign_blocks,
        "avg_latency_ms": round(total_latency / total_requests, 3)
        if total_requests
        else 0.0,
        "per_category_outcome": {
            category: dict(counts)
            for category, counts in per_category.items()
        },
    }


def append_safety_event(
    prompt: str,
    *,
    requested_category: str | None = None,
    trace_path: Path | None = None,
) -> dict[str, Any]:
    """Run one prompt through the Safety Gate and append its trace."""

    destination = trace_path or get_trace_path()
    payload = load_trace_document(destination)

    trace_data = trace_to_dict(SafetyGate().handle(prompt))
    pre_gen = trace_data.get("pre_gen")
    pre_gen_category = (
        str(pre_gen.get("category", "benign"))
        if isinstance(pre_gen, dict)
        else "benign"
    )

    corpus, category = infer_event_labels(
        pre_gen_category=pre_gen_category,
        requested_category=requested_category,
    )

    event = {
        "corpus": corpus,
        "fixture_id": None,
        "category": category,
        **trace_data,
    }

    traces = payload["traces"]
    traces.append(event)
    payload["summary"] = rebuild_summary(traces)

    destination.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    return event


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser."""

    parser = argparse.ArgumentParser(
        description="Append one live Safety Gate event to gate_trace.json."
    )
    parser.add_argument(
        "--prompt",
        required=True,
        help="Prompt to evaluate through the Safety Gate.",
    )
    parser.add_argument(
        "--category",
        default=None,
        help="Optional known category override.",
    )
    return parser


def main() -> None:
    """Append one CLI-provided event and print its result."""

    args = build_parser().parse_args()
    event = append_safety_event(
        args.prompt,
        requested_category=args.category,
    )

    print("SafeFlow AI event appended")
    print(f"Request ID:   {event['request_id']}")
    print(f"Corpus:       {event['corpus']}")
    print(f"Category:     {event['category']}")
    print(f"Final action: {event['final_action']}")
    print(f"Latency ms:   {event['latency_ms']}")


if __name__ == "__main__":
    main()

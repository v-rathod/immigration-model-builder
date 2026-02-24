"""
Usage registry: lightweight event logger for dataset/model usage tracking.

Appends NDJSON lines to artifacts/metrics/usage_registry.ndjson and maintains
a compact JSON index at artifacts/metrics/usage_registry.json.

Event structure:
    {"ts":"<ISO8601>","task":"<str>","phase":"begin|end","inputs":[...],"outputs":[...],"metrics":{...}}
"""
from __future__ import annotations

import json
import pathlib
from datetime import datetime, timezone
from typing import Any

_METRICS_DIR = pathlib.Path("artifacts/metrics")
_NDJSON = _METRICS_DIR / "usage_registry.ndjson"
_INDEX = _METRICS_DIR / "usage_registry.json"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_event(event: dict) -> None:
    _METRICS_DIR.mkdir(parents=True, exist_ok=True)
    with _NDJSON.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, default=str) + "\n")
    _rebuild_index()


def _rebuild_index() -> None:
    """Read all NDJSON events and write a compact JSON index."""
    events: list[dict] = []
    if _NDJSON.exists():
        for line in _NDJSON.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    # build summary: task → {begin, end, inputs, outputs, metrics}
    tasks: dict[str, Any] = {}
    for ev in events:
        task = ev.get("task", "unknown")
        if task not in tasks:
            tasks[task] = {"inputs": [], "outputs": [], "metrics": {}}
        phase = ev.get("phase")
        if phase == "begin":
            tasks[task]["begin"] = ev.get("ts")
            tasks[task]["inputs"] = ev.get("inputs", [])
            tasks[task]["outputs"] = ev.get("outputs", [])
        elif phase == "end":
            tasks[task]["end"] = ev.get("ts")
            tasks[task]["metrics"].update(ev.get("metrics", {}))
        elif phase == "stub":
            # stub events store skip_reason in metrics
            tasks[task]["stub"] = ev.get("ts")
            tasks[task]["inputs"] = ev.get("inputs", [])
            tasks[task]["outputs"] = ev.get("outputs", [])
            tasks[task]["metrics"].update(ev.get("metrics", {}))
    index = {"generated": _now(), "tasks": tasks, "events": events}
    _INDEX.write_text(json.dumps(index, indent=2, default=str), encoding="utf-8")


def begin_task(task: str, inputs: list[str], outputs: list[str]) -> None:
    """Log the start of a task with its declared inputs and outputs."""
    event = {
        "ts": _now(),
        "task": task,
        "phase": "begin",
        "inputs": [str(i) for i in inputs],
        "outputs": [str(o) for o in outputs],
        "metrics": {},
    }
    _write_event(event)


def end_task(task: str, metrics: dict[str, Any] | None = None) -> None:
    """Log the end of a task with optional metrics."""
    event = {
        "ts": _now(),
        "task": task,
        "phase": "end",
        "inputs": [],
        "outputs": [],
        "metrics": metrics or {},
    }
    _write_event(event)


def log_stub(task: str, reason: str, inputs: list[str] | None = None,
             outputs: list[str] | None = None) -> None:
    """Log a skipped/stub task with a human-readable reason."""
    event = {
        "ts": _now(),
        "task": task,
        "phase": "stub",
        "inputs": [str(i) for i in (inputs or [])],
        "outputs": [str(o) for o in (outputs or [])],
        "metrics": {"skip_reason": reason},
    }
    _write_event(event)

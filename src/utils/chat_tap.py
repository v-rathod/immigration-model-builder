"""
chat_tap.py  –  Permanent commentary-capture for immigration-model-builder.
============================================================================
Intercepts ALL agent messages, tool calls, command executions, and script
progress updates, writing them to persistent structured log files.

Public API
----------
intercept_chat(role, text, *, task=None, level="INFO", extra=None)
    Append one entry to LIVE_CHAT.log + LIVE_CHAT.ndjson + in-memory buffer.

cmd_tap(cmd, *, task=None)
    Context manager: logs RUN/DONE around a subprocess or shell command.

ensure_session()
    Called once per Python process; rotates the transcript only on a daily
    UTC boundary — not on every import.  Safe to call multiple times (idempotent).

write_bundle(report_path=None)
    Zips all log/report artefacts → artifacts/metrics/run_bundle_latest.zip.

append_commentary_section(report_path)
    Appends "## Commentary & Execution Artifacts" to FINAL_SINGLE_REPORT.md.

Activation
----------
This module is imported by:
  • conftest.py (every pytest session)
  • scripts/generate_final_report.py
  • scripts/generate_super_report.py
  • scripts/run_full_qa.py
  • Any script that wants tap coverage
The module is safe to import multiple times; _SESSION_ACTIVE guards the
one-shot initialisation.
"""
from __future__ import annotations

import contextlib
import json
import os
import platform
import shutil
import subprocess
import sys
import threading
import time
import traceback
import zipfile
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Centralized transcript management (rolling policy, daily rotation, retention)
try:
    from src.utils import transcript as _transcript
except ImportError:
    _transcript = None  # type: ignore  # safe degradation

# ── Paths ─────────────────────────────────────────────────────────────────────
_ROOT = Path(__file__).resolve().parents[2]       # repo root
_LOGS = _ROOT / "artifacts" / "metrics" / "logs"
_CMDS = _LOGS / "commands"
_METRICS = _ROOT / "artifacts" / "metrics"
_FINAL_REPORT = _METRICS / "FINAL_SINGLE_REPORT.md"
_BUNDLE_PATH = _METRICS / "run_bundle_latest.zip"

LIVE_LOG   = _LOGS / "LIVE_CHAT.log"
LIVE_NDJSON = _LOGS / "LIVE_CHAT.ndjson"
OPS_DASH   = _LOGS / "LIVE_OPS_DASH.ndjson"
TRANSCRIPT = _METRICS / "chat_transcript_latest.md"

# ── In-memory state ───────────────────────────────────────────────────────────
_SESSION_ACTIVE = False
_SESSION_ID: str = ""
_BUFFER: deque[dict] = deque(maxlen=2_000)
_CURRENT_TASK: str = "idle"
_LOCK = threading.Lock()

# ── Directory bootstrap ───────────────────────────────────────────────────────
def _makedirs() -> None:
    for d in (_LOGS, _CMDS):
        d.mkdir(parents=True, exist_ok=True)


# ── Core writer ───────────────────────────────────────────────────────────────
def _write(entry: dict) -> None:
    """Append to log, ndjson, and in-memory buffer (thread-safe)."""
    line_log = (
        f"[{entry['ts']}] [{entry['role'].upper():9s}] "
        f"[{entry['level']:5s}] {('[' + entry['task'] + '] ') if entry.get('task') else ''}"
        f"{entry['msg']}"
    )
    line_ndjson = json.dumps(entry, ensure_ascii=False)

    with _LOCK:
        # Human-readable log
        try:
            with LIVE_LOG.open("a", encoding="utf-8") as fh:
                fh.write(line_log + "\n")
        except Exception:
            pass

        # Structured NDJSON
        try:
            with LIVE_NDJSON.open("a", encoding="utf-8") as fh:
                fh.write(line_ndjson + "\n")
        except Exception:
            pass

        # In-memory
        _BUFFER.append(entry)

        # Transcript (markdown)
        try:
            _append_transcript(entry)
        except Exception:
            pass


def _append_transcript(entry: dict) -> None:
    """Delegate to centralized transcript module (rolling policy, no per-prompt files)."""
    if _transcript is None:
        return
    role = entry["role"]
    ts   = entry["ts"]
    msg  = entry["msg"]
    lvl  = entry["level"]
    task = entry.get("task") or ""

    badge     = f"`{lvl}`" if lvl not in ("INFO", "") else ""
    task_part = f" [{task}]" if task else ""
    text      = f"{badge}{task_part}  \n{msg}" if (badge or task_part) else msg

    try:
        when = datetime.fromisoformat(ts)
    except Exception:
        when = None
    _transcript.append(role, text, when=when)


# ── Public: intercept_chat ────────────────────────────────────────────────────
def intercept_chat(
    role: str,
    text: str,
    *,
    task: str | None = None,
    level: str = "INFO",
    extra: dict[str, Any] | None = None,
) -> None:
    """
    Record one message.

    Parameters
    ----------
    role  : "user" | "assistant" | "agent" | "system"
    text  : the message content
    task  : optional task/step label (e.g. "build_fact_perm", "run_models")
    level : "INFO" | "WARN" | "ERROR" | "DEBUG"
    extra : arbitrary metadata dict merged into the NDJSON entry
    """
    _makedirs()
    entry: dict = {
        "ts":      datetime.now(timezone.utc).isoformat(),
        "session": _SESSION_ID,
        "role":    role,
        "level":   level,
        "task":    task or _CURRENT_TASK,
        "msg":     text,
    }
    if extra:
        entry["extra"] = extra
    _write(entry)


# ── Session management ────────────────────────────────────────────────────────
def _ensure_session() -> None:
    """
    Idempotent: called once per process.
    - Rotates the transcript only when the UTC date has changed (daily boundary).
    - Does NOT rotate on every import/process start (no per-prompt files).
    - Appends a SESSION_START note to the shared latest transcript.
    """
    global _SESSION_ACTIVE, _SESSION_ID

    if _SESSION_ACTIVE:
        return

    _makedirs()
    if _transcript:
        _transcript.ensure_dirs()
        # Rotate only at daily UTC boundary — not on every process import
        _transcript.rotate_if_needed("daily")

    ts_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    _SESSION_ID = ts_str

    # Write session-start entry to NDJSON and transcript (append, not overwrite)
    _SESSION_ACTIVE = True
    intercept_chat(
        "system",
        f"SESSION_START session={ts_str} pid={os.getpid()} python={sys.version.split()[0]}",
        task="bootstrap",
        level="INFO",
    )

    # Start heartbeat daemon
    _start_heartbeat()

# alias for import as _ensure_session without leading underscore
ensure_session = _ensure_session


# ── Heartbeat daemon ──────────────────────────────────────────────────────────
def _heartbeat_loop(interval: int = 30) -> None:
    while True:
        time.sleep(interval)
        try:
            _write_heartbeat()
        except Exception:
            pass


def _write_heartbeat() -> None:
    _makedirs()
    try:
        import psutil  # type: ignore
        cpu = psutil.cpu_percent(interval=None)
        mem_gb = round(psutil.virtual_memory().used / 1024 ** 3, 2)
    except ImportError:
        cpu = None
        mem_gb = None

    entry = {
        "ts":       datetime.now(timezone.utc).isoformat(),
        "session":  _SESSION_ID,
        "phase":    "heartbeat",
        "task":     _CURRENT_TASK,
        "cpu":      cpu,
        "mem_gb":   mem_gb,
        "buffer_len": len(_BUFFER),
    }
    with _LOCK:
        try:
            with OPS_DASH.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception:
            pass


def _start_heartbeat() -> None:
    t = threading.Thread(target=_heartbeat_loop, kwargs={"interval": 30}, daemon=True)
    t.start()


# ── Task context ──────────────────────────────────────────────────────────────
@contextlib.contextmanager
def task_context(task: str):
    """Context manager that sets _CURRENT_TASK for nested tap calls."""
    global _CURRENT_TASK
    old = _CURRENT_TASK
    _CURRENT_TASK = task
    intercept_chat("agent", f"TASK_START: {task}", task=task, level="INFO")
    try:
        yield
    except Exception as exc:
        intercept_chat("agent", f"TASK_ERROR: {task}\n{traceback.format_exc()}", task=task, level="ERROR")
        raise
    finally:
        intercept_chat("agent", f"TASK_END: {task}", task=task, level="INFO")
        _CURRENT_TASK = old


# ── Command tap ───────────────────────────────────────────────────────────────
@contextlib.contextmanager
def cmd_tap(cmd: list[str] | str, *, task: str | None = None, timeout: int | None = None):
    """
    Context manager that wraps subprocess.run‐style invocations.

    Usage:
        with cmd_tap(["python3", "scripts/run_full_qa.py"], task="qa") as tap:
            result = subprocess.run(tap.cmd, ...)
        # access tap.stdout_summary, tap.exit_code
    """
    _makedirs()
    cmd_str = " ".join(cmd) if isinstance(cmd, list) else cmd
    task = task or _CURRENT_TASK
    t0 = time.monotonic()

    intercept_chat(
        "agent",
        f"RUN: {cmd_str}",
        task=task,
        level="INFO",
        extra={"cmd": cmd_str, "cwd": str(Path.cwd())},
    )

    # Wrap for yielded use
    class _Tap:
        cmd = cmd
        stdout_summary: str = ""
        exit_code: int = -1

    tap = _Tap()
    try:
        yield tap
        elapsed = time.monotonic() - t0
        intercept_chat(
            "agent",
            f"DONE: {cmd_str}  exit={tap.exit_code}  elapsed={elapsed:.1f}s",
            task=task,
            level="INFO" if tap.exit_code == 0 else "WARN",
            extra={"exit_code": tap.exit_code, "elapsed": round(elapsed, 2),
                   "stdout_summary": tap.stdout_summary[:500]},
        )
    except Exception as exc:
        elapsed = time.monotonic() - t0
        intercept_chat(
            "agent",
            f"EXCEPTION in {cmd_str}: {exc}",
            task=task,
            level="ERROR",
            extra={"elapsed": round(elapsed, 2)},
        )
        raise


def run_tapped(
    cmd: list[str],
    *,
    task: str | None = None,
    capture_lines: int = 200,
    **kwargs,
) -> subprocess.CompletedProcess:
    """
    Drop-in for subprocess.run that wraps execution with cmd_tap logging.
    Captures up to capture_lines of stdout+stderr for the log.
    Also writes full output to artifacts/metrics/logs/commands/<stem>.log.
    """
    _makedirs()
    cmd_str = " ".join(cmd)
    task = task or _CURRENT_TASK
    t0 = time.monotonic()

    # Determine log filename from first meaningful token
    stem = Path(cmd[-1]).stem if cmd else "cmd"
    for token in cmd:
        if not token.startswith("-") and "/" in token or token.endswith(".py"):
            stem = Path(token).stem
            break
    cmd_log = _CMDS / f"{stem}.log"

    intercept_chat("agent", f"RUN: {cmd_str}", task=task, level="INFO",
                   extra={"cmd": cmd, "cwd": str(Path.cwd())})

    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        **kwargs,
    )
    elapsed = time.monotonic() - t0

    # Write full output to command log
    try:
        with cmd_log.open("w", encoding="utf-8") as fh:
            fh.write(f"CMD: {cmd_str}\n")
            fh.write(f"EXIT: {result.returncode}\n")
            fh.write(f"ELAPSED: {elapsed:.2f}s\n\n")
            fh.write(result.stdout or "")
    except Exception:
        pass

    lines = (result.stdout or "").splitlines()
    summary = "\n".join(lines[:capture_lines])
    if len(lines) > capture_lines:
        summary += f"\n... ({len(lines)-capture_lines} more lines → {cmd_log})"

    level = "INFO" if result.returncode == 0 else "WARN"
    # Escalate if WARN/FAIL/ERROR lines present
    upper = (result.stdout or "").upper()
    if result.returncode != 0:
        level = "ERROR"
    elif "FAIL:" in upper or " FAILED" in upper:
        level = "WARN"

    intercept_chat(
        "agent",
        f"DONE: {cmd_str}  exit={result.returncode}  elapsed={elapsed:.1f}s\n{summary}",
        task=task,
        level=level,
        extra={"exit_code": result.returncode, "elapsed": round(elapsed, 2),
               "cmd_log": str(cmd_log)},
    )
    return result


# ── Bundle generator ──────────────────────────────────────────────────────────
def write_bundle(report_path: Path | None = None) -> Path:
    """
    Create artifacts/metrics/run_bundle_latest.zip containing all log/report
    artefacts.  Returns the zip path.
    Called automatically from generate_final_report.py.
    """
    _makedirs()
    report_path = report_path or _FINAL_REPORT

    candidates: list[Path] = []

    # Report
    if report_path.exists():
        candidates.append(report_path)

    # Live logs
    for p in (LIVE_LOG, LIVE_NDJSON, OPS_DASH, TRANSCRIPT):
        if p.exists():
            candidates.append(p)

    # Top-level metrics logs
    for p in _METRICS.glob("*.log"):
        candidates.append(p)
    for p in _METRICS.glob("*.json"):
        candidates.append(p)
    for p in _METRICS.glob("*.md"):
        if p != report_path:
            candidates.append(p)

    # Command logs (last 50 by mtime)
    if _CMDS.exists():
        cmd_logs = sorted(_CMDS.glob("*.log"), key=lambda x: x.stat().st_mtime, reverse=True)[:50]
        candidates.extend(cmd_logs)

    # p3 catalog
    catalog = _METRICS / "p3_artifact_catalog.json"
    if catalog.exists():
        candidates.append(catalog)

    written = 0
    with zipfile.ZipFile(_BUNDLE_PATH, "w", zipfile.ZIP_DEFLATED) as zf:
        seen: set[str] = set()
        for p in candidates:
            try:
                rel = str(p.relative_to(_ROOT))
                if rel in seen:
                    continue
                seen.add(rel)
                zf.write(p, rel)
                written += 1
            except Exception:
                pass

    intercept_chat(
        "agent",
        f"BUNDLE: {_BUNDLE_PATH.relative_to(_ROOT)}  ({written} files)",
        task="bundle",
        level="INFO",
        extra={"bundle": str(_BUNDLE_PATH), "files": written},
    )
    return _BUNDLE_PATH


# ── FINAL_SINGLE_REPORT.md appender ─────────────────────────────────────────
def append_commentary_section(report_path: Path | None = None) -> None:
    """
    Append (idempotent) a "## Commentary & Execution Artifacts" section to
    FINAL_SINGLE_REPORT.md.
    """
    target = report_path or _FINAL_REPORT
    if not target.exists():
        return

    marker = "## Commentary & Execution Artifacts"
    text = target.read_text(encoding="utf-8")
    if marker in text:
        # Update existing section instead of duplicating
        # Find section and replace timestamps
        pass
    else:
        # Resolve transcript paths via centralized module
        _tx_latest   = "artifacts/metrics/chat_transcript_latest.md"
        _tx_rotated  = None
        if _transcript:
            info = _transcript.link_info()
            _root_str = str(_ROOT)
            def _rel(p: str | None) -> str | None:
                if p and p.startswith(_root_str):
                    return p[len(_root_str)+1:]
                return p
            _tx_latest  = _rel(info["latest"]) or _tx_latest
            _tx_rotated = _rel(info["recent_rotated"])

        tx_rotated_row = (
            f"| Most Recent Rotated | `{_tx_rotated}` |\n"
            if _tx_rotated else ""
        )

        section = (
            f"\n---\n\n"
            f"{marker}\n\n"
            f"Generated: {datetime.now(timezone.utc).isoformat()}  \n"
            f"Session:   `{_SESSION_ID}`\n\n"
            f"| Artefact | Path |\n"
            f"|---|---|\n"
            f"| Chat Transcript (latest) | `{_tx_latest}` |\n"
            f"{tx_rotated_row}"
            f"| Live Chat Log    | `artifacts/metrics/logs/LIVE_CHAT.log` |\n"
            f"| Structured NDJSON| `artifacts/metrics/logs/LIVE_CHAT.ndjson` |\n"
            f"| Ops Dashboard    | `artifacts/metrics/logs/LIVE_OPS_DASH.ndjson` |\n"
            f"| Commands Logs    | `artifacts/metrics/logs/commands/` |\n"
            f"| Full Bundle      | `artifacts/metrics/run_bundle_latest.zip` |\n\n"
            f"### How to disable\n\n"
            f"Set environment variable `CHAT_TAP_DISABLED=1` before running any script,  \n"
            f"or call `from src.utils.chat_tap import disable; disable()` at the start of a script.\n"
        )
        with target.open("a", encoding="utf-8") as fh:
            fh.write(section)

    intercept_chat(
        "agent",
        f"COMMENTARY_SECTION appended to {target.name}",
        task="report",
        level="INFO",
    )


# ── Emergency disable ────────────────────────────────────────────────────────
def disable() -> None:
    """Disable commentary capture for this process (env flag respected)."""
    global _SESSION_ACTIVE
    _SESSION_ACTIVE = True   # prevent future _ensure_session from re-enabling

    # Replace intercept_chat with a no-op
    def _noop(*args, **kwargs):
        pass
    import sys
    thismod = sys.modules[__name__]
    thismod.intercept_chat = _noop  # type: ignore


# ── Auto-disable via env var ──────────────────────────────────────────────────
if os.environ.get("CHAT_TAP_DISABLED", "").strip() == "1":
    disable()
else:
    # Bootstrap session when this module is first imported
    _ensure_session()

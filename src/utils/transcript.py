"""
transcript.py  –  Centralized rolling transcript management.
=============================================================
Provides a single append-only "latest" transcript file with:
  • Daily rotation (UTC boundary)
  • Explicit rotation on "finalize" or "explicit" reason
  • Retention: keep 10 most-recent daily transcripts; delete older ones
  • No per-prompt file creation — callers always append to latest

Public API
----------
get_paths()            → dict of dir/latest/rotated_glob path strings
ensure_dirs()          → create metrics dir if absent (idempotent)
append(role, text, when=None)
                       → write one entry to chat_transcript_latest.md
rotate_if_needed(reason: str) → bool  (True if rotation occurred)
link_info()            → {"latest": "...", "recent_rotated": "... or null"}
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ── Paths ─────────────────────────────────────────────────────────────────────
_ROOT     = Path(__file__).resolve().parents[2]
_METRICS  = _ROOT / "artifacts" / "metrics"
_LATEST   = _METRICS / "chat_transcript_latest.md"
_GLOB     = "chat_transcript_*.md"       # rotated archives match this glob
_LATEST_NAME = "chat_transcript_latest.md"

_LOCK = threading.Lock()
_RETENTION = 10     # keep N most-recent rotated archives


# ── Helpers ───────────────────────────────────────────────────────────────────
def _rotated_files() -> list[Path]:
    """All rotated archives, sorted newest first. Excludes latest."""
    return sorted(
        [
            p for p in _METRICS.glob(_GLOB)
            if p.name != _LATEST_NAME
        ],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )


def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def _mtime_date(p: Path) -> str:
    """UTC date string of file mtime."""
    return datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc).strftime("%Y%m%d")


# ── Public API ────────────────────────────────────────────────────────────────
def get_paths() -> dict[str, str]:
    """Return canonical path strings for the transcript system."""
    return {
        "dir":          str(_METRICS),
        "latest":       str(_LATEST),
        "rotated_glob": str(_METRICS / _GLOB),
    }


def ensure_dirs() -> None:
    """Create the metrics directory if it does not exist (idempotent)."""
    _METRICS.mkdir(parents=True, exist_ok=True)


def append(role: str, text: str, when: Optional[datetime] = None) -> None:
    """
    Append one entry to chat_transcript_latest.md.

    Parameters
    ----------
    role : "user" | "assistant" | "agent" | "system" | any label
    text : the message content (written as-is; caller may use markdown)
    when : UTC datetime for the timestamp; defaults to now
    """
    ensure_dirs()
    ts = (when or datetime.now(timezone.utc)).isoformat()

    prefix = {
        "user":      "**User**",
        "assistant": "**Copilot**",
        "system":    "*System*",
        "agent":     "> **Agent**",
    }.get(role, f"**{role}**")

    header = f"### [{ts}] {prefix}"
    body   = f"{text}\n"
    block  = f"{header}\n\n{body}\n"

    with _LOCK:
        try:
            with _LATEST.open("a", encoding="utf-8") as fh:
                fh.write(block)
        except Exception:
            pass


def rotate_if_needed(reason: str = "daily") -> bool:
    """
    Rotate the active transcript when:
      • The file's UTC mtime date < today's UTC date  (daily boundary), OR
      • reason is "finalize" or "explicit"

    After rotation:
      • Renames latest → chat_transcript_YYYYMMDD.md
      • Enforces _RETENTION limit (deletes oldest beyond limit)
      • Creates a fresh latest with a one-line banner

    Returns True if rotation occurred, False otherwise.
    """
    ensure_dirs()
    with _LOCK:
        force = reason in ("finalize", "explicit")
        needs_rotate = force

        if not force and _LATEST.exists() and _LATEST.stat().st_size > 20:
            if _mtime_date(_LATEST) < _today_utc():
                needs_rotate = True

        if not needs_rotate:
            return False

        # Determine archive name — avoid collisions by appending _N
        date_str = _today_utc()
        archive   = _METRICS / f"chat_transcript_{date_str}.md"
        counter   = 0
        while archive.exists():
            counter += 1
            archive = _METRICS / f"chat_transcript_{date_str}_{counter}.md"

        # Move latest → archive
        if _LATEST.exists() and _LATEST.stat().st_size > 20:
            try:
                _LATEST.rename(archive)
            except Exception:
                # fallback: copy then truncate
                try:
                    import shutil
                    shutil.copy2(_LATEST, archive)
                except Exception:
                    pass

        # Enforce retention
        _enforce_retention()

        # Create fresh latest
        ts_now  = datetime.now(timezone.utc).isoformat()
        banner  = (
            f"# Chat Transcript\n\n"
            f"### New transcript started {ts_now} (reason={reason})\n\n"
        )
        try:
            with _LATEST.open("w", encoding="utf-8") as fh:
                fh.write(banner)
        except Exception:
            pass

    return True


def _enforce_retention() -> None:
    """Delete rotated archives beyond _RETENTION limit. Called inside _LOCK."""
    archived = _rotated_files()   # newest first
    for old in archived[_RETENTION:]:
        try:
            old.unlink()
        except Exception:
            pass


def link_info() -> dict[str, str | None]:
    """
    Return a dict with:
      latest:          absolute path to chat_transcript_latest.md
      recent_rotated:  absolute path to most recent rotated archive, or None
    """
    ensure_dirs()
    recent = _rotated_files()
    return {
        "latest":         str(_LATEST),
        "recent_rotated": str(recent[0]) if recent else None,
    }

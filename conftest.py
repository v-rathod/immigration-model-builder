"""
conftest.py  –  Root-level pytest configuration for immigration-model-builder.
===============================================================================
Activates commentary-capture (chat_tap) automatically on every pytest run.
No flags needed — runs unconditionally unless CHAT_TAP_DISABLED=1.
"""
from __future__ import annotations

import sys
from pathlib import Path

# Ensure src/ is importable even when invoked from repo root
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# ── Activate commentary capture ───────────────────────────────────────────────
try:
    from src.utils import chat_tap as _tap

    _tap._ensure_session()
    _tap.intercept_chat(
        "system",
        f"pytest session started  args={sys.argv}",
        task="pytest",
        level="INFO",
    )
except Exception as _e:
    # Never block tests because of tap failures
    pass


# ── Pytest hooks ─────────────────────────────────────────────────────────────
def pytest_runtest_logreport(report):
    """Log each test result to the tap."""
    if report.when != "call":
        return
    try:
        from src.utils import chat_tap as _tap  # noqa: F401

        level = "INFO"
        status = "PASS"
        if report.failed:
            level = "ERROR"
            status = "FAIL"
        elif report.skipped:
            level = "INFO"
            status = "SKIP"

        msg = f"TEST {status}: {report.nodeid}"
        if report.failed and report.longreprtext:
            msg += f"\n{report.longreprtext[:400]}"

        _tap.intercept_chat("agent", msg, task="pytest", level=level)
    except Exception:
        pass


def pytest_sessionfinish(session, exitstatus):
    """Log final summary and flush any pending state."""
    try:
        from src.utils import chat_tap as _tap  # noqa: F401

        passed  = session.testscollected - session.testsfailed
        _tap.intercept_chat(
            "agent",
            f"pytest FINISHED: collected={session.testscollected} "
            f"failed={session.testsfailed} exit={exitstatus}",
            task="pytest",
            level="INFO" if exitstatus == 0 else "WARN",
        )
    except Exception:
        pass

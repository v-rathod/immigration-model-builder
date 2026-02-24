"""
pythonstartup_chat_tap.py
=========================
Auto-bootstrap for interactive Python sessions in this workspace.
Referenced by PYTHONSTARTUP env var in .vscode/settings.json.

This file is sourced automatically by CPython on startup.
It activates commentary capture without any explicit call.
"""
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    from src.utils import chat_tap as _tap
    _tap._ensure_session()
    _tap.intercept_chat("system", "PYTHONSTARTUP bootstrap in interactive session",
                        task="bootstrap", level="INFO")
except Exception:
    pass

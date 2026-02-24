#!/usr/bin/env python3
import sys
from pathlib import Path

# Ensure project root is on sys.path so `src.*` imports resolve
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from datetime import datetime, timezone
import json
from src.utils.objective_loader import load_objective

def main():
    obj = load_objective()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    banner = {
        "ts": now,
        "north_star": obj.get("program", {}).get("north_star"),
        "p3_features": obj.get("phases", {}).get("P3", {}).get("planned_features", []),
        "p2_quality_gates": obj.get("phases", {}).get("P2", {}).get("quality_gates", {}),
        "guidance": obj.get("agent_guidance", {}).get("defaults", [])
    }
    print("[Program Objective Loaded]")
    print(json.dumps(banner, indent=2))

if __name__ == "__main__":
    main()

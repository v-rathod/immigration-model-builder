from pathlib import Path
import yaml

OBJ_YAML = Path("configs/project_objective_P1_P2_P3.yaml")

def load_objective():
    if not OBJ_YAML.exists():
        return {"error": f"Objective file not found: {OBJ_YAML.as_posix()}"}
    with OBJ_YAML.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)

"""Configuration for ASE-ETL orchestration."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

ORCHESTRATION_ROOT = (
    PROJECT_ROOT / "data" / "orchestration"
)

LOG_ROOT = ORCHESTRATION_ROOT / "logs"

STATE_ROOT = ORCHESTRATION_ROOT / "state"

LOG_ROOT.mkdir(parents=True, exist_ok=True)
STATE_ROOT.mkdir(parents=True, exist_ok=True)
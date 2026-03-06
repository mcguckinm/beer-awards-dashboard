from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone
import json

ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = ROOT / "data" / "raw"
DATA_CLEAN = ROOT /"data" / "clean"
DB_DIR = ROOT / "db"
CONFIG_DIR = ROOT / "config"

for p in [DATA_RAW, DATA_CLEAN, DB_DIR, CONFIG_DIR]:
    p.mkdir(parents=True, exist_ok=True)

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def load_settings() -> dict:
    return json.loads((CONFIG_DIR / "settings.json").read_text(encoding="utf-8"))
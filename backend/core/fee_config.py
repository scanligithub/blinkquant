"""FeeSchedule YAML configuration loader."""

import datetime
from pathlib import Path
from typing import Optional, Union

import yaml

from core.backtest_types import FeeConfig, FeeSchedule


def load_fee_schedule(path: Union[str, Path]) -> FeeSchedule:
    """Load FeeSchedule from YAML file.

    YAML schema:
        fee_schedule:
          - date_start: "2023-08-28"
            commission_rate: 0.0008
            commission_min: 5.0
            stamp_tax_rate: 0.0005
            transfer_fee_rate: 0.00001
          - date_start: "2000-01-01"
            commission_rate: 0.0003
            commission_min: 5.0
            stamp_tax_rate: 0.001
            transfer_fee_rate: 0.00001
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Fee schedule config not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    entries_data = data.get("fee_schedule", [])
    if not entries_data:
        raise ValueError(f"Fee schedule config has no entries: {path}")

    entries = []
    for entry in entries_data:
        date_str = entry.get("date_start")
        if date_str:
            date_start = datetime.date.fromisoformat(str(date_str))
        else:
            date_start = None

        entries.append(FeeConfig(
            commission_rate=float(entry.get("commission_rate", 0.00025)),
            commission_min=float(entry.get("commission_min", 5.0)),
            stamp_tax_rate=float(entry.get("stamp_tax_rate", 0.0005)),
            transfer_fee_rate=float(entry.get("transfer_fee_rate", 0.00001)),
            date_start=date_start,
        ))

    return FeeSchedule(entries)


def save_fee_schedule(schedule: FeeSchedule, path: Union[str, Path]) -> None:
    """Save FeeSchedule to YAML file."""
    path = Path(path)
    entries = []
    for entry in schedule.entries:
        d = {
            "commission_rate": entry.commission_rate,
            "commission_min": entry.commission_min,
            "stamp_tax_rate": entry.stamp_tax_rate,
            "transfer_fee_rate": entry.transfer_fee_rate,
        }
        if entry.date_start:
            d["date_start"] = entry.date_start.isoformat()
        entries.append(d)

    data = {"fee_schedule": entries}
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True)

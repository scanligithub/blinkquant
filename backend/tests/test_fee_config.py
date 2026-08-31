"""Tests for FeeSchedule YAML configuration loader."""

import datetime
import tempfile
from pathlib import Path

import yaml
import pytest

from core.fee_config import load_fee_schedule, save_fee_schedule
from core.backtest_types import FeeConfig, FeeSchedule


@pytest.fixture
def tmp_yaml(tmp_path: Path) -> Path:
    """Create a temporary fee schedule YAML file."""
    data = {
        "fee_schedule": [
            {
                "date_start": "2023-08-28",
                "commission_rate": 0.0008,
                "commission_min": 5.0,
                "stamp_tax_rate": 0.0005,
                "transfer_fee_rate": 0.00001,
            },
            {
                "date_start": "2000-01-01",
                "commission_rate": 0.00025,
                "commission_min": 5.0,
                "stamp_tax_rate": 0.001,
                "transfer_fee_rate": 0.00001,
            },
        ]
    }
    path = tmp_path / "fee_schedule.yaml"
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(data, f, default_flow_style=False)
    return path


class TestLoadFeeSchedule:
    """Tests for load_fee_schedule."""

    def test_load_basic(self, tmp_yaml: Path):
        schedule = load_fee_schedule(tmp_yaml)
        assert isinstance(schedule, FeeSchedule)
        assert len(schedule.entries) == 2

    def test_load_sorted_by_date(self, tmp_yaml: Path):
        schedule = load_fee_schedule(tmp_yaml)
        dates = [e.date_start for e in schedule.entries]
        assert dates == sorted(dates)

    def test_get_fee_config_post_2023(self, tmp_yaml: Path):
        schedule = load_fee_schedule(tmp_yaml)
        config = schedule.get_fee_config(datetime.date(2024, 1, 1))
        assert config.commission_rate == 0.0008
        assert config.stamp_tax_rate == 0.0005

    def test_get_fee_config_pre_2023(self, tmp_yaml: Path):
        schedule = load_fee_schedule(tmp_yaml)
        config = schedule.get_fee_config(datetime.date(2023, 1, 1))
        assert config.commission_rate == 0.00025
        assert config.stamp_tax_rate == 0.001

    def test_get_fee_config_exact_boundary(self, tmp_yaml: Path):
        schedule = load_fee_schedule(tmp_yaml)
        config = schedule.get_fee_config(datetime.date(2023, 8, 28))
        assert config.commission_rate == 0.0008

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            load_fee_schedule("/nonexistent/fee_schedule.yaml")

    def test_empty_entries_raises(self, tmp_path: Path):
        path = tmp_path / "empty.yaml"
        with open(path, "w") as f:
            yaml.dump({"fee_schedule": []}, f)
        with pytest.raises(ValueError, match="no entries"):
            load_fee_schedule(path)


class TestSaveFeeSchedule:
    """Tests for save_fee_schedule."""

    def test_roundtrip(self, tmp_path: Path):
        original = FeeSchedule([
            FeeConfig(
                commission_rate=0.001,
                commission_min=10.0,
                stamp_tax_rate=0.001,
                transfer_fee_rate=0.00001,
                date_start=datetime.date(2024, 1, 1),
            ),
        ])
        path = tmp_path / "saved.yaml"
        save_fee_schedule(original, path)
        loaded = load_fee_schedule(path)
        assert len(loaded.entries) == 1
        assert loaded.entries[0].commission_rate == 0.001
        assert loaded.entries[0].date_start == datetime.date(2024, 1, 1)


class TestDefaultConfig:
    """Tests for the default config file."""

    def test_default_config_loads(self):
        config_path = Path(__file__).parent.parent.parent / "config" / "fee_schedule.yaml"
        if config_path.exists():
            schedule = load_fee_schedule(config_path)
            assert len(schedule.entries) >= 1
            # Verify known boundary
            config = schedule.get_fee_config(datetime.date(2024, 6, 1))
            assert config.commission_rate == 0.0008

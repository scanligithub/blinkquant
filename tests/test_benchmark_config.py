import pytest

from core.backtest_config import BenchmarkConfig


def test_benchmark_defaults_to_disabled_000300():
    cfg = BenchmarkConfig()
    assert cfg.enabled is False
    assert cfg.type == "index"
    assert cfg.index_id == "000300"


def test_benchmark_round_trip():
    cfg = BenchmarkConfig.from_dict(
        {"enabled": True, "type": "index", "index_id": "CSI300"}
    )
    assert cfg.to_dict() == {
        "enabled": True,
        "type": "index",
        "index_id": "CSI300",
    }


def test_enabled_benchmark_requires_index_id():
    with pytest.raises(ValueError, match="index_id"):
        BenchmarkConfig(enabled=True, index_id="   ")


def test_only_index_benchmark_type_is_supported():
    with pytest.raises(ValueError, match="benchmark.type"):
        BenchmarkConfig(type="fund")

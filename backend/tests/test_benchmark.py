import datetime as dt

import polars as pl

from core.backtest_config import BacktestConfig, BenchmarkConfig
from core.index_price_store import IndexPriceStore
from core.strategy import StrategyDefinition


def _strategy():
    return StrategyDefinition.from_dict({
        "universe": {"type": "all_a"},
        "entry": {
            "condition": "CLOSE > MA(CLOSE,20)",
            "trigger": "condition",
            "timeframe": "D",
        },
        "sizing": {"method": "top_n_equal_weight", "max_positions": 20},
        "rebalance": {"frequency": "weekly"},
        "mode": "target_portfolio",
    })


def test_benchmark_config_round_trip():
    config = BacktestConfig(
        start_date=dt.date(2024, 1, 2),
        end_signal_date=dt.date(2024, 3, 29),
        initial_cash=1_000_000,
        strategy=_strategy(),
        benchmark=BenchmarkConfig(enabled=True, type="index", index_id="CSI300"),
    )
    data = config.to_dict()
    assert data["benchmark"] == {
        "enabled": True,
        "type": "index",
        "index_id": "CSI300",
    }
    restored = BacktestConfig.from_dict(data)
    assert restored.benchmark.enabled is True
    assert restored.benchmark.index_id == "CSI300"


def test_index_price_store_alias_and_returns(tmp_path):
    frame = pl.DataFrame({
        "date": ["2024-01-02", "2024-01-03", "2024-01-04"],
        "code": ["sh.000300"] * 3,
        "open": [100.0, 101.0, 103.0],
        "high": [100.0, 101.0, 103.0],
        "low": [100.0, 101.0, 103.0],
        "close": [100.0, 101.0, 103.0],
        "volume": [1.0, 1.0, 1.0],
        "amount": [1.0, 1.0, 1.0],
    })
    frame.write_parquet(tmp_path / "index_kline_2024.parquet")

    store = IndexPriceStore(data_root=str(tmp_path))
    loaded = store.load_close_window("CSI300", dt.date(2024, 1, 2), dt.date(2024, 1, 4))
    assert loaded["code"].unique().to_list() == ["sh.000300"]

    series, cumulative = store.load_returns(
        "000300",
        [dt.date(2024, 1, 2), dt.date(2024, 1, 3), dt.date(2024, 1, 4)],
    )
    assert series["benchmark_return"].to_list()[0] == 0.0
    assert abs(cumulative - 0.03) < 1e-12


def test_index_price_store_rejects_missing_index(tmp_path):
    frame = pl.DataFrame({
        "date": ["2024-01-02"],
        "code": ["sh.000001"],
        "close": [100.0],
    })
    frame.write_parquet(tmp_path / "index_kline_2024.parquet")
    store = IndexPriceStore(data_root=str(tmp_path))
    try:
        store.load_close_window("000300", dt.date(2024, 1, 2), dt.date(2024, 1, 2))
    except ValueError as exc:
        assert "no data" in str(exc)
    else:
        raise AssertionError("missing benchmark index must fail")

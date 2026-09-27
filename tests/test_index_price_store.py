import datetime as dt

import polars as pl
import pytest

from core.index_price_store import IndexPriceStore


def _write_index(path, rows):
    pl.DataFrame(rows).write_parquet(path / "index_kline_2024.parquet")


def test_benchmark_returns_include_prior_trading_close(tmp_path):
    _write_index(
        tmp_path,
        {
            "date": [dt.date(2023, 12, 29), dt.date(2024, 1, 2), dt.date(2024, 1, 3)],
            "code": ["sh.000300"] * 3,
            "close": [100.0, 105.0, 107.1],
        },
    )
    store = IndexPriceStore(data_root=str(tmp_path))
    series, cumulative = store.load_returns(
        "000300",
        [dt.date(2024, 1, 2), dt.date(2024, 1, 3)],
    )

    assert series["benchmark_return"].to_list() == pytest.approx([0.05, 0.02])
    assert cumulative == pytest.approx(0.071)


def test_benchmark_alias_is_canonicalized(tmp_path):
    _write_index(
        tmp_path,
        {
            "date": [dt.date(2024, 1, 2)],
            "code": ["sh.000300"],
            "close": [100.0],
        },
    )
    store = IndexPriceStore(data_root=str(tmp_path))
    df = store.load_close_window(
        "CSI300",
        dt.date(2024, 1, 2),
        dt.date(2024, 1, 2),
    )
    assert df["code"].to_list() == ["sh.000300"]


def test_unknown_or_empty_benchmark_fails_fast(tmp_path):
    _write_index(
        tmp_path,
        {
            "date": [dt.date(2024, 1, 2)],
            "code": ["sh.000300"],
            "close": [100.0],
        },
    )
    store = IndexPriceStore(data_root=str(tmp_path))
    with pytest.raises(ValueError, match="has no data"):
        store.load_close_window(
            "000905",
            dt.date(2024, 1, 2),
            dt.date(2024, 1, 2),
        )


def test_missing_later_valuation_observation_is_not_zero_filled(tmp_path):
    _write_index(
        tmp_path,
        {
            "date": [dt.date(2023, 12, 29), dt.date(2024, 1, 2), dt.date(2024, 1, 4)],
            "code": ["sh.000300"] * 3,
            "close": [100.0, 105.0, 110.0],
        },
    )
    store = IndexPriceStore(data_root=str(tmp_path))
    with pytest.raises(ValueError, match="missing trading-day observations"):
        store.load_returns(
            "000300",
            [dt.date(2024, 1, 2), dt.date(2024, 1, 3), dt.date(2024, 1, 4)],
        )

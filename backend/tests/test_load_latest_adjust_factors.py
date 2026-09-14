"""Tests for load_latest_adjust_factors window trimming (Task 4)."""
import datetime
import tempfile
import polars as pl
from core.raw_price_store import RawPriceStore


def _make_parquet(tmpdir: str, year: int, codes: list[str], adj_factors: dict[str, float]):
    """Create a parquet file with adjustFactor column for testing."""
    rows = []
    for code in codes:
        rows.append({
            "date": datetime.date(year, 1, 2),
            "code": code,
            "open": 10.0,
            "high": 10.5,
            "low": 9.5,
            "close": 10.0,
            "volume": 1_000_000.0,
            "amount": 10_000_000.0,
            "adjustFactor": adj_factors.get(code, 1.0),
        })
    df = pl.DataFrame(rows)
    df.write_parquet(f"{tmpdir}/stock_kline_{year}.parquet")


def test_load_latest_adjust_factors_no_args_backward_compat():
    """Calling without args should still work (backward compatibility)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        _make_parquet(tmpdir, 2024, ["sh.600000"], {"sh.600000": 1.2})
        store = RawPriceStore(data_root=tmpdir)
        result = store.load_latest_adjust_factors()
        assert "sh.600000" in result
        assert result["sh.600000"] == 1.2


def test_load_latest_adjust_factors_with_window():
    """With start/end, only scan relevant years."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create data across multiple years
        _make_parquet(tmpdir, 2022, ["sh.600000"], {"sh.600000": 1.0})
        _make_parquet(tmpdir, 2023, ["sh.600000"], {"sh.600000": 1.1})
        _make_parquet(tmpdir, 2024, ["sh.600000"], {"sh.600000": 1.2})
        _make_parquet(tmpdir, 2025, ["sh.600000"], {"sh.600000": 1.3})

        store = RawPriceStore(data_root=tmpdir)

        # Window: 2024 only → should scan 2023-2025 (start.year-1 to end.year+1)
        result = store.load_latest_adjust_factors(
            start=datetime.date(2024, 1, 2),
            end=datetime.date(2024, 12, 31),
        )
        assert "sh.600000" in result
        # Should get the latest within scanned range (2025 data if scanned)
        # Since we scan 2023-2025, the latest is 2025's value = 1.3
        assert result["sh.600000"] == 1.3


def test_load_latest_adjust_factors_narrow_window():
    """Narrow window scans fewer years than full scan."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create data in 2020 and 2025 only
        _make_parquet(tmpdir, 2020, ["sh.600000"], {"sh.600000": 1.0})
        _make_parquet(tmpdir, 2025, ["sh.600000"], {"sh.600000": 2.0})

        store_full = RawPriceStore(data_root=tmpdir)
        store_narrow = RawPriceStore(data_root=tmpdir)

        # Full scan (no args) → sees both years, latest = 2.0
        result_full = store_full.load_latest_adjust_factors()
        assert result_full["sh.600000"] == 2.0

        # Narrow window: 2025 only → scans 2024-2026, only finds 2025 data
        result_narrow = store_narrow.load_latest_adjust_factors(
            start=datetime.date(2025, 1, 2),
            end=datetime.date(2025, 12, 31),
        )
        assert result_narrow["sh.600000"] == 2.0


def test_load_latest_adjust_factors_multiple_codes():
    """Window trimming works with multiple codes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        _make_parquet(tmpdir, 2023, ["sh.600000", "sz.000001"], 
                      {"sh.600000": 1.1, "sz.000001": 2.1})
        _make_parquet(tmpdir, 2024, ["sh.600000", "sz.000001"],
                      {"sh.600000": 1.2, "sz.000001": 2.2})

        store = RawPriceStore(data_root=tmpdir)
        result = store.load_latest_adjust_factors(
            start=datetime.date(2024, 1, 2),
            end=datetime.date(2024, 12, 31),
        )
        assert result["sh.600000"] == 1.2
        assert result["sz.000001"] == 2.2


def test_load_latest_adjust_factors_empty_window():
    """Window with no matching data returns empty dict."""
    with tempfile.TemporaryDirectory() as tmpdir:
        _make_parquet(tmpdir, 2023, ["sh.600000"], {"sh.600000": 1.1})

        store = RawPriceStore(data_root=tmpdir)
        # Window far from data
        result = store.load_latest_adjust_factors(
            start=datetime.date(2030, 1, 2),
            end=datetime.date(2030, 12, 31),
        )
        assert result == {}

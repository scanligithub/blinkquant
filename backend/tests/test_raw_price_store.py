import datetime
import tempfile
import polars as pl
from core.raw_price_store import RawPriceStore


def test_scan_window_returns_lazyframe_with_correct_schema():
    with tempfile.TemporaryDirectory() as tmpdir:
        df = pl.DataFrame({
            "date": [datetime.date(2025,1,2), datetime.date(2025,1,3)] * 2,
            "code": ["sh.600000"]*2 + ["sz.000001"]*2,
            "open": [10.0, 10.5, 20.0, 20.5],
            "high": [10.2, 10.7, 20.2, 20.7],
            "low": [9.8, 10.3, 19.8, 20.3],
            "close": [10.1, 10.6, 20.1, 20.6],
            "volume": [1000000]*4,
            "amount": [10000000]*4,
        })
        path = f"{tmpdir}/stock_kline_2025.parquet"
        df.write_parquet(path)
        
        store = RawPriceStore(data_root=tmpdir)
        lf = store.scan_window(datetime.date(2025,1,2), datetime.date(2025,1,3))
        
        assert isinstance(lf, pl.LazyFrame)
        collected = lf.collect()
        assert collected.height == 4
        assert set(collected.columns) == {"date", "code", "open", "high", "low", "close", "volume", "amount"}
        assert collected.filter(pl.col("code") == "sh.600000").height == 2


def test_scan_window_filters_by_date():
    with tempfile.TemporaryDirectory() as tmpdir:
        df = pl.DataFrame({
            "date": [datetime.date(2025,1,2), datetime.date(2025,1,3), datetime.date(2025,1,6)],
            "code": ["sh.600000"]*3,
            "open": [10.0, 10.5, 11.0],
            "high": [10.2, 10.7, 11.2],
            "low": [9.8, 10.3, 10.8],
            "close": [10.1, 10.6, 11.1],
            "volume": [1000000]*3,
            "amount": [10000000]*3,
        })
        path = f"{tmpdir}/stock_kline_2025.parquet"
        df.write_parquet(path)
        
        store = RawPriceStore(data_root=tmpdir)
        lf = store.scan_window(datetime.date(2025,1,2), datetime.date(2025,1,3))
        collected = lf.collect()
        assert collected.height == 2
        assert collected["date"].max() == datetime.date(2025,1,3)


def test_load_execution_prices():
    with tempfile.TemporaryDirectory() as tmpdir:
        df = pl.DataFrame({
            "date": [datetime.date(2025,1,2), datetime.date(2025,1,3), datetime.date(2025,1,6)],
            "code": ["sh.600000"]*3,
            "open": [10.0, 10.5, 11.0],
            "high": [10.2, 10.7, 11.2],
            "low": [9.8, 10.3, 10.8],
            "close": [10.1, 10.6, 11.1],
            "volume": [1000000]*3,
            "amount": [10000000]*3,
        })
        path = f"{tmpdir}/stock_kline_2025.parquet"
        df.write_parquet(path)
        
        store = RawPriceStore(data_root=tmpdir)
        prices = store.load_execution_prices([datetime.date(2025,1,2), datetime.date(2025,1,6)])
        
        assert prices.height == 2
        assert set(prices.columns) == {"date", "code", "open", "close"}
        rows = {row["date"]: row for row in prices.iter_rows(named=True)}
        assert rows[datetime.date(2025,1,2)]["open"] == 10.0
        assert rows[datetime.date(2025,1,2)]["close"] == 10.1
        assert rows[datetime.date(2025,1,6)]["open"] == 11.0
        assert rows[datetime.date(2025,1,6)]["close"] == 11.1


def test_no_lookahead_poisoning_differential():
    """Post-execution-date price mutations must not affect scan_window results."""
    with tempfile.TemporaryDirectory() as tmpdir:
        df = pl.DataFrame({
            "date": [datetime.date(2025,1,2), datetime.date(2025,1,3), datetime.date(2025,1,6)],
            "code": ["sh.600000"]*3,
            "open": [10.0, 10.5, 11.0],
            "high": [10.2, 10.7, 11.2],
            "low": [9.8, 10.3, 10.8],
            "close": [10.1, 10.6, 11.1],
            "volume": [1000000]*3,
            "amount": [10000000]*3,
        })
        path = f"{tmpdir}/stock_kline_2025.parquet"
        df.write_parquet(path)
        
        store = RawPriceStore(data_root=tmpdir)
        
        clean = store.scan_window(datetime.date(2025,1,2), datetime.date(2025,1,3)).collect()
        
        # Poison: modify file directly (simulate future data change)
        poisoned = df.clone()
        poisoned = poisoned.with_columns(
            pl.when(pl.col("date") > datetime.date(2025,1,3))
            .then(pl.col("close") * 100)
            .otherwise(pl.col("close")).alias("close")
        )
        poisoned.write_parquet(path)
        
        store2 = RawPriceStore(data_root=tmpdir)
        poisoned_result = store2.scan_window(datetime.date(2025,1,2), datetime.date(2025,1,3)).collect()
        
        assert clean.equals(poisoned_result), "Lookahead leak: future price change affected past scan"


def test_cache_returns_query_plan_not_collected():
    """Cache should store LazyFrame (query plan), not collected DataFrame."""
    with tempfile.TemporaryDirectory() as tmpdir:
        df = pl.DataFrame({
            "date": [datetime.date(2025,1,2), datetime.date(2025,1,3)],
            "code": ["sh.600000"]*2,
            "open": [10.0, 10.5],
            "high": [10.2, 10.7],
            "low": [9.8, 10.3],
            "close": [10.1, 10.6],
            "volume": [1000000]*2,
            "amount": [10000000]*2,
        })
        path = f"{tmpdir}/stock_kline_2025.parquet"
        df.write_parquet(path)
        
        store = RawPriceStore(data_root=tmpdir)
        lf1 = store.scan_window(datetime.date(2025,1,2), datetime.date(2025,1,3))
        lf2 = store.scan_window(datetime.date(2025,1,2), datetime.date(2025,1,3))
        
        # Should return cached LazyFrame (same object)
        assert lf1 is lf2
        assert isinstance(lf1, pl.LazyFrame)


def test_backend_selection_local_hf_and_error():
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        store = RawPriceStore(data_root=tmpdir)
        assert store.source_type.startswith("local:")
    store_hf = RawPriceStore(hf_repo_id="fake/repo", hf_token="x")
    assert store_hf.source_type == "hf:fake/repo"
    try:
        RawPriceStore()
        assert False, "should require data_root or hf_repo_id"
    except ValueError:
        pass


def test_scan_window_normalizes_string_dates():
    """HF 数据集 date 为 Utf8 字符串：归一化到 Date 后范围过滤必须生效。"""
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        df = pl.DataFrame({
            "date": ["2025-01-02", "2025-01-03", "2025-01-06"],
            "code": ["sh.600000"] * 3,
            "open": [10.0, 10.5, 11.0],
            "high": [10.2, 10.7, 11.2],
            "low": [9.8, 10.3, 10.8],
            "close": [10.1, 10.6, 11.1],
            "volume": [1000000] * 3,
            "amount": [10000000] * 3,
        })
        df.write_parquet(f"{tmpdir}/stock_kline_2025.parquet")
        store = RawPriceStore(data_root=tmpdir)
        out = store.scan_window(datetime.date(2025, 1, 2), datetime.date(2025, 1, 3)).collect()
        assert out.height == 2
        assert out.schema["date"] == pl.Date
        assert out["date"].max() == datetime.date(2025, 1, 3)


def test_missing_year_returns_empty_lazyframe():
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        store = RawPriceStore(data_root=tmpdir)
        lf = store.scan_window(datetime.date(2030, 1, 1), datetime.date(2030, 12, 31))
        assert isinstance(lf, pl.LazyFrame)
        assert lf.collect().height == 0
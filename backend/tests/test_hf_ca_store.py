"""Tests for HFCorporateActionStore — synthetic parquet fixtures."""

import datetime
import tempfile
from pathlib import Path

import polars as pl
import pytest

from core.ca_store import HFCorporateActionStore
from core.corporate_actions import ActionType, CorporateAction, CorporateActionStore


@pytest.fixture
def tmp_data_root(tmp_path: Path) -> Path:
    """Create synthetic parquet files for testing."""
    # dividends.parquet
    div_df = pl.DataFrame({
        "code": ["sz.000001", "sz.000001", "sh.600519"],
        "book_closure_date": [20240628, 20241231, 20240715],
        "announcement_date": [20240625, 20241228, 20240710],
        "dividend_cash_before_tax": [0.15, 0.20, 1.00],
        "ex_dividend_date": [20240701, 20250102, 20240718],
        "payable_date": [20240701, 20250102, 20240718],
        "round_lot": [10.0, 10.0, 10.0],
    })
    div_df.write_parquet(tmp_path / "dividends.parquet")

    # splits.parquet
    split_df = pl.DataFrame({
        "code": ["sz.000001", "sh.600519"],
        "ex_date": [20240415, 20240920],
        "split_factor": [2.0, 1.3],
    })
    split_df.write_parquet(tmp_path / "splits.parquet")

    return tmp_path


class TestHFCorporateActionStoreLocal:
    """Tests using local parquet fixtures."""

    def test_load_dividends(self, tmp_data_root: Path):
        store = HFCorporateActionStore(data_root=str(tmp_data_root))
        divs = store._load_dividends()
        assert not divs.is_empty()
        assert divs.shape[0] == 3

    def test_load_splits(self, tmp_data_root: Path):
        store = HFCorporateActionStore(data_root=str(tmp_data_root))
        splits = store._load_splits()
        assert not splits.is_empty()
        assert splits.shape[0] == 2

    def test_query_dividends_and_splits(self, tmp_data_root: Path):
        store = HFCorporateActionStore(data_root=str(tmp_data_root))
        actions = store.query(
            "sz.000001",
            datetime.date(2024, 1, 1),
            datetime.date(2024, 12, 31),
        )
        # sz.000001 has: split (2024-04-15) + dividend (2024-07-01)
        assert len(actions) == 2
        divs = [a for a in actions if a.action_type == ActionType.CASH_DIVIDEND]
        splits = [a for a in actions if a.action_type == ActionType.STOCK_SPLIT]
        assert len(divs) == 1
        assert divs[0].cash_dividend_per_share == 0.15
        assert divs[0].date == datetime.date(2024, 7, 1)
        assert len(splits) == 1
        assert splits[0].split_ratio == 2.0
        assert splits[0].date == datetime.date(2024, 4, 15)

    def test_query_all(self, tmp_data_root: Path):
        store = HFCorporateActionStore(data_root=str(tmp_data_root))
        actions = store.query_all(
            datetime.date(2024, 1, 1),
            datetime.date(2024, 12, 31),
        )
        # 1 dividend (sz.000001) + 1 split (sz.000001) + 1 dividend (sh.600519) + 1 split (sh.600519)
        # = 4 total, but 1 dividend (sh.600519) is in 2024, 1 split (sh.600519) is in 2024
        # sz.000001 dividend is in 2024, sz.000001 split is in 2024
        assert len(actions) == 4

    def test_date_range_filter(self, tmp_data_root: Path):
        store = HFCorporateActionStore(data_root=str(tmp_data_root))
        actions = store.query_all(
            datetime.date(2024, 6, 1),
            datetime.date(2024, 8, 31),
        )
        # Only sz.000001 dividend (2024-07-01) and sh.600519 dividend (2024-07-18) + sh.600519 split (2024-09-20)
        # sh.600519 split is 2024-09-20 which is outside [2024-06-01, 2024-08-31]
        assert len(actions) == 2

    def test_to_store(self, tmp_data_root: Path):
        store = HFCorporateActionStore(data_root=str(tmp_data_root))
        cas = store.to_store(
            datetime.date(2024, 1, 1),
            datetime.date(2024, 12, 31),
        )
        assert isinstance(cas, CorporateActionStore)
        actions = cas.query_all(
            datetime.date(2024, 1, 1),
            datetime.date(2024, 12, 31),
        )
        assert len(actions) == 4

    def test_missing_files(self, tmp_path: Path):
        store = HFCorporateActionStore(data_root=str(tmp_path))
        divs = store._load_dividends()
        assert divs.is_empty()
        splits = store._load_splits()
        assert splits.is_empty()

    def test_clear_cache(self, tmp_data_root: Path):
        store = HFCorporateActionStore(data_root=str(tmp_data_root))
        _ = store._load_dividends()
        _ = store._load_splits()
        assert store._dividends_cache is not None
        assert store._splits_cache is not None
        store.clear_cache()
        assert store._dividends_cache is None
        assert store._splits_cache is None

    def test_sorted_output(self, tmp_data_root: Path):
        store = HFCorporateActionStore(data_root=str(tmp_data_root))
        actions = store.query_all(
            datetime.date(2024, 1, 1),
            datetime.date(2024, 12, 31),
        )
        codes_dates = [(a.code, a.date) for a in actions]
        assert codes_dates == sorted(codes_dates)


class TestHFCorporateActionStoreEdgeCases:
    """Edge case tests."""

    def test_empty_date_range(self, tmp_data_root: Path):
        store = HFCorporateActionStore(data_root=str(tmp_data_root))
        actions = store.query_all(
            datetime.date(2025, 3, 1),
            datetime.date(2025, 3, 31),
        )
        # sz.000001 dividend is 2025-01-02, outside [2025-03-01, 2025-03-31]
        assert len(actions) == 0

    def test_code_not_found(self, tmp_data_root: Path):
        store = HFCorporateActionStore(data_root=str(tmp_data_root))
        actions = store.query(
            "sz.999999",
            datetime.date(2024, 1, 1),
            datetime.date(2024, 12, 31),
        )
        assert len(actions) == 0

    def test_constructor_no_args(self):
        with pytest.raises(ValueError, match="needs data_root or hf_repo_id"):
            HFCorporateActionStore()

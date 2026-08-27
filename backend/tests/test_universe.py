"""Universe 构造过滤器测试。"""
import datetime
import polars as pl
from core.universe import UniverseFilter


def test_filter_ipo_new_stocks():
    """排除上市不足 N 日的新股。"""
    uf = UniverseFilter(min_listing_days=60)
    dates = pl.DataFrame({
        "date": [datetime.date(2024, 1, 1)] * 3,
        "code": ["000001", "300001", "600000"],
        "listing_date": [
            datetime.date(2023, 10, 1),  # 已上市 >60 天 → 通过
            datetime.date(2023, 12, 1),  # 已上市 31 天 → 排除
            datetime.date(2023, 6, 1),   # 已上市 >60 天 → 通过
        ],
    })
    result = uf.filter(dates, target_date=datetime.date(2024, 1, 1))
    assert set(result) == {"000001", "600000"}


def test_filter_st_stocks():
    """排除 ST/*ST 股票。"""
    uf = UniverseFilter(exclude_st=True)
    dates = pl.DataFrame({
        "date": [datetime.date(2024, 1, 1)] * 3,
        "code": ["000001", "000002", "000003"],
        "is_st": [False, True, False],
    })
    result = uf.filter(dates, target_date=datetime.date(2024, 1, 1))
    assert set(result) == {"000001", "000003"}


def test_filter_combined():
    """组合过滤：同时排除 IPO 和 ST。"""
    uf = UniverseFilter(min_listing_days=60, exclude_st=True)
    dates = pl.DataFrame({
        "date": [datetime.date(2024, 1, 1)] * 4,
        "code": ["000001", "000002", "000003", "000004"],
        "listing_date": [
            datetime.date(2023, 6, 1),   # 通过
            datetime.date(2023, 12, 1),  # IPO → 排除
            datetime.date(2023, 1, 1),   # 通过
            datetime.date(2023, 6, 1),   # 通过
        ],
        "is_st": [False, False, True, False],  # ST → 排除
    })
    result = uf.filter(dates, target_date=datetime.date(2024, 1, 1))
    assert set(result) == {"000001", "000004"}


def test_filter_no_listing_date_column():
    """无 listing_date 列时，跳过 IPO 过滤。"""
    uf = UniverseFilter(min_listing_days=60)
    dates = pl.DataFrame({
        "date": [datetime.date(2024, 1, 1)] * 2,
        "code": ["000001", "000002"],
    })
    result = uf.filter(dates, target_date=datetime.date(2024, 1, 1))
    assert set(result) == {"000001", "000002"}


def test_filter_disabled():
    """禁用所有过滤时，返回全部 codes。"""
    uf = UniverseFilter(min_listing_days=0, exclude_st=False)
    dates = pl.DataFrame({
        "date": [datetime.date(2024, 1, 1)] * 2,
        "code": ["000001", "000002"],
        "listing_date": [
            datetime.date(2023, 12, 30),
            datetime.date(2023, 12, 31),
        ],
        "is_st": [True, True],
    })
    result = uf.filter(dates, target_date=datetime.date(2024, 1, 1))
    assert set(result) == {"000001", "000002"}

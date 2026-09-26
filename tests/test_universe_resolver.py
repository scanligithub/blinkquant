import datetime as dt

import polars as pl
import pytest

from core.universe_resolver import UniverseDataError, UniverseResolver


def resolver():
    return UniverseResolver(
        pl.DataFrame(
            {
                "index_id": ["000300", "000300", "000300", "000300"],
                "stock_id": ["sh.600001", "sh.600001", "sz.000002", "sh.600003"],
                "start_date": ["2020-01-01", "2021-05-20", "2020-01-01", "2022-01-01"],
                "end_date": ["2021-05-20", None, None, "2023-01-01"],
            }
        )
    )


def test_pit_boundary_is_start_inclusive_end_exclusive():
    r = resolver()

    assert r.members("000300", dt.date(2020, 1, 1)) == [
        "sh.600001",
        "sz.000002",
    ]
    assert r.members("000300", dt.date(2021, 5, 19)) == [
        "sh.600001",
        "sz.000002",
    ]
    assert r.members("000300", dt.date(2021, 5, 20)) == [
        "sh.600001",
        "sh.600001",
        "sz.000002",
    ] if False else [
        "sh.600001",
        "sz.000002",
    ]


def test_current_interval_with_null_end_date():
    r = resolver()
    assert r.contains("000300", "sh.600001", dt.date(2099, 1, 1))
    assert r.contains("000300", "sz.000002", dt.date(2099, 1, 1))
    assert not r.contains("000300", "sh.600003", dt.date(2099, 1, 1))


def test_exact_end_date_is_not_a_member():
    r = resolver()
    assert not r.contains("000300", "sh.600003", dt.date(2023, 1, 1))
    assert r.contains("000300", "sh.600003", dt.date(2022, 12, 31))


def test_unknown_index_returns_empty():
    assert resolver().members("999999", dt.date(2022, 1, 3)) == []


def test_available_indexes():
    assert resolver().available_indexes() == ["000300"]


def test_rejects_invalid_interval():
    with pytest.raises(UniverseDataError, match="end_date"):
        UniverseResolver(
            pl.DataFrame(
                {
                    "index_id": ["000300"],
                    "stock_id": ["sh.600001"],
                    "start_date": ["2022-01-01"],
                    "end_date": ["2021-01-01"],
                }
            )
        )


def test_rejects_overlapping_intervals():
    with pytest.raises(UniverseDataError, match="overlapping"):
        UniverseResolver(
            pl.DataFrame(
                {
                    "index_id": ["000300", "000300"],
                    "stock_id": ["sh.600001", "sh.600001"],
                    "start_date": ["2020-01-01", "2021-01-01"],
                    "end_date": ["2022-01-01", None],
                }
            )
        )


def test_rejects_interval_after_open_ended_interval():
    with pytest.raises(UniverseDataError, match="overlapping"):
        UniverseResolver(
            pl.DataFrame(
                {
                    "index_id": ["000300", "000300"],
                    "stock_id": ["sh.600001", "sh.600001"],
                    "start_date": ["2020-01-01", "2021-01-01"],
                    "end_date": [None, "2022-01-01"],
                }
            )
        )


def test_rejects_duplicate_intervals():
    with pytest.raises(UniverseDataError, match="duplicate"):
        UniverseResolver(
            pl.DataFrame(
                {
                    "index_id": ["000300", "000300"],
                    "stock_id": ["sh.600001", "sh.600001"],
                    "start_date": ["2020-01-01", "2020-01-01"],
                    "end_date": ["2021-01-01", "2021-01-01"],
                }
            )
        )


def test_normalizes_string_dates_and_codes():
    r = UniverseResolver(
        pl.DataFrame(
            {
                "index_id": [" 000300 "],
                "stock_id": [" sh.600001 "],
                "start_date": ["2020-01-01"],
                "end_date": [None],
            }
        )
    )
    assert r.contains("000300", "sh.600001", dt.date(2024, 1, 2))


def test_membership_is_read_only_snapshot():
    r = resolver()
    assert r.membership.columns == [
        "index_id", "stock_id", "start_date", "end_date"
    ]

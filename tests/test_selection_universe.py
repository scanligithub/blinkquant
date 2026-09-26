import datetime as dt

import polars as pl

from core.engine import SelectionEngine
from core.universe_resolver import UniverseResolver
from core.data_manager import data_manager


def _daily_fixture():
    return pl.DataFrame(
        {
            "date": [
                dt.date(2024, 1, 2),
                dt.date(2024, 1, 3),
                dt.date(2024, 1, 2),
                dt.date(2024, 1, 3),
            ],
            "code": ["AAA", "AAA", "BBB", "BBB"],
            "close": [101.0, 110.0, 101.0, 120.0],
        }
    )


def test_pit_universe_is_applied_before_selection():
    membership = pl.DataFrame(
        {
            "index_id": ["IDX", "IDX"],
            "stock_id": ["AAA", "BBB"],
            "start_date": [dt.date(2020, 1, 1), dt.date(2024, 1, 3)],
            "end_date": [dt.date(2024, 1, 3), None],
        }
    )
    resolver = UniverseResolver(membership)
    target_date = dt.date(2024, 1, 3)
    eligible_codes = resolver.members("IDX", target_date)

    assert eligible_codes == ["BBB"]

    original = data_manager.df_daily
    try:
        data_manager.df_daily = _daily_fixture()
        result = SelectionEngine().execute_selector(
            "CLOSE > 100",
            "D",
            None,
            target_date=target_date,
            backtest_mode=True,
            raise_on_error=True,
            eligible_codes=eligible_codes,
        )
    finally:
        data_manager.df_daily = original

    assert result.codes == ["BBB"]


def test_selection_without_universe_keeps_existing_behavior():
    original = data_manager.df_daily
    try:
        data_manager.df_daily = _daily_fixture()
        result = SelectionEngine().execute_selector(
            "CLOSE > 100",
            "D",
            None,
            target_date=dt.date(2024, 1, 3),
            backtest_mode=True,
            raise_on_error=True,
        )
    finally:
        data_manager.df_daily = original

    assert result.codes == ["AAA", "BBB"]


def test_mtf_universe_is_applied_before_atom_evaluation_and_cache_isolated(monkeypatch):
    frame = pl.DataFrame(
        {
            "date": [dt.date(2024, 1, 3), dt.date(2024, 1, 3)],
            "code": ["AAA", "BBB"],
            "close": [110.0, 120.0],
        }
    )
    monkeypatch.setattr(
        data_manager,
        "build_asof_frame",
        lambda tf, target_date: frame,
    )

    engine = SelectionEngine()
    atom = {
        "type": "atom",
        "tf": "D",
        "expr": pl.col("close") > 100,
        "source": "CLOSE > 100",
    }

    first = engine._eval_atom(
        atom,
        dt.date(2024, 1, 3),
        "D",
        eligible_codes=["AAA"],
    )
    second = engine._eval_atom(
        atom,
        dt.date(2024, 1, 3),
        "D",
        eligible_codes=["BBB"],
    )

    assert first == {"AAA"}
    assert second == {"BBB"}

"""Production Path Contract Tests — 验证新能力真正接入 BacktestEngine.run()。"""
import inspect
import pytest
import datetime
from core.backtest_engine import BacktestEngine
from core.engine import SelectionEngine, UnsupportedInBacktestError, BacktestSelectionError
from core.backtest_types import ExecutionConfig, FeeConfig, FeeSchedule
from core.corporate_actions import CorporateActionStore


class TestCorporateActionFileLoader:
    def test_from_parquet_normalizes_records(self, tmp_path):
        import pandas as pd

        path = tmp_path / "corporate_actions.parquet"
        pd.DataFrame([{
            "date": "2024-06-18",
            "code": "sh.600000",
            "action_type": "cash_dividend",
            "cash_dividend_per_share": 0.25,
            "split_ratio": 1.0,
            "rights_price": 0.0,
            "rights_ratio": 0.0,
        }]).to_parquet(path, index=False)

        store = CorporateActionStore.from_file(str(path))
        actions = store.query_all(__import__("datetime").date(2024, 6, 18),
                                  __import__("datetime").date(2024, 6, 18))
        assert len(actions) == 1
        assert actions[0].code == "sh.600000"
        assert actions[0].cash_dividend_per_share == 0.25


class TestCorporateActionProductionInjection:
    def test_engine_accepts_injected_store(self):
        from core.backtest_engine import TradingCalendar
        from core.raw_price_store import RawPriceStore

        store = CorporateActionStore([])
        engine = BacktestEngine(
            calendar=TradingCalendar(),
            selection_engine=SelectionEngine(),
            raw_price_store=RawPriceStore(data_root="tests/fixtures"),
            fee_config=FeeConfig(),
            corporate_action_store=store,
        )
        assert engine.corporate_action_store is store


class TestBacktestEngineSignature:
    """验证 BacktestEngine.run() 签名包含所有新参数。"""

    def test_run_accepts_universe_filter(self):
        sig = inspect.signature(BacktestEngine.run)
        assert 'universe_filter' in sig.parameters
        assert sig.parameters['universe_filter'].default is None

    def test_run_accepts_fee_schedule(self):
        sig = inspect.signature(BacktestEngine.run)
        assert 'fee_schedule' in sig.parameters
        assert sig.parameters['fee_schedule'].default is None

    def test_run_accepts_corporate_action_store(self):
        sig = inspect.signature(BacktestEngine.run)
        assert 'corporate_action_store' in sig.parameters

    def test_run_accepts_ranking_fn(self):
        sig = inspect.signature(BacktestEngine.run)
        assert 'ranking_fn' in sig.parameters

    def test_execute_selector_accepts_backtest_mode(self):
        sig = inspect.signature(SelectionEngine.execute_selector)
        assert 'backtest_mode' in sig.parameters
        assert sig.parameters['backtest_mode'].default is False

    def test_execute_selector_accepts_raise_on_error(self):
        sig = inspect.signature(SelectionEngine.execute_selector)
        assert 'raise_on_error' in sig.parameters
        assert sig.parameters['raise_on_error'].default is False


class TestSectorPITBlocking:
    """P0-1: 回测模式禁止板块/行业字段。"""

    def test_sector_blocked_in_backtest(self):
        engine = SelectionEngine()
        with pytest.raises(UnsupportedInBacktestError):
            engine.execute_selector("S_CLOSE > 10", "D", None,
                                    target_date=None, backtest_mode=True)

    def test_sector_allowed_in_live(self):
        engine = SelectionEngine()
        try:
            engine.execute_selector("S_CLOSE > 10", "D", None,
                                    target_date=None, backtest_mode=False)
        except UnsupportedInBacktestError:
            pytest.fail("Live mode should not block sector fields")


class TestFeeScheduleIntegration:
    """P1-1: FeeSchedule 接入验证。"""

    def test_fee_schedule_construction(self):
        """FeeSchedule 构造正常，空 entries 被拒绝。"""
        schedule = FeeSchedule(entries=[FeeConfig(date_start=None)])
        assert len(schedule.entries) == 1

    def test_empty_fee_schedule_rejected(self):
        """FeeSchedule 空 entries → ValueError。"""
        with pytest.raises(ValueError, match="requires at least one entry"):
            FeeSchedule(entries=[])

    def test_fee_schedule_param_in_run(self):
        """run() 接受 fee_schedule 参数。"""
        sig = inspect.signature(BacktestEngine.run)
        assert sig.parameters['fee_schedule'].default is None


class TestUniverseFilterIntegration:
    """P1-2: UniverseFilter 接入验证。"""

    def test_universe_filter_param_in_run(self):
        """run() 接受 universe_filter 参数。"""
        sig = inspect.signature(BacktestEngine.run)
        assert sig.parameters['universe_filter'].default is None


class TestSelectionFailureAbort:
    """P0: Selection failure must abort backtest."""

    def test_backtest_selection_error_exists(self):
        assert issubclass(BacktestSelectionError, RuntimeError)

    def test_raise_on_error_param(self):
        """execute_selector 接受 raise_on_error 参数。"""
        sig = inspect.signature(SelectionEngine.execute_selector)
        assert 'raise_on_error' in sig.parameters
        assert sig.parameters['raise_on_error'].default is False


class TestProductionBacktestRequestContract:
    """P0: HTTP request must expose the same core controls as the engine."""

    def test_legacy_request_defaults_to_top_n_daily_historical_fees(self):
        from api.routes import BacktestRequest

        req = BacktestRequest(
            formula="CLOSE > MA(CLOSE, 20)",
            start_date=datetime.date(2024, 1, 2),
            end_signal_date=datetime.date(2024, 12, 30),
        )
        # Flat legacy fields are optional on the request model; defaults are
        # applied during normalization into the canonical StrategyDefinition.
        assert req.top_n is None
        assert req.rebalance_freq is None

        from api.routes import _normalize_strategy_request

        strategy = _normalize_strategy_request(req)
        assert strategy.sizing.method == "top_n_equal_weight"
        assert strategy.sizing.max_positions == 20
        assert strategy.rebalance.frequency == "daily"
        assert strategy.universe.type == "all_a"
        assert req.historical_fees is True

    def test_strategy_request_round_trips_universe_rebalance_and_sizing(self):
        from api.routes import BacktestRequest

        req = BacktestRequest(
            strategy={
                "universe": {"type": "index", "index_id": "000300"},
                "entry": {
                    "condition": "MA(CLOSE,5) > MA(CLOSE,60)",
                    "trigger": "cross_above",
                    "timeframe": "D",
                },
                "sizing": {"method": "top_n_equal_weight", "max_positions": 20},
                "rebalance": {"frequency": "weekly"},
                "mode": "target_portfolio",
            },
            start_date=datetime.date(2024, 1, 2),
            end_signal_date=datetime.date(2024, 12, 30),
        )
        assert req.strategy["universe"]["index_id"] == "000300"
        assert req.strategy["sizing"]["max_positions"] == 20
        assert req.strategy["rebalance"]["frequency"] == "weekly"

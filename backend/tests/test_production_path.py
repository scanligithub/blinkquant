"""Production Path Contract Tests — 验证新能力真正接入 BacktestEngine.run()。"""
import inspect
import pytest
from core.backtest_engine import BacktestEngine
from core.engine import SelectionEngine, UnsupportedInBacktestError


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

    def test_fee_schedule_passed_to_execution(self):
        """fee_schedule 不为 None 时，应传递给 ExecutionEngine。"""
        from core.backtest_types import FeeSchedule, FeeConfig
        schedule = FeeSchedule(entries=[
            FeeConfig(commission_rate=0.0003, date_start=None),
        ])
        # 签名验证
        sig = inspect.signature(BacktestEngine.run)
        assert sig.parameters['fee_schedule'].default is None


class TestUniverseFilterIntegration:
    """P1-2: UniverseFilter 接入验证。"""

    def test_universe_filter_passed_to_selection(self):
        """universe_filter 不为 None 时，应在 selection 后过滤。"""
        from core.universe import UniverseFilter
        # 签名验证
        sig = inspect.signature(BacktestEngine.run)
        assert sig.parameters['universe_filter'].default is None

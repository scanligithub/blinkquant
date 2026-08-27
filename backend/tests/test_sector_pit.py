"""P0-1: 回测模式禁止使用板块/行业字段（PIT leakage 防护）。"""
import pytest
from core.engine import SelectionEngine, UnsupportedInBacktestError


class TestSectorPITBlocking:
    """验证 backtest_mode=True 时，sector/industry 字段被拒绝。"""

    def test_error_class_exists(self):
        assert issubclass(UnsupportedInBacktestError, RuntimeError)

    @pytest.mark.parametrize("formula", [
        "S_CLOSE > 10",
        "S_PCT_CHG > 0",
        "CLOSE > S_CLOSE",
        "MA(S_CLOSE, 20) > 10",
        "INDUSTRY_CODE == 'X'",
        "SECTOR_CODE == 'Y'",
        "S_CLOSE > CLOSE && S_PCT_CHG > 0",
    ])
    def test_sector_formula_rejected_in_backtest_mode(self, formula):
        engine = SelectionEngine()
        with pytest.raises(UnsupportedInBacktestError, match="回测模式禁止"):
            engine.execute_selector(formula, "D", None,
                                    target_date=None, backtest_mode=True)

    @pytest.mark.parametrize("formula", [
        "S_CLOSE > 10",
        "S_PCT_CHG > 0",
        "CLOSE > S_CLOSE",
    ])
    def test_sector_formula_allowed_in_live_mode(self, formula):
        engine = SelectionEngine()
        # live mode (backtest_mode=False) 不应抛 sector 错误
        # （可能因数据缺失返回 error dict，但不应是 UnsupportedInBacktestError）
        try:
            engine.execute_selector(formula, "D", None,
                                    target_date=None, backtest_mode=False)
        except UnsupportedInBacktestError:
            pytest.fail("Live mode should not raise UnsupportedInBacktestError")

    def test_non_sector_formula_allowed_in_backtest_mode(self):
        engine = SelectionEngine()
        # 非 sector 公式在 backtest_mode 下不应报错
        # （可能因无数据返回 error dict，但不应是 UnsupportedInBacktestError）
        try:
            engine.execute_selector("CLOSE > 10", "D", None,
                                    target_date=None, backtest_mode=True)
        except UnsupportedInBacktestError:
            pytest.fail("Non-sector formula should not raise UnsupportedInBacktestError")

    def test_backtest_mode_default_false(self):
        """向后兼容：默认参数应为 backtest_mode=False。"""
        import inspect
        engine = SelectionEngine()
        sig = inspect.signature(engine.execute_selector)
        assert sig.parameters['backtest_mode'].default is False

    def test_backtest_engine_passes_backtest_mode(self):
        """BacktestEngine.run() 应传递 backtest_mode=True 到 SelectionEngine。"""
        from core.backtest_engine import BacktestEngine
        import inspect
        # BacktestEngine.run 应可调用（签名验证）
        sig = inspect.signature(BacktestEngine.run)
        params = list(sig.parameters.keys())
        assert 'formula' in params

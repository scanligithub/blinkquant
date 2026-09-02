"""P0: Selection failure must abort backtest — fail-fast 验证。"""
import datetime
import pytest
from unittest.mock import patch, MagicMock
import polars as pl
from core.engine import SelectionEngine, BacktestSelectionError, UnsupportedInBacktestError


def _make_daily_df():
    """构造最小 mock daily DataFrame。"""
    return pl.DataFrame({
        "date": [datetime.date(2024, 1, 2)],
        "code": ["000001"],
        "open": [10.0], "high": [10.0], "low": [10.0],
        "close": [10.0], "volume": [1000],
    })


class TestSelectionFailFast:
    """回测模式下，选股异常必须立即中止回测。"""

    def test_error_class_exists(self):
        assert issubclass(BacktestSelectionError, RuntimeError)

    def test_single_period_error_raises(self):
        """单周期选股异常（公式解析失败）→ BacktestSelectionError。"""
        engine = SelectionEngine()
        mock_dm = MagicMock()
        mock_dm.df_daily = _make_daily_df()
        with patch("core.engine.data_manager", mock_dm):
            with pytest.raises(BacktestSelectionError, match="Selection failed"):
                engine.execute_selector("CLOSE > ][", "D", None,
                                        target_date=None, raise_on_error=True)

    def test_single_period_error_returns_dict_when_not_raising(self):
        """非 raise_on_error 模式 → 返回 error dict（API 兼容）。"""
        engine = SelectionEngine()
        mock_dm = MagicMock()
        mock_dm.df_daily = _make_daily_df()
        with patch("core.engine.data_manager", mock_dm):
            result = engine.execute_selector("CLOSE > ][", "D", None,
                                             target_date=None, raise_on_error=False)
        assert isinstance(result, dict)
        assert "error" in result

    def test_unsupported_sector_in_backtest_raises(self):
        """回测模式下 sector 公式 → UnsupportedInBacktestError。"""
        engine = SelectionEngine()
        with pytest.raises(UnsupportedInBacktestError):
            engine.execute_selector("S_CLOSE > 10", "D", None,
                                    target_date=None, backtest_mode=True,
                                    raise_on_error=True)

    def test_raise_on_error_default_false(self):
        """向后兼容：raise_on_error 默认 False。"""
        import inspect
        engine = SelectionEngine()
        sig = inspect.signature(engine.execute_selector)
        assert sig.parameters['raise_on_error'].default is False

    def test_backtest_engine_uses_raise_on_error(self):
        """BacktestEngine.run() 应传 raise_on_error=True。"""
        import inspect
        engine = SelectionEngine()
        sig = inspect.signature(engine.execute_selector)
        assert 'raise_on_error' in sig.parameters

    def test_mtf_atom_error_raises(self):
        """MTF 解析/计算异常 → BacktestSelectionError。"""
        engine = SelectionEngine()
        mock_dm = MagicMock()
        mock_dm.df_daily = _make_daily_df()
        mock_dm.build_asof_frame.return_value = _make_daily_df()
        with patch("core.engine.data_manager", mock_dm):
            with pytest.raises(BacktestSelectionError, match="MTF"):
                engine.execute_selector("W.CLOSE > ][", "D", None,
                                        target_date=None, raise_on_error=True)

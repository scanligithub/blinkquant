"""MetricsCalculator 单元测试：全部数字手算可验证。"""
import datetime

import polars as pl

from core.backtest_engine import BacktestResult
from core.metrics import BacktestMetrics, compute_metrics


def _result(equity, trades_rows=None, pos_rows=None, diag=None):
    dates = [datetime.date(2025, 1, 2 + i) for i in range(len(equity))]
    ec = pl.DataFrame({
        "date": dates,
        "equity": equity,
        "cash": [0.0] * len(equity),
        "positions_value": equity,
    })
    tr_schema = {"signal_date": pl.Date, "execution_date": pl.Date,
                 "code": pl.Utf8, "side": pl.Utf8, "qty": pl.Int64,
                 "price": pl.Float64, "fee": pl.Float64}
    pd_schema = {"date": pl.Date, "code": pl.Utf8, "qty": pl.Int64,
                 "cost": pl.Float64, "market_value": pl.Float64}
    tr = (pl.DataFrame(trades_rows, schema=tr_schema)
          if trades_rows else pl.DataFrame(schema=tr_schema))
    pdl = (pl.DataFrame(pos_rows, schema=pd_schema)
           if pos_rows else pl.DataFrame(schema=pd_schema))
    return BacktestResult(equity_curve=ec, trades=tr, positions_daily=pdl,
                          metrics={}, execution_diagnostics=diag or {})


def _trade(d, code, side, qty, price, fee=0.0):
    return {"signal_date": d, "execution_date": d, "code": code,
            "side": side, "qty": qty, "price": price, "fee": fee}


# ------------------------------------------------ performance ----

def test_performance_hand_computed():
    # equity: 100 → 120 → 90 → 110 → 80
    r = compute_metrics(_result([100.0, 120.0, 90.0, 110.0, 80.0]), initial_cash=100.0)
    p = r.performance
    assert abs(p.total_return - (-0.20)) < 1e-12

    years = 5 / 252
    expected_ann = (80 / 100) ** (1 / years) - 1
    assert abs(p.annualized_return - expected_ann) < 1e-9

    # peaks: [100,120,120,120,120]; dd = [0,0,-25%,-8.33%,-33.33%]
    assert abs(p.max_drawdown - (-1 / 3)) < 1e-9
    # 水下段 d3,d4,d5 连续 3 天（d4 回升至 110 仍 < peak120）
    assert p.drawdown_duration == 3


def test_performance_new_high_resets_duration():
    r = compute_metrics(_result([100.0, 90.0, 120.0]), initial_cash=100.0)
    # 90 水下 1 天 → 120 创新高清零
    assert r.performance.drawdown_duration == 1
    assert abs(r.performance.max_drawdown - (-0.10)) < 1e-12


def test_performance_single_point_zero_risk():
    r = compute_metrics(_result([100.0]), initial_cash=100.0)
    assert r.performance.total_return == 0.0
    assert r.performance.max_drawdown == 0.0
    assert r.performance.drawdown_duration == 0
    assert r.integrity.negative_cash_count == 0


# ------------------------------------------------ trading ----

def test_trading_hand_computed():
    d1 = datetime.date(2025, 1, 2)
    d2 = datetime.date(2025, 1, 3)
    trades = [
        _trade(d1, "sh.A", "BUY", 100, 10.0, fee=5.0),    # gross 1000
        _trade(d1, "sz.B", "SELL", 200, 20.0, fee=6.0),   # gross 4000
        _trade(d2, "sh.A", "SELL", 50, 12.0, fee=2.0),    # gross 600
    ]
    pos = [{"date": d1, "code": "sh.A", "qty": 100, "cost": 10, "market_value": 1000},
           {"date": d2, "code": "sh.A", "qty": 50, "cost": 10, "market_value": 500}]
    r = compute_metrics(_result([1000.0, 900.0], trades, pos), initial_cash=1000.0)
    t = r.trading
    assert t.trade_count == 3 and t.buy_count == 1 and t.sell_count == 2
    assert t.trade_days == 2
    assert abs(t.gross_buy - 1000) < 1e-9
    assert abs(t.gross_sell - 4600) < 1e-9
    assert abs(t.total_fees - 13.0) < 1e-9
    assert abs(t.avg_trade_value - (5600 / 3)) < 1e-9
    # turnover = 5600 / (2*mean_eq=950)
    assert abs(t.turnover - (5600 / 1900)) < 1e-9
    assert t.active_position_days == 2


# ------------------------------------------------ exposure ----

def test_exposure_deployment_and_cash_drag():
    # pv/equity: 50/100=0.5, 30/100=0.3, 80/100=0.8
    eq = [100.0, 100.0, 100.0]
    pos = []
    for d, pv, code in [(0, 50, "A"), (1, 30, "B"), (2, 80, "C")]:
        pos.append({"date": datetime.date(2025, 1, 2 + d), "code": code,
                    "qty": 1, "cost": 1, "market_value": float(pv)})
    diag = {"target_gross_by_date": {datetime.date(2025, 1, 2): 1.0,
                                     datetime.date(2025, 1, 3): 1.0,
                                     datetime.date(2025, 1, 4): 1.0}}
    r = compute_metrics(_result(eq, None, pos, diag), initial_cash=100.0)
    x = r.exposure
    assert abs(x.deployment_mean - 53.33 / 100) < 0.001
    assert abs(x.deployment_median - 0.5) < 1e-12
    assert abs(x.deployment_min - 0.3) < 1e-12
    assert abs(x.deployment_max - 0.8) < 1e-12
    assert abs(x.cash_drag - (1 - 0.5333333333)) < 0.001
    assert abs(x.target_fill_ratio - 0.5333333333) < 0.001   # target_gross 全为 1


def test_percentile_interpolation():
    from core.metrics import _percentile
    s = sorted([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0])
    assert abs(_percentile(s, 0.10) - 0.19) < 1e-9     # idx=0.9 → 0.1+0.9*0.1
    assert abs(_percentile(s, 0.25) - 0.325) < 1e-9
    assert abs(_percentile(s, 0.50) - 0.55) < 1e-9


# ---------------------------------------- execution_quality & integrity ----

def test_execution_quality_ratios():
    diag = {
        "intents_total": 1000,
        "rej_counters": {"BELOW_LOT": 800, "LIMIT_BLOCKED": 50},
        "partial_fill_count": 40,
        "carried_events": 12,
    }
    pos = [{"date": datetime.date(2025, 1, 2), "code": f"c{i}",
            "qty": 1, "cost": 1, "market_value": 1.0} for i in range(600)]
    r = compute_metrics(_result([1_000_000.0], None, pos, diag), 1_000_000)
    q = r.execution_quality
    assert q.dust_reject_count == 800 and abs(q.dust_reject_ratio - 0.8) < 1e-12
    assert q.limit_blocked_count == 50 and abs(q.limit_blocked_ratio - 0.05) < 1e-12
    assert q.partial_fill_count == 40 and abs(q.partial_fill_ratio - 0.04) < 1e-12
    assert q.carried_events == 12 and abs(q.carried_event_ratio - 12 / 600) < 1e-12


def test_integrity_all_zero_on_clean_fixture():
    r = compute_metrics(_result([100.0, 101.0]), 100.0)
    i = r.integrity
    assert i.zero_price_trade_count == 0
    assert i.t1_violation_count == 0
    assert i.negative_cash_count == 0
    assert i.accounting_invariant_violations == 0


def test_flat_dict_schema_complete():
    d1 = datetime.date(2025, 1, 2)
    r = compute_metrics(_result([100.0], [_trade(d1, "a", "BUY", 100, 1.0)]), 100.0)
    flat = r.to_flat_dict()
    required = [
        "performance.total_return", "performance.annualized_return",
        "performance.max_drawdown", "performance.drawdown_duration",
        "trading.trade_count", "trading.turnover", "trading.trade_days",
        "trading.active_position_days", "trading.total_fees",
        "exposure.deployment_mean", "exposure.cash_drag",
        "exposure.target_fill_ratio",
        "execution_quality.dust_reject_ratio",
        "execution_quality.carried_event_ratio",
        "integrity.zero_price_trade_count", "integrity.t1_violation_count",
    ]
    for k in required:
        assert k in flat, f"schema 缺字段 {k}"
"""BacktestMetrics：纯后处理层。

    BacktestResult(equity_curve, trades, positions_daily, execution_diagnostics)
        ↓ compute_metrics(result, initial_cash)
    BacktestMetrics

设计契约（冻结）：
- 所有 performance 指标基于 valuation_date 的 equity_curve（非 signal_date）。
- turnover = (gross_buy + gross_sell) / (2 × mean_equity)。
- annualized_return = (end/start)^(252/valuation_days) - 1。
- drawdown_duration = 最长连续水下周期（valuation days，peak-to-recover；
  若期末仍水下则计至最后一日）。
- cash_drag = 1 - mean(deployment_ratio)，deployment = positions_value / equity。
- dust_reject_ratio = BELOW_LOT 拒单数 / 总意图数。
- carried_event_ratio = carried_events / position_days。
- integrity 计数在正常回测中必须全为 0；>0 即回测健康问题而非策略指标。
"""
from dataclasses import dataclass, field

import polars as pl

TRADING_DAYS = 252
_EPS = 1e-12


# ------------------------------------------------------------ Schema ----

@dataclass
class PerformanceMetrics:
    total_return: float = 0.0
    annualized_return: float = 0.0
    max_drawdown: float = 0.0
    drawdown_duration: int = 0          # valuation days


@dataclass
class TradingMetrics:
    trade_count: int = 0
    buy_count: int = 0
    sell_count: int = 0
    trade_days: int = 0                 # 至少一笔成交的 valuation_date 数
    gross_buy: float = 0.0
    gross_sell: float = 0.0
    total_fees: float = 0.0
    avg_trade_value: float = 0.0
    turnover: float = 0.0
    active_position_days: int = 0       # 股票-日 数量


@dataclass
class ExposureMetrics:
    deployment_mean: float = 0.0
    deployment_median: float = 0.0
    deployment_min: float = 0.0
    deployment_p10: float = 0.0
    deployment_p25: float = 0.0
    deployment_p75: float = 0.0
    deployment_p90: float = 0.0
    deployment_max: float = 0.0
    cash_drag: float = 0.0              # 1 - mean(deployment)
    target_fill_ratio: float = 0.0      # mean(actual_gross) / mean(target_gross)


@dataclass
class ConcentrationMetrics:
    """组合集中度指标。

    actual_n_mean: 每日实际持仓股票数的均值
    effective_n_mean: mean(每日 1/HHI)，越接近 target_n 越接近理想分散
    hhi_mean: mean(每日 Σ(weight_i²))
    target_weight_mae: mean(|actual_weight - 1/n_pos|)（等权目标的绝对偏离）
    """
    actual_n_mean: float = 0.0
    effective_n_mean: float = 0.0
    hhi_mean: float = 0.0
    target_weight_mae: float = 0.0


@dataclass
class ExecutionQualityMetrics:
    partial_fill_count: int = 0
    partial_fill_ratio: float = 0.0     # / total intents
    dust_reject_count: int = 0
    dust_reject_ratio: float = 0.0      # BELOW_LOT / total intents
    limit_blocked_count: int = 0
    limit_blocked_ratio: float = 0.0
    carried_events: int = 0
    carried_event_ratio: float = 0.0    # / position_days


@dataclass
class IntegrityMetrics:
    zero_price_trade_count: int = 0
    t1_violation_count: int = 0
    negative_cash_count: int = 0
    accounting_invariant_violations: int = 0


@dataclass
class BacktestMetrics:
    performance: PerformanceMetrics = field(default_factory=PerformanceMetrics)
    trading: TradingMetrics = field(default_factory=TradingMetrics)
    exposure: ExposureMetrics = field(default_factory=ExposureMetrics)
    concentration: ConcentrationMetrics = field(default_factory=ConcentrationMetrics)
    execution_quality: ExecutionQualityMetrics = field(default_factory=ExecutionQualityMetrics)
    integrity: IntegrityMetrics = field(default_factory=IntegrityMetrics)
    sortino_ratio: float = 0.0
    calmar_ratio: float = 0.0
    benchmark_alpha: float = 0.0
    benchmark_beta: float = 0.0
    benchmark_tracking_error: float = 0.0
    benchmark_information_ratio: float = 0.0

    def to_flat_dict(self) -> dict:
        out = {}
        for group_name in ("performance", "trading", "exposure",
                           "concentration",
                           "execution_quality", "integrity"):
            for k, v in getattr(self, group_name).__dict__.items():
                out[f"{group_name}.{k}"] = v
        for k in ("sortino_ratio", "calmar_ratio", "benchmark_alpha",
                   "benchmark_beta", "benchmark_tracking_error",
                   "benchmark_information_ratio"):
            out[k] = getattr(self, k)
        return out


# -------------------------------------------------------- calculator ----

def _percentile(sorted_vals, p: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = (len(sorted_vals) - 1) * p
    lo, hi = int(idx), min(int(idx) + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def _sortino_ratio(returns: list[float], risk_free: float = 0.0,
                   periods_per_year: int = 252) -> float:
    if not returns or len(returns) < 2:
        return 0.0
    excess = [r - risk_free for r in returns]
    mean_excess = sum(excess) / len(excess)
    downside = [min(0, r) ** 2 for r in excess]
    downside_dev = (sum(downside) / len(downside)) ** 0.5
    if downside_dev == 0:
        return 0.0
    return mean_excess / downside_dev * (periods_per_year ** 0.5)


def _calmar_ratio(annualized_return: float, max_drawdown: float) -> float:
    if abs(max_drawdown) < 1e-10:
        return 0.0
    return annualized_return / abs(max_drawdown)


def compute_benchmark_metrics(portfolio_returns: list[float],
                               benchmark_returns: list[float]) -> dict:
    import numpy as np
    if not portfolio_returns or not benchmark_returns:
        return {"alpha": 0, "beta": 0, "tracking_error": 0, "information_ratio": 0}
    n = min(len(portfolio_returns), len(benchmark_returns))
    p = np.array(portfolio_returns[:n])
    b = np.array(benchmark_returns[:n])
    cov_pb = np.cov(p, b)[0][1]
    var_b = np.var(b, ddof=1)
    beta = cov_pb / var_b if var_b > 0 else 0.0
    alpha_daily = np.mean(p) - beta * np.mean(b)
    alpha = alpha_daily * 252
    active = p - b
    tracking_error = float(np.std(active, ddof=1)) * (252 ** 0.5)
    ir = (alpha / tracking_error) if tracking_error > 0 else 0.0
    return {
        "alpha": float(alpha),
        "beta": float(beta),
        "tracking_error": float(tracking_error),
        "information_ratio": float(ir),
    }


def _max_drawdown_and_duration(equities) -> tuple:
    """返回 (max_drawdown, 最长连续水下 valuation days)。"""
    peak = float("-inf")
    max_dd, dur, worst_dur = 0.0, 0, 0
    for v in equities:
        v = float(v)
        if v > peak:
            peak = v
            dur = 0                      # 创新高 → 回到水面
        else:
            dur += 1                     # 水下一天
            worst_dur = max(worst_dur, dur)
            dd = (v - peak) / peak if peak > 0 else 0.0
            max_dd = min(max_dd, dd)
    return max_dd, worst_dur


def compute_metrics(result, initial_cash: float) -> BacktestMetrics:
    """纯后处理：从 BacktestResult 计算 BacktestMetrics。不触碰引擎。"""
    ec = result.equity_curve
    tr = result.trades
    pdl = result.positions_daily
    diag = result.execution_diagnostics or {}

    m = BacktestMetrics()

    # ---------------- performance ----------------
    if ec.height > 0:
        eq = ec["equity"]
        first, last = float(eq.head(1)[0]), float(eq.tail(1)[0])
        n = ec.height
        p = m.performance
        p.total_return = (last / first) - 1 if first > 0 else 0.0
        years = n / TRADING_DAYS
        p.annualized_return = ((last / first) ** (1 / years) - 1
                               ) if first > 0 and years > 0 else 0.0
        p.max_drawdown, p.drawdown_duration = _max_drawdown_and_duration(eq.to_list())

        eq_list = eq.to_list()
        if len(eq_list) >= 2:
            daily_returns = [
                (eq_list[i] / eq_list[i - 1]) - 1
                for i in range(1, len(eq_list))
            ]
            m.sortino_ratio = _sortino_ratio(daily_returns)
            m.calmar_ratio = _calmar_ratio(p.annualized_return, p.max_drawdown)

    # ---------------- trading ----------------
    t = m.trading
    if tr.height > 0:
        buy = tr.filter(pl.col("side") == "BUY")
        sell = tr.filter(pl.col("side") == "SELL")
        t.buy_count, t.sell_count = buy.height, sell.height
        t.trade_count = tr.height
        t.trade_days = tr["execution_date"].n_unique()
        t.gross_buy = float((buy["qty"] * buy["price"]).sum() or 0.0)
        t.gross_sell = float((sell["qty"] * sell["price"]).sum() or 0.0)
        t.total_fees = round(float(tr["fee"].sum() or 0.0), 2)
        t.avg_trade_value = (
            (t.gross_buy + t.gross_sell) / t.trade_count if t.trade_count else 0.0
        )
    if ec.height > 0:
        mean_eq = float(ec["equity"].mean() or 0.0)
        t.turnover = ((t.gross_buy + t.gross_sell) / (2 * mean_eq)) if mean_eq > 0 else 0.0
    t.active_position_days = pdl.height

    # ---------------- exposure ----------------
    x = m.exposure
    if ec.height > 0:
        daily_pv = {
            r["date"]: r["pv"]
            for r in pdl.group_by("date")
            .agg(pl.col("market_value").sum().alias("pv")).to_dicts()
        }
        dep = []
        for r in ec.to_dicts():
            pv = daily_pv.get(r["date"], 0.0)
            dep.append(pv / r["equity"] if r["equity"] > 0 else 0.0)
        if dep:
            s = sorted(dep)
            x.deployment_min = s[0]
            x.deployment_max = s[-1]
            x.deployment_median = _percentile(s, 0.5)
            x.deployment_p10 = _percentile(s, 0.10)
            x.deployment_p25 = _percentile(s, 0.25)
            x.deployment_p75 = _percentile(s, 0.75)
            x.deployment_p90 = _percentile(s, 0.90)
            mean_dep = sum(dep) / len(dep)
            x.deployment_mean = mean_dep
            x.cash_drag = 1.0 - mean_dep
        tg = diag.get("target_gross_by_date", {})
        if tg:
            mean_tg = sum(tg.values()) / len(tg)
            x.target_fill_ratio = (x.deployment_mean / mean_tg) if mean_tg > _EPS else 0.0

    # ---------------- execution_quality ----------------
    q = m.execution_quality
    intents_total = diag.get("intents_total", 0)
    rej = diag.get("rej_counters", {})
    q.dust_reject_count = rej.get("BELOW_LOT", 0)
    q.limit_blocked_count = rej.get("LIMIT_BLOCKED", 0)
    q.partial_fill_count = diag.get("partial_fill_count", 0)
    q.carried_events = diag.get("carried_events", 0)

    def ratio(n, d):
        return n / d if d and d > 0 else 0.0

    q.dust_reject_ratio = ratio(q.dust_reject_count, intents_total)
    q.limit_blocked_ratio = ratio(q.limit_blocked_count, intents_total)
    q.partial_fill_ratio = ratio(q.partial_fill_count, intents_total)
    position_days = pdl.height
    q.carried_event_ratio = ratio(q.carried_events, position_days)

    # ---------------- integrity ----------------
    i = m.integrity
    i.zero_price_trade_count = diag.get("zero_price_trade_count", 0)
    i.t1_violation_count = diag.get("t1_violation_count", 0)
    i.negative_cash_count = diag.get("negative_cash_count", 0)
    i.accounting_invariant_violations = diag.get("accounting_invariant_violations", 0)

    # ---------------- concentration ----------------
    c = m.concentration
    if pdl.height > 0:
        daily_stats = (
            pdl.with_columns(
                (pl.col("market_value") / pl.col("market_value").sum().over("date"))
                .alias("weight")
            )
            .group_by("date")
            .agg(
                (pl.col("weight") ** 2).sum().alias("hhi"),
                (pl.col("weight") - 1.0 / pl.len()).abs().mean().alias("w_mae"),
                pl.len().alias("n_pos"),
            )
            .sort("date")
        )
        hhi_vals = daily_hhi["hhi"].to_list() if False else daily_stats["hhi"].to_list()
        eff_vals = [(1.0 / h) if h > _EPS else 0.0 for h in hhi_vals]
        n_vals = daily_stats["n_pos"].to_list()
        wae_vals = daily_stats["w_mae"].to_list()

        if n_vals:
            c.actual_n_mean = sum(n_vals) / len(n_vals)
        if eff_vals:
            c.effective_n_mean = sum(eff_vals) / len(eff_vals)
        if hhi_vals:
            c.hhi_mean = sum(hhi_vals) / len(hhi_vals)
        if wae_vals:
            c.target_weight_mae = sum(wae_vals) / len(wae_vals)

    return m
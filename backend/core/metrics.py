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
    """组合集中度指标：实际权重分布与目标等权的偏离程度。"""
    hhi_mean: float = 0.0               # Herfindahl-Hirschman Index = Σ(weight_i²)，日均值
    effective_n_mean: float = 0.0       # 1 / HHI，越接近 top_n 越接近理想分散
    weight_deviation_mean: float = 0.0  # mean(|actual_weight - target_weight|) 跨持仓跨日


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

    def to_flat_dict(self) -> dict:
        out = {}
        for group_name in ("performance", "trading", "exposure",
                           "concentration",
                           "execution_quality", "integrity"):
            for k, v in getattr(self, group_name).__dict__.items():
                out[f"{group_name}.{k}"] = v
        return out


# -------------------------------------------------------- calculator ----

def _percentile(sorted_vals, p: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = (len(sorted_vals) - 1) * p
    lo, hi = int(idx), min(int(idx) + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


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
        daily_hhi = (
            pdl.with_columns(
                (pl.col("market_value") / pl.col("market_value").sum().over("date"))
                .alias("weight")
            )
            .group_by("date")
            .agg((pl.col("weight") ** 2).sum().alias("hhi"),
                 pl.len().alias("n_pos"))
            .sort("date")
        )
        hhi_vals = daily_hhi["hhi"].to_list()
        if hhi_vals:
            c.hhi_mean = sum(hhi_vals) / len(hhi_vals)
            c.effective_n_mean = (1.0 / c.hhi_mean) if c.hhi_mean > _EPS else 0.0

        # weight_deviation: |effective_n - actual_n| / actual_n，日均值
        n_vals = daily_hhi["n_pos"].to_list()
        devs = []
        for h, np_ in zip(hhi_vals, n_vals):
            if np_ > 0 and h > _EPS:
                eff = 1.0 / h
                devs.append(abs(eff - np_) / np_)
        if devs:
            c.weight_deviation_mean = sum(devs) / len(devs)

    return m
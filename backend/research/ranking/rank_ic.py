# -*- coding: utf-8 -*-
"""Rank IC 模块：衡量 score 横截面预测能力。

Rank IC(T) = Spearman(score(T), future_return(T→T+K))

指标：
  IC mean     — 所有信号日的 Rank IC 均值
  IC std      — 标准差
  ICIR        — IC mean / IC std（信息比率）
  IC positive — IC > 0 的比例

约束：
  - score(T) 只使用 ≤ T 的数据（no-lookahead）
  - future_return 只出现在评价标签中，不进入 ranking 过程
"""
import datetime
from dataclasses import dataclass
from typing import Optional

import numpy as np
import polars as pl


@dataclass
class RankICResult:
    """Rank IC 评价指标。"""
    ic_mean: float = 0.0
    ic_std: float = 0.0
    icir: float = 0.0
    ic_positive_ratio: float = 0.0
    n_dates: int = 0
    ic_series: list = None  # list of (date, ic_value)

    def __post_init__(self):
        if self.ic_series is None:
            self.ic_series = []


def _spearman_rank_corr(scores: np.ndarray, returns: np.ndarray) -> float:
    """计算 Spearman 秩相关系数。

    等价于 scipy.stats.spearmanr，但避免引入 scipy 依赖。
    使用排名平均法（average ranks）。
    """
    n = len(scores)
    if n < 3:
        return 0.0

    def _avg_rank(arr):
        sorted_idx = np.argsort(arr)
        ranks = np.empty_like(sorted_idx, dtype=float)
        ranks[sorted_idx] = np.arange(1, n + 1, dtype=float)
        # 处理 tie：相同值取平均排名
        for i in range(n):
            val = arr[i]
            ties = np.where(arr == val)[0]
            if len(ties) > 1:
                avg = np.mean([ranks[t] for t in ties])
                for t in ties:
                    ranks[t] = avg
        return ranks

    rank_s = _avg_rank(scores)
    rank_r = _avg_rank(returns)

    mean_s = np.mean(rank_s)
    mean_r = np.mean(rank_r)
    cov = np.sum((rank_s - mean_s) * (rank_r - mean_r))
    std_s = np.sqrt(np.sum((rank_s - mean_s) ** 2))
    std_r = np.sqrt(np.sum((rank_r - mean_r) ** 2))

    if std_s == 0 or std_r == 0:
        return 0.0
    return float(cov / (std_s * std_r))


def compute_rank_ic(
    scores_by_date: dict[datetime.date, dict[str, float]],
    df_daily: pl.DataFrame,
    forward_days: int = 5,
    signal_dates: list[datetime.date] = None,
) -> RankICResult:
    """计算 Rank IC。

    Args:
        scores_by_date: {signal_date: {code: score}} 评分结果
        df_daily: 完整的 daily 数据（用于计算 future return）
        forward_days: 未来收益窗口（T+forward_days 收盘价 / T 收盘价 - 1）
        signal_dates: 用于计算 IC 的信号日列表（None 时使用 scores_by_date 的 keys）

    Returns:
        RankICResult
    """
    if signal_dates is None:
        signal_dates = sorted(scores_by_date.keys())

    ic_values = []

    for t in signal_dates:
        scores = scores_by_date.get(t, {})
        if not scores or len(scores) < 3:
            continue

        codes = list(scores.keys())
        score_vals = np.array([scores[c] for c in codes])

        # 计算未来收益：T 日收盘 → T+forward_days 日收盘
        future_returns = []
        valid_codes = []
        valid_scores = []

        for i, code in enumerate(codes):
            # T 日的 close
            t_data = df_daily.filter(
                (pl.col("code") == code) & (pl.col("date") == t)
            )
            if t_data.is_empty():
                continue
            close_t = t_data["close"][0]

            # T+K 日的 close
            future_data = df_daily.filter(
                (pl.col("code") == code) & (pl.col("date") > t)
            ).sort("date").head(forward_days)
            if future_data.is_empty():
                continue
            close_tk = future_data["close"][-1]

            if close_t and close_t > 0 and close_tk and close_tk > 0:
                ret = close_tk / close_t - 1.0
                future_returns.append(ret)
                valid_codes.append(code)
                valid_scores.append(score_vals[i])

        if len(valid_codes) < 3:
            continue

        ic = _spearman_rank_corr(np.array(valid_scores), np.array(future_returns))
        ic_values.append((t, ic))

    if not ic_values:
        return RankICResult()

    ic_vals = [v for _, v in ic_values]
    ic_mean = float(np.mean(ic_vals))
    ic_std = float(np.std(ic_vals))
    icir = ic_mean / ic_std if ic_std > 0 else 0.0
    ic_pos = sum(1 for v in ic_vals if v > 0) / len(ic_vals)

    return RankICResult(
        ic_mean=ic_mean,
        ic_std=ic_std,
        icir=icir,
        ic_positive_ratio=ic_pos,
        n_dates=len(ic_values),
        ic_series=ic_values,
    )

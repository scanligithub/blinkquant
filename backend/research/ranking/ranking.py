# -*- coding: utf-8 -*-
"""Ranking Contract 与内置 ranking 实现。

RankingFn 协议：
    (frame: pl.DataFrame, signal_date: datetime.date) -> pl.DataFrame

    输入：frame 包含 ≤ signal_date 的 daily 数据（code, date, close 等列）
    输出：pl.DataFrame[code, score]，按 score desc 排序，相同 score 按 code asc tie-break

内置 ranking：
    code_asc_ranking     — R0，确定性 null ordering（所有 score=0）
    strength_desc_ranking — R1，CLOSE/MA20 desc（趋势最强优先）
    strength_asc_ranking  — R2，CLOSE/MA20 asc（最接近/低于 MA20 优先）
"""
import datetime
from typing import Protocol

import polars as pl


class RankingFn(Protocol):
    """Ranking 函数签名。"""
    def __call__(self, frame: pl.DataFrame, signal_date: datetime.date) -> pl.DataFrame:
        """对 eligible set 中的股票评分并排序。

        Args:
            frame: 包含 ≤ signal_date 的 daily 数据（code, date, close 等列）
            signal_date: 信号日（T），ranking 只能使用 ≤ T 的数据

        Returns:
            pl.DataFrame 包含 'code' 和 'score' 列，按 score desc 排序
        """
        ...


def _safe_div(a: pl.Expr, b: pl.Expr, default: float = 0.0) -> pl.Expr:
    """安全除法：b==0 时返回 default。"""
    return pl.when(b > 0).then(a / b).otherwise(pl.lit(default))


def _apply_tie_break(df: pl.DataFrame) -> pl.DataFrame:
    """Apply deterministic tie-break: same score → code asc."""
    return df.sort(["score", "code"], descending=[True, False])


def _compute_ma20(df: pl.DataFrame) -> pl.DataFrame:
    """为 frame 中的每只股票计算 MA20。

    要求 frame 已按 code, date 排序。返回 frame 新增 'ma20' 列。
    """
    return df.with_columns(
        pl.col("close")
        .rolling_mean(window_size=20)
        .over("code")
        .alias("ma20")
    )


def code_asc_ranking(frame: pl.DataFrame, signal_date: datetime.date) -> pl.DataFrame:
    """R0: 确定性 null ordering。所有 score=0，排序完全由 code asc 决定。

    用途：作为 ranking 实验的 control group（无经济排序信息）。
    """
    codes = frame.filter(pl.col("date") == signal_date)["code"].unique().to_list()
    if not codes:
        return pl.DataFrame({"code": [], "score": []})
    return _apply_tie_break(
        pl.DataFrame({"code": sorted(codes), "score": [0.0] * len(codes)})
    )


def _strength_score(frame: pl.DataFrame, signal_date: datetime.date) -> pl.DataFrame:
    """计算 CLOSE/MA20 score（共用逻辑）。"""
    # 只保留 signal_date 的数据，计算 MA20
    day_df = frame.filter(pl.col("date") == signal_date)
    if day_df.is_empty():
        return pl.DataFrame({"code": [], "score": []})

    # 计算 MA20：用 ≤ signal_date 的所有数据做 rolling mean，取 signal_date 那天的值
    scored = (
        frame
        .sort(["code", "date"])
        .with_columns(
            pl.col("close").rolling_mean(window_size=20).over("code").alias("ma20")
        )
        .filter(pl.col("date") == signal_date)
        .select(["code", "close", "ma20"])
        .with_columns(_safe_div(pl.col("close"), pl.col("ma20")).alias("score"))
        .filter(pl.col("score").is_not_null() & pl.col("score").is_finite())
        .select(["code", "score"])
    )
    return scored


def strength_desc_ranking(frame: pl.DataFrame, signal_date: datetime.date) -> pl.DataFrame:
    """R1: CLOSE / MA20 desc。趋势延伸最强的股票优先。

    用途：测试"追最强"策略的横截面预测能力（预期：负向，均值回归效应）。
    """
    scored = _strength_score(frame, signal_date)
    return _apply_tie_break(scored)


def strength_asc_ranking(frame: pl.DataFrame, signal_date: datetime.date) -> pl.DataFrame:
    """R2: CLOSE / MA20 asc。最接近或低于 MA20 的股票优先（负延伸优先）。

    用途：测试"均值回归"方向的横截面预测能力。
    """
    scored = _strength_score(frame, signal_date)
    if scored.is_empty():
        return scored
    return scored.sort(["score", "code"], descending=[False, False])


RANKINGS = {
    "code_asc": code_asc_ranking,
    "strength_desc": strength_desc_ranking,
    "strength_asc": strength_asc_ranking,
}

"""指标注册表：BlinkQuant 选股 DSL 的单一事实来源。

约定：每个注册函数签名 f(column: pl.Expr, n: int) -> pl.Expr，
第一个参数永远是白名单字段列，第二个永远是正整数窗口常量。
开发者新增指标只需在此字典加一项，AST 白名单 / Hot-JIT / nl-meta 自动派生。
"""

import polars as pl

INDICATORS = {
    "MA":  {"func": lambda c, n: c.rolling_mean(window_size=n),            "window": True},
    "EMA": {"func": lambda c, n: c.ewm_mean(span=n, adjust=False),          "window": True},
    "STD": {"func": lambda c, n: c.rolling_std(window_size=n),             "window": True},
    "ROC": {"func": lambda c, n: ((c / c.shift(n).over("code")) - 1) * 100, "window": True},
    "REF": {"func": lambda c, n: c.shift(n).over("code"),                  "window": True},
}

# 字段白名单：必须与 security.py 现有 fields 键集逐项一致（防 drift）
FIELDS = [
    "CLOSE", "OPEN", "HIGH", "LOW", "VOL", "AMOUNT", "PCT_CHG", "S_CLOSE",
    "PE_TTM", "PB_MRQ", "FORECAST_YOY", "IS_FORECAST_GOOD", "IS_FORECAST_BAD",
    "TOTAL_SHARES", "FLOAT_SHARES", "TOTAL_MV", "FLOAT_MV", "TURN",
]

# 单位标注：用于 LLM 提示词与前端展示（覆盖全部白名单字段）
UNITS = {
    "TOTAL_MV": "元", "FLOAT_MV": "元", "TOTAL_SHARES": "股",
    "FLOAT_SHARES": "股", "AMOUNT": "元", "VOL": "股",
    "CLOSE": "元", "OPEN": "元", "HIGH": "元", "LOW": "元",
    "PE_TTM": "无量纲(倍)", "PB_MRQ": "无量纲(倍)", "TURN": "百分比(%)",
    "FORECAST_YOY": "百分比(%)", "PCT_CHG": "百分比(%)", "S_CLOSE": "指数点位",
    "IS_FORECAST_GOOD": "布尔标记(0/1)", "IS_FORECAST_BAD": "布尔标记(0/1)",
}

EXAMPLE_QUERIES = [
    "CLOSE > MA(CLOSE, 20)",
    "PE_TTM < 20 AND TOTAL_MV > 1e10",
]

TIMEFRAMES = ["D", "W", "M"]

# 供 Hot-JIT 与动态正则使用的纯函数子集（window 型）
INDICATOR_FUNCS = {name: entry["func"] for name, entry in INDICATORS.items() if entry.get("window")}
INDICATOR_NAMES = sorted(INDICATOR_FUNCS.keys())


def nl_meta() -> dict:
    """nl-meta 接口数据（注册表驱动的单一事实来源）"""
    return {
        "fields": FIELDS,
        "indicators": INDICATOR_NAMES,
        "timeframes": TIMEFRAMES,
        "units": UNITS,
        "example_queries": EXAMPLE_QUERIES,
    }

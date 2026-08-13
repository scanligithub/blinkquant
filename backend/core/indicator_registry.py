"""指标注册表：BlinkQuant 选股 DSL 的单一事实来源。

约定：注册函数签名由每条目的 "signature" 字段声明，参数形态取值：
- field   = 白名单字段的 ast.Name
- pos_int = 正整数常量（1 ≤ n ≤ 500）
- series  = field 或 一层窗口函数调用（如 MA(CLOSE,20)）
- cond    = 布尔表达式（Compare > >= < <= 或 AND/OR 组合）

"window": True 的条目签名恒为 [field, pos_int]，参与 Hot-JIT 挂载/统计；
其余为慢路径实时计算。开发者新增指标只需在此字典加一项。
"""

import functools
import polars as pl


def cross_up(a, b):
    prev_a, prev_b = a.shift(1).over("code"), b.shift(1).over("code")
    return (a > b) & (prev_a <= prev_b)


def cross_down(a, b):
    prev_a, prev_b = a.shift(1).over("code"), b.shift(1).over("code")
    return (a < b) & (prev_a >= prev_b)


def count(cond, n):
    return cond.cast(pl.Int32).rolling_sum(window_size=n).over("code")


def barslast(cond):
    row = pl.int_range(pl.len()).over("code")
    anchor = pl.when(cond).then(row).otherwise(None)
    filled = anchor.forward_fill().over("code")
    return (row - filled).cast(pl.Int32)


INDICATORS = {
    # ---- window 型（签名 [field, pos_int]，Hot-JIT 挂载）----
    "MA":  {"func": lambda c, n: c.rolling_mean(window_size=n).over("code"),            "window": True, "signature": ["field", "pos_int"]},
    "EMA": {"func": lambda c, n: c.ewm_mean(span=n, adjust=False).over("code"),          "window": True, "signature": ["field", "pos_int"]},
    "STD": {"func": lambda c, n: c.rolling_std(window_size=n).over("code"),             "window": True, "signature": ["field", "pos_int"]},
    "ROC": {"func": lambda c, n: ((c / c.shift(n).over("code")) - 1) * 100, "window": True, "signature": ["field", "pos_int"]},
    "REF": {"func": lambda c, n: c.shift(n).over("code"),                               "window": True, "signature": ["field", "pos_int"]},
    "HHV": {"func": lambda c, n: c.rolling_max(window_size=n).over("code"),             "window": True, "signature": ["field", "pos_int"]},
    "LLV": {"func": lambda c, n: c.rolling_min(window_size=n).over("code"),             "window": True, "signature": ["field", "pos_int"]},
    "SUM": {"func": lambda c, n: c.rolling_sum(window_size=n).over("code"),             "window": True, "signature": ["field", "pos_int"]},
    # ---- 非 window 型（慢路径实时计算）----
    "CROSS_UP":   {"func": cross_up,   "window": False, "signature": ["series", "series"]},
    "CROSS_DOWN": {"func": cross_down, "window": False, "signature": ["series", "series"]},
    "MAX": {"func": lambda a, b: pl.max_horizontal(a, b), "window": False, "signature": ["series", "series"]},
    "MIN": {"func": lambda a, b: pl.min_horizontal(a, b), "window": False, "signature": ["series", "series"]},
    "ABS": {"func": lambda x: x.abs(), "window": False, "signature": ["series"]},
    "COUNT":    {"func": count,    "window": False, "signature": ["cond", "pos_int"]},
    "BARSLAST": {"func": barslast, "window": False, "signature": ["cond"]},
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

# 算子中文说明：用于 LLM 提示词（value 会拼入 buildSystemPrompt）
DESCRIPTIONS = {
    "MA": "N日简单移动平均", "EMA": "N日指数移动平均", "STD": "N日标准差",
    "ROC": "N日变动率(%)", "REF": "N日前值", "HHV": "N周期内最高值",
    "LLV": "N周期内最低值", "SUM": "N周期内求和",
    "CROSS_UP": "上穿（今日A>B且昨日A<=B）", "CROSS_DOWN": "下穿（今日A<B且昨日A>=B）",
    "MAX": "取两序列较大值", "MIN": "取两序列较小值", "ABS": "绝对值",
    "COUNT": "N周期内条件成立次数", "BARSLAST": "距上次条件成立周期数",
}

EXAMPLE_QUERIES = [
    "CLOSE > MA(CLOSE, 20)",
    "PE_TTM < 20 AND TOTAL_MV > 1e10",
    "CROSS_UP(MA(CLOSE, 20), MA(CLOSE, 60))",
    "SUM(AMOUNT, 5) > 5e9",
]

TIMEFRAMES = ["D", "W", "M"]

# window 型纯函数子集（供 Hot-JIT 与动态正则）
INDICATOR_FUNCS = {name: entry["func"] for name, entry in INDICATORS.items() if entry.get("window")}
WINDOW_NAMES = sorted(INDICATOR_FUNCS.keys())
INDICATOR_NAMES = sorted(INDICATORS.keys())


def nl_meta() -> dict:
    """nl-meta 接口数据（注册表驱动的单一事实来源）"""
    return {
        "fields": FIELDS,
        "indicators": INDICATOR_NAMES,
        "timeframes": TIMEFRAMES,
        "units": UNITS,
        "example_queries": EXAMPLE_QUERIES,
        "signatures": {name: entry["signature"] for name, entry in INDICATORS.items()},
        "descriptions": DESCRIPTIONS,
    }

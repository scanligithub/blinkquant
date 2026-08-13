# DSL 算子扩展：CROSS/HHV/LLV/SUM/ABS/MAX/MIN/COUNT/BARSLAST 设计文档

版本: v1.0 | 日期: 2026-08-13 | 状态: 待评审

---

## 背景与目标

自然语言选股（NL 选股）已上线，但「20日均线上穿60日均线」这类交叉需求被强校验拒绝——现有 DSL 只支持 `FUNC(FIELD, N)` 单层调用（参数必须是纯字段名+正整数窗口），不支持嵌套/条件/二元算子。用户需求是补齐 A 股选股常见算子。

## 目标

- 新增 9 个算子：`HHV` / `LLV` / `SUM` / `CROSS_UP` / `CROSS_DOWN` / `MAX` / `MIN` / `ABS` / `COUNT` / `BARSLAST`
- 把安全层从"两层节点校验"升级为**注册表签名驱动的递归校验器**
- 保持「指标注册表 = 单一事实来源」哲学：加算子只改注册表，AST/引擎/prompt/前端校验自动派生
- 现有 `MA/EMA/STD/ROC/REF` 与全部现有合法公式向后兼容，结果不变

## 架构决策

| 决策点 | 结论 |
|---|---|
| 参数形态建模 | 注册表条目新增 `signature` 字段，声明每个参数形态（`field` / `pos_int` / `series` / `cond`） |
| 安全层 | `_visit(ast.Call)` 改为按签名递归校验参数 |
| CROSS 判定 | `CROSS_UP(A,B)` = 今日 `A>B` 且昨日 `A<=B`；`CROSS_DOWN` 对称（含等号，通达信对齐） |
| COUNT/BARSLAST 条件 | 支持单比较与 `AND/OR` 布尔组合 |
| Hot-JIT | `metric_pattern` 升级为从 `FIELDS` 全量生成，顺带修复 `VOL→vol` 与真实列 `volume` 不符 bug |
| 窗口上限 | `pos_int` 校验带上限 500，防超大 rolling 计算 |

## 新算子签名表

| 算子 | signature | 返回 | 语义 | 示例 |
|---|---|---|---|---|
| `HHV` | `[field, pos_int]` | 数值 | N 周期最高 | `HHV(CLOSE, 20)` |
| `LLV` | `[field, pos_int]` | 数值 | N 周期最低 | `LLV(CLOSE, 20)` |
| `SUM` | `[field, pos_int]` | 数值 | N 周期求和 | `SUM(AMOUNT, 5)` |
| `CROSS_UP` | `[series, series]` | 布尔 | 上穿（今日 A>B 且昨日 A<=B） | `CROSS_UP(MA(CLOSE,20), MA(CLOSE,60))` |
| `CROSS_DOWN` | `[series, series]` | 布尔 | 下穿 | `CROSS_DOWN(MA(CLOSE,20), MA(CLOSE,60))` |
| `MAX` | `[series, series]` | 数值 | 取大 | `MAX(CLOSE, OPEN)` |
| `MIN` | `[series, series]` | 数值 | 取小 | `MIN(CLOSE, OPEN)` |
| `ABS` | `[series]` | 数值 | 绝对值 | `ABS(PCT_CHG)` |
| `COUNT` | `[cond, pos_int]` | 数值 | N 周期内条件成立次数 | `COUNT(CLOSE > MA(CLOSE,20), 10)` |
| `BARSLAST` | `[cond]` | 数值 | 距上次条件成立周期数 | `BARSLAST(CLOSE > MA(CLOSE,20))` |

**参数形态定义：**
- `field` = 白名单字段的 `ast.Name`（`FIELDS`）
- `pos_int` = 正整数 `ast.Constant`（1 ≤ n ≤ 500）
- `series` = `field` 或 窗口函数调用（如 `MA(CLOSE,20)`），即嵌套一层
- `cond` = 布尔表达式：`Compare(series op series|number)` 或 `BoolOp(AND/OR)` 组合，比较运算符限定 `> >= < <=`

**向后兼容：** 现有 `MA/EMA/STD/ROC/REF` 保持 signature `[field, pos_int]`，行为不变。

## 后端设计

### 1. `backend/core/indicator_registry.py`

`INDICATORS` 条目扩展 `signature` 字段，并新增算子的 lambda 实现：

```python
INDICATORS = {
    # 原有（签名 [field, pos_int]，window 型 → Hot-JIT 挂载）
    "MA":  {"func": ..., "window": True, "signature": ["field", "pos_int"]},
    ...
    # 新增窗口类
    "HHV": {"func": lambda c, n: c.rolling_max(window_size=n).over("code"), "window": True, "signature": ["field", "pos_int"]},
    "LLV": {"func": lambda c, n: c.rolling_min(window_size=n).over("code"), "window": True, "signature": ["field", "pos_int"]},
    "SUM": {"func": lambda c, n: c.rolling_sum(window_size=n).over("code"), "window": True, "signature": ["field", "pos_int"]},
    # 非 window 型（慢路径实时计算，不挂载）
    "CROSS_UP":   {"func": cross_up,   "window": False, "signature": ["series", "series"]},
    "CROSS_DOWN": {"func": cross_down, "window": False, "signature": ["series", "series"]},
    "MAX": {"func": lambda a, b: pl.max_horizontal(a, b), "window": False, "signature": ["series", "series"]},
    "MIN": {"func": lambda a, b: pl.min_horizontal(a, b), "window": False, "signature": ["series", "series"]},
    "ABS": {"func": lambda x: x.abs(), "window": False, "signature": ["series"]},
    "COUNT":     {"func": count,     "window": False, "signature": ["cond", "pos_int"]},
    "BARSLAST":  {"func": barslast,  "window": False, "signature": ["cond"]},
}
```

CROSS / COUNT / BARSLAST 需处理 `shift` 与布尔→数值转换：

```python
def cross_up(a, b):
    prev_a, prev_b = a.shift(1).over("code"), b.shift(1).over("code")
    return (a > b) & (prev_a <= prev_b)

def cross_down(a, b):
    prev_a, prev_b = a.shift(1).over("code"), b.shift(1).over("code")
    return (a < b) & (prev_a >= prev_b)

def count(cond, n):
    return cond.cast(pl.Int32).rolling_sum(window_size=n).over("code")

def barslast(cond):
    """距上次条件成立的行数；首次成立前为 null。
    实现：按 code 分组，条件为真处锚定为当前行号，forward_fill 后取差值。"""
    row = pl.int_range(pl.len()).over("code")
    anchor = pl.when(cond).then(row).otherwise(None)
    filled = anchor.forward_fill().over("code")
    return (row - filled).cast(pl.Int32)
```

注意：`INDICATOR_FUNCS`（供 Hot-JIT / metric_pattern）只应含 `window: True` 的条目；`INDICATOR_NAMES` 需拆分为"全部名称（nl-meta/prompt 展示）"与"window 名称（Hot-JIT 统计）"两组。

### 2. `backend/core/security.py`

`_visit(ast.Call)` 从"查注册表+断言两参"改为按签名递归校验：

```python
elif isinstance(node, ast.Call):
    func = node.func.id.upper()
    entry = INDICATORS.get(func)
    if entry is None:
        raise ValueError(f"Unknown function {func}")
    sig = entry["signature"]
    if len(node.args) != len(sig) or node.keywords:
        raise ValueError(f"Function {func} expects {len(sig)} positional args")
    args = [self._visit_arg(a, s, func) for a, s in zip(node.args, sig)]
    # window 型：快路径（Hot-JIT 挂载列）优先
    if entry.get("window"):
        field_name, n = args
        pure_key = f"{func}_{field_name}_{n}"
        if self.current_df is not None and pure_key in self.current_df.columns:
            return pl.col(pure_key)
        return entry["func"](pl.col(field_name.lower()), n)
    return entry["func"](*args)
```

参数形态校验器通过 `_visit` 复用实现（模块级纯函数，供单测直接覆盖）：

```python
def _require_series(node, func) -> pl.Expr:
    """series = 字段 或 一层窗口函数调用（复用 _visit：快路径优先）"""
    if isinstance(node, ast.Name):
        name = _require_whitelist_field(node)
        return pl.col(name.lower())
    if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
            and node.func.id.upper() in WINDOW_NAMES):
        return _visit(node)   # 复用窗口调用分支（含 Hot-JIT 快路径）
    raise ValueError(f"Function {func} arg must be a field or window indicator call")

def _require_cond(node, func) -> pl.Expr:
    """cond = Compare(series op series|number) 或 BoolOp(AND/OR) 组合。
    先做结构白名单校验（节点类型/运算符/操作数形态，递归深度受限），
    校验通过后委托 _visit 求值（Compare/BoolOp 分支已存在）。
    禁止嵌套 COUNT/BARSLAST 或任意深调用。"""
    validate_cond_structure(node, func)   # 白名单：Compare(> >= < <=) 或 BoolOp(AND/OR)
    return _visit(node)
```

`_require_positive_int` 增加窗口上限：`1 ≤ n ≤ 500`。`_require_number` 限定为数值 `ast.Constant`（int/float，不含 bool）。

关键：`validate_cond_structure` 是**纯结构校验器**（返回 bool 或抛错），与求值分离——保证白名单安全模型「先形态校验、后求值」不破坏；`_visit` 里 Compare/BoolOp 分支保留现状。

**重要安全约束（保持）**：`series` 只允许一层嵌套（不能再套 CROSS/COUNT 等）；`cond` 的操作数只能是 `series` 或数值常量，不允许裸函数返回值再做算术运算。递归深度受限，AST 白名单安全模型不破坏。

### 3. `backend/core/data_manager.py`

`INDICATOR_MAP = {name: entry["func"] for name, entry in INDICATORS.items() if entry.get("window")}` —— 保持 window 型子集（现有 `INDICATOR_FUNCS` 已如此），无需改动。

### 4. `backend/core/engine.py`

`metric_pattern` 现硬编码 `(CLOSE|OPEN|HIGH|LOW|VOL|AMOUNT)`，升级为从 `FIELDS` 全量生成，并修复 `VOL→pl.col('vol')` 与真实列 `volume` 不符的 bug：

```python
_fields = "|".join(FIELDS)
self.metric_pattern = re.compile(
    rf'\b({_funcs})\s*\(\s*({_fields})\s*,\s*(\d+)\s*\)', re.IGNORECASE)
```

`_prepare_hot_jit` 中的字段→列映射改为查 `blink_parser.fields`（统一映射，`VOL→volume`、`CLOSE→close` 等），避免硬编码。嵌套公式（如 `CROSS_UP(MA(CLOSE,20),...)`）中 `findall` 仍能提取内部窗口调用做预计算。

`_funcs` 只用 window 型名称（`INDICATOR_FUNCS` 的键），非 window 算子（CROSS/COUNT 等）不参与 Hot-JIT 挂载/统计。

### 5. `backend/api/routes.py`

`nl_meta()` 的 `indicators` 返回全部算子名（含 CROSS 等，供 prompt/前端校验）；`METRIC_REGEX` 沿用 `selection_engine.metric_pattern`（仅 window 型）。`EXAMPLE_QUERIES` 增加 CROSS 示例，引导 LLM。

## 前端设计

### 6. `frontend/src/lib/selectNL.ts`

`validateFormula` 从"拒绝嵌套括号"改为按签名递归校验（复制后端同一逻辑，数据驱动自 `nl-meta`）：

- `series` 校验：字段白名单 或 一层窗口调用（`FUNC(FIELD, pos_int)`）
- `cond` 校验：`Compare(series op series|number)` 或 `AND/OR` 组合，递归校验
- `pos_int` 上限 500；括号配对检查保留
- 放行布尔返回的顶层信号（`CROSS_UP(...)` 直接作为公式）

`buildSystemPrompt` 自动注入新算子说明（从 nl-meta 的 indicators + 描述派生），示例含 CROSS。

## 测试

### 后端（`backend/tests/`）
- 签名表覆盖：每个新算子合法形态通过、非法形态（参数数量/形态/嵌套超深/未注册函数）拒绝
- `CROSS_UP/CROSS_DOWN` 判定：构造含昨日数据的 DataFrame，断言穿越/未穿越/恰好相等
- `COUNT`/`BARSLAST`：条件为真/假/AND/OR 组合计数正确
- 窗口上限：`MA(CLOSE, 501)` 拒绝
- 安全回归：现有合法公式结果不变；未知函数/字段/负窗口仍拒绝
- `FIELDS` 与 `security.py` fields 键集一致（防 drift，已有测试）

### 前端（`frontend/tests/select-nl.test.mjs`）
- `validateFormula`：新算子通过/拒绝用例（CROSS 嵌套、COUNT 条件、非法嵌套深、超窗口上限）
- `buildSystemPrompt`：输出含新算子说明

## 部署与验收

1. 后端改动推 main 触发 3 节点部署
2. 手动验收：
   - `20日均线上穿60日均线` → `CROSS_UP(MA(CLOSE,20), MA(CLOSE,60))` 可运行
   - `5日成交额大于50亿` → `SUM(AMOUNT, 5) > 5e9` 可运行
   - `创20日新高` → `CLOSE > HHV(CLOSE, 20)` 可运行
   - `10日内收盘价大于20日均线的天数>=7` → `COUNT(CLOSE > MA(CLOSE,20), 10) >= 7` 可运行
   - 非法嵌套（`CROSS_UP(MA(CLOSE,20), MA(CLOSE,60))` 内再套 CROSS）被拒绝

## 非目标（YAGNI）

- 不做 `AND/OR` 之外的布尔运算（`NOT` 后端不支持，保持排除）
- 不做 `series` 任意深度嵌套（只允许一层窗口调用）
- 不做 `cond` 内嵌套 `COUNT/BARSLAST`（条件操作数仅限 `series` 或数值）
- 不做 MACD/KDJ/RSI 等复合指标（可由基础算子组合表达，非本次范围）
- 不改动后端 `ast.Not` / 其它运算符接受集（final review 记录的独立事项）

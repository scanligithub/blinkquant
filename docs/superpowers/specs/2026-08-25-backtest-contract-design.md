# blinkquant 回测前置契约设计文档

> **基线提交**：`b4960c8`（159/159 测试通过）  
> **设计目标**：在不动现有选股链（已 159/159 绿）的前提下，冻结回测所需的所有上游契约与下游执行语义，形成"可实施、可测试、可演进"的回测架构基线。  
> **核心原则**：**只在已证明 Point-in-Time Safe 的数据上做回测**。

---

## 1. 架构分层契约

```
┌─────────────────────────────────────────────────────────────────┐
│                        研究层 (Selection)                        │
├─────────────────────────────────────────────────────────────────┤
│ Formula / AI  │  D / W / M  │  target_date  │  as-of  │ no-leak  │
└──────────────┬──────────────┘
               │
               ▼ SelectionResult
┌─────────────────────────────────────────────────────────────────┐
│                       交易层 (Backtest)                          │
├─────────────────────────────────────────────────────────────────┤
│ Calendar  │  Execution  │  Portfolio  │  Fees  │  Equity Curve  │
└─────────────────────────────────────────────────────────────────┘
```

**边界原则**：
- SelectionEngine **不知道**：cash、position、commission、slippage、order、fill
- BacktestEngine **不知道**：MA、RSI、公式 DSL、W/M/Multi-TF parser
- 中间**仅**通过 `SelectionResult` 连接

---

## 2. 冻结契约清单（Phase 0 已完成）

### 2.1 价格体系契约

| 层级 | 数据来源 | 生命周期 |
|------|----------|----------|
| **选股/信号** | `df_daily/weekly/monthly` (qfq OHLCV) | 常驻内存，热挂载 |
| **回测成交/估值** | `RawPriceStore.lazy_scan_parquet(window)` | 按回测窗口按需加载，用后释放 |

**实现路径**：`RawPriceStore` 接口 + lazy parquet scan + 窗口缓存，**不**把 raw OHLCV 常驻入 `df_daily`。

---

### 2.2 板块/行业契约

| 能力 | 实时选股 | 历史回测 |
|------|----------|----------|
| `SECTOR_*` / `INDUSTRY_*` | ✅ 当前 `df_mapping` | ❌ **报错**：`BacktestUnsupportedFeature` |

**理由**：现有板块 K 线基于"当前板块体系"重采样，不具备 Point-in-Time 安全性。Phase C 单独建立 `HistoricalSectorEngine` 再接入。

---

### 2.3 SelectionResult 契约

```python
SelectionResult(
    requested_date: date | None,   # 用户原始输入（可为非交易日），API 审计必需
    signal_date: date,              # 归一化后的实际 as-of 交易日
    codes: list[str],               # 选股结果代码列表
    metadata: dict,                 # 扩展字段
)

metadata = {
    "formula": str,
    "timeframe": str,          # 基础周期
    "has_mtf": bool,           # 是否多周期
    "nodes_responding": int,   # 聚合模式下成功节点数
    "degraded": bool,          # 是否降级
}
```

**日期语义**：
- `requested_date`：用户原始输入（可为空/非交易日）
- `signal_date`：归一化后的 as-of 交易日（≤ requested_date 的最近交易日）
- `execution_date = next_trade_day(signal_date)` **由回测层计算**，不放入 SelectionResult

---

### 2.4 执行时序契约（方案 A 标准模式）

| 契约点 | 规则 |
|--------|------|
| **成交时点** | T+1 开盘价（集合竞价成交价） |
| **调仓顺序** | **先卖后买** → 先卖出不在目标组合的股票 → 释放现金 → 再买入目标组合 |
| **卖出回款** | T+1 当日可用于再投资（同一 execution cycle 内），但不可转出银行 |
| **部分成交** | 一次分配 → 失败股票现金留存 → **不**递归重分配 |
| **涨跌停/停牌** | 涨停不可买 / 跌停不可卖 / 停牌跳过 → 记录 partial fill |
| **费用模型** | `FeeConfig` 参数化（见 2.6） |

---

### 2.5 目标组合契约

| 模式 | 规则 |
|------|------|
| **MVP 默认** | 等权：`weight = 1 / len(codes)` |
| **扩展接口** | `allocator(codes: list[str], signal_date: date) -> dict[str, float]` |

---

### 2.6 费用模型契约

```python
@dataclass
class FeeConfig:
    commission_rate: float = 0.00025      # 佣金费率（研究用默认值，非历史真实费率）
    commission_min: float = 5.0           # 最低佣金（研究用默认值）
    stamp_tax_rate: float = 0.0005        # 印花税费率（单边卖出，研究用默认值）
    transfer_fee_rate: float = 0.00001    # 过户费费率（双向，研究用默认值）
```

**原则**：
- 当前默认值为 **研究用默认费率模型**，不代表 A 股历史真实费率
- 不写死常量，支持 `historical_fee_schedule(date)` 后续扩展（Phase 7）

---

### 2.7 T+1 持仓约束契约（标准 A 股现金账户）

| 场景 | 规则 |
|------|------|
| T 日旧仓 | T+1 可卖出 ✅ |
| T+1 新买 | T+1 当日不可卖 ❌，最早 T+2 可卖 |
| 同一股票同一 execution_date | 最多一个净交易方向（BUY / SELL 二选一） |
| 账户类型 | 纯现金账户：不融资、不融券、不日内回转 |

**Position 结构扩展（支持 T+1 可卖/冻结语义）**：
```python
@dataclass
class Position:
    code: str
    total_qty: int
    available_qty: int   # 当日可卖数量（T 日旧仓 + 历史已解冻）
    frozen_qty: int      # 当日冻结数量（T+1 新买入，T+2 解冻）
    avg_cost: float
    market_value: float
```

**解冻规则**：
- T+1 新买入 `qty` → `frozen_qty += qty`
- 次交易日开盘前 → `frozen_qty -= qty`；`available_qty += qty`

---

### 2.8 数据语义边界表

| 数据 | 选股层 | 回测成交层 | 回测估值层 |
|------|--------|------------|------------|
| qfq OHLCV | ✅ 常驻 | ❌ | ❌ |
| raw OHLCV | ❌ | ✅ 按窗口加载 | ✅ 组合估值 |
| limit_up/down | ✅ 复用 | ✅ 复用 | — |
| suspension | ✅ 复用 | ✅ 复用 | — |
| sector/industry | ✅ 实时 | ❌ 报错 | — |

---

## 3. 模块接口契约

### 3.1 SelectionEngine（现有，仅扩展返回值）

```python
SelectionEngine.execute_selector(
    formula: str,
    timeframe: str,
    background_tasks,
    target_date: date | None = None
) -> SelectionResult
```

### 3.2 RawPriceStore（新增）

```python
class RawPriceStore:
    def scan_window(self, start: date, end: date) -> pl.LazyFrame:
        """返回指定日期窗口的 raw OHLCV LazyFrame，支持 predicate pushdown"""
    
    def load_execution_prices(self, dates: list[date]) -> pl.DataFrame:
        """返回指定交易日的 raw_open / raw_close 等成交所需字段"""
```

---

### 3.3 ExecutionConfig / FeeConfig / Allocator（新增接口）

```python
@dataclass
class FeeConfig:
    commission_rate: float = 0.00025      # 佣金费率
    commission_min: float = 5.0           # 最低佣金
    stamp_tax_rate: float = 0.0005        # 印花税费率（单边卖出）
    transfer_fee_rate: float = 0.00001    # 过户费费率（双向）

# ---- MVP 冻结配置（不可修改）----
MVP_EXECUTION_CONFIG = ExecutionConfig(
    price_mode="open",              # 固定：open（集合竞价成交价）
    order_sequence="sell_first",    # 固定：先卖后买
    cash_reinvestment="same_cycle", # 固定：卖出回款当周期可再投资
    partial_fill_policy="keep_cash",# 固定：失败资金留存，不递归重分配
)

@dataclass
class ExecutionConfig:
    price_mode: str = "open"               # MVP 冻结为 "open"（不可改）
    order_sequence: str = "sell_first"     # MVP 冻结为 "sell_first"（不可改）
    cash_reinvestment: str = "same_cycle"  # MVP 冻结为 "same_cycle"（不可改）
    partial_fill_policy: str = "keep_cash" # MVP 冻结为 "keep_cash"（不可改）

# Future extension（暂不实现，接口预留）：
# price_mode: "open" / "vwap" / "close"
# order_sequence: "sell_first" / "simultaneous"
# cash_reinvestment: "same_cycle" / "T+2"
# partial_fill_policy: "keep_cash" / "redistribute"

# MVP 仅内置等权，通过 callable 预留扩展
Allocator = Callable[[list[str], date], dict[str, float]]
```

---

### 3.4 BacktestEngine 接口（规划预览）

```python
class BacktestEngine:
    def __init__(
        self,
        calendar: TradingCalendar,
        selection_engine: SelectionEngine,
        raw_price_store: RawPriceStore,
        fee_config: FeeConfig,
        execution_config: ExecutionConfig = MVP_EXECUTION_CONFIG,
        allocator: Allocator = equal_weight_allocator,
    ):
        ...

    def run(
        self,
        formula: str,
        start_date: date,
        end_signal_date: date,
        initial_cash: float = 1_000_000,
        initial_positions: dict[str, Position] | None = None,  # MVP: 默认 {}
    ) -> BacktestResult:
        ...
```

**MVP 冻结**：
- `rebalance_freq` 固定为 `"daily"`（每个 `signal_date` 调仓）
- `weekly` / `monthly` / `signal` 暂不实现，仅在接口保留字符串字面量以备扩展

---

## 4. 回测事件循环时序（Signal Calendar 模型）

**核心定义**：
- `signal_date` ∈ `[start_date, end_signal_date]` —— 策略观察截止日，也是 `SelectionEngine` 的 `target_date`
- `execution_date = next_trade_day(signal_date)` —— 实际成交日（T+1 开盘）
- `end_date`（可选）默认等于 `end_signal_date`；若需指定最后成交日，请显式传 `end_execution_date`

**区间定义示例**：
```python
start_date = date(2025, 1, 2)
end_signal_date = date(2025, 12, 31)
# 最后 signal_date = 2025-12-31
# 最后 execution_date = 2026-01-02（次一个交易日）
```

**逐日事件循环**：
```
for signal_date in calendar.signal_range(start_date, end_signal_date):
    execution_date = calendar.next_trade_day(signal_date)
    
    # 1. 市场状态 as-of(signal_date)
    # 2. 策略信号 → SelectionEngine(target_date=signal_date) → SelectionResult
    # 3. 目标组合 → allocator(codes, signal_date) → target_weights
    # 4. 订单意图 → diff(current_portfolio, target_weights) → OrderIntent[]
    # 5. 执行 → ExecutionEngine.execute(execution_date, intents) → fills
    # 6. 组合更新 → Portfolio.update(fills) → cash/positions/equity
    # 6. 估值 → raw_close(execution_date) → mark_to_market
    # 7. 快照 → AccountSnapshot(equity, cash, positions, pnl)
```

**边界约束**：
- 最后一个 `signal_date` ≤ `end_signal_date`
- 最后一个 `execution_date` 可能超出 `end_signal_date`（允许），但必须 ≤ `end_execution_date`（若指定）
- 第一个 `signal_date` ≥ `start_date`；若 `start_date` 非交易日，自动归一到 ≥ `start_date` 的首个交易日
```

---

## 5. 测试契约（必须随代码同步冻结）

| 测试层 | 核心断言 |
|--------|----------|
| **Selection Contract** | 同一 `target_date` → 同一 `SelectionResult`；未来新增数据不改变历史 `signal_date` 的结果 |
| **No-Lookahead** | Poisoning differential：投毒 T 后数据 → 结果不变；Truncation equivalence |
| **As-of Frame Invariants** | 所有日期 ≤ target_date；随 T 推进行数/代码数单调非减 |
| **Friday Completion** | 周五 target_date 的 partial week OHLCV == 完整周线 |
| **Canary Detector** | 故意注入泄漏 → 投毒差分必须报警 |
| **Execution Contract** | T+1 买入不可卖 / T+1 旧仓可卖 / 先卖后买资金可用 / 费用计算正确 |
| **Execution Boundary No-Lookahead** | 投毒 execution_date (T+1) 的 open/high/low/close → signal_date (T) 的结果不变；仅 execution 结果受影响 |
| **FeeConfig 参数化** | 修改 FeeConfig → 回测结果按比例变化 |

---

## 6. 实施阶段规划

| Phase | 交付物 | 关键产出 |
|-------|--------|----------|
| **Phase 0** | 契约冻结文档（本文档） | ✅ 已完成 |
| **Phase 1** | `RawPriceStore` + 窗口缓存 | `backend/core/raw_price_store.py` |
| **Phase 2** | `FeeConfig` / `ExecutionConfig` / `Allocator` 接口 | `backend/core/backtest_types.py` |
| **Phase 2** | `SelectionResult` 数据类 | `backend/core/selection_result.py` |
| **Phase 3** | `ExecutionEngine`（T+1 开盘、先卖后买、涨跌停/停牌、费用、部分成交） | `backend/core/execution.py` |
| **Phase 4** | `Portfolio`（现金/持仓/冻结/权益/订单/逐日快照） | `backend/core/portfolio.py` |
| **Phase 5** | `BacktestEngine`（时序推进/逐日事件循环/equity curve/metrics） | `backend/core/backtest_engine.py` |
| **Phase 6** | 组合级 no-lookahead 测试 + 历史费率表 + 端到端验证 | `tests/test_backtest_*.py` |

---

## 7. 验收标准

| 指标 | 标准 |
|------|------|
| **功能完整性** | 所有契约点有对应实现与测试 |
| **无回归** | 原 159 单测全绿 + 新增回测测试全绿 |
| **PIT 安全** | 所有回测测试通过 poisoning differential / truncation equivalence |
| **性能** | 5 年全市场回测 < 60 秒（单节点） |
| **可复现** | 同一参数多次运行 equity curve 逐日完全一致 |

---

## 8. 风险与缓解

| 风险 | 缓解 |
|------|------|
| RawPriceStore parquet IO 瓶颈 | 窗口缓存 + predicate pushdown + 预取 |
| 回测速度 | 向量化执行 + 批量 fee 计算 + 避免逐股票循环 |
| 历史费率变更 | `FeeConfig` + `historical_fee_schedule(date)` 扩展点预留 |
| 板块 PIT 后续接入 | `HistoricalSectorEngine` 独立模块，仅在回测层注入 |

---

## 9. 附录：关键数据结构定义

```python
# SelectionResult
@dataclass
class SelectionResult:
    requested_date: date | None
    signal_date: date
    codes: list[str]
    metadata: dict

# BacktestResult
@dataclass
class BacktestResult:
    equity_curve: pl.DataFrame          # date, equity, cash, positions_value
    trades: pl.DataFrame                # date, code, side, qty, price, fee, pnl
    positions_daily: pl.DataFrame       # date, code, qty, cost, market_value
    metrics: dict                       # CAGR, Sharpe, MaxDD, Turnover, WinRate...

# Position (支持 T+1 可卖/冻结)
@dataclass
class Position:
    code: str
    total_qty: int
    available_qty: int      # 当日可卖数量
    frozen_qty: int         # 冻结数量（T+1 新买入，T+2 解冻）
    avg_cost: float
    market_value: float

# AccountSnapshot (逐日)
@dataclass
class AccountSnapshot:
    date: date
    cash: float
    positions: dict[str, Position]
    equity: float
    daily_pnl: float
```
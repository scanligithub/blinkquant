# ADR-001: RQAlpha as Execution Kernel (Not Backtest Engine)

> **Status**: Accepted  
> **Date**: 2026-08-27  
> **Supersedes**: None (initial ADR for RQAlpha integration)  
> **Related**: v1.1 Event State Machine spec, blinkquant v1.0.3 baseline  

---

## Context

**blinkquant v1.0.3** 已完成：
- PIT / as-of / D-W-M 语义冻结
- SelectionEngine + Blink 解析
- SignalTrace / Checkpoint / Metrics
- CorporateAction MVP
- T+1 / PIT / no-lookahead / Determinism
- 314/314 tests passing

**待解决核心问题**：v1.1 Event State Machine 的 `execution-before-signal` + checkpoint boundary 重设计。

---

## Decision

**RQAlpha 6.3.0 仅作为 Execution Kernel 接入，不作为 Backtest Engine。**

### 架构边界

```
┌────────────────────────────────────────────────────────────────────┐
│                        blinkquant (orchestrator)                   │
├────────────────────────────────────────────────────────────────────┤
│  PIT / as-of / D-W-M / Formula / AI Selection                      │
│  HF Parquet / Polars / SelectionResult / SignalTrace               │
│  TradingCalendar / signal_date / execution_date / valuation_date   │
│  Checkpoint / Resume / SignalTrace / Metrics                       │
└──────────────────────────────┬─────────────────────────────────────┘
                               │ SelectionResult
                               ▼
┌────────────────────────────────────────────────────────────────────┐
│                        RQAlpha Adapter                             │
│  语义转换：SelectionResult → RQAlpha Order                         │
│  数据转换：blinkquant Parquet → RQAlpha DataSource                 │
│  结果归一化：RQAlpha Trade/Fill → blinkquant Fill/Trade            │
└──────────────────────────────┬─────────────────────────────────────┘
                               │
                               ▼
┌────────────────────────────────────────────────────────────────────┐
│                    RQAlpha Execution Kernel                        │
│  Order / Broker / Matcher / T+1 / Portfolio / Cash                 │
│  Transaction Cost / Corporate Action / Partial Fill                │
│  Price Limit / Volume Limit / Suspension / ST                      │
└────────────────────────────────────────────────────────────────────┘
```

### 保留在 blinkquant（不外包）

| 模块 | 理由 |
|---|---|
| PIT / as-of / D-W-M | 核心差异化能力 |
| Blink Formula / AI Selection | 核心产品能力 |
| TradingCalendar + 时间语义边界 | 契约核心，不能外包给 RQAlpha 事件循环 |
| SelectionResult | Selection 与 Execution 的唯一交接契约 |
| Checkpoint / Resume 编排 | 状态机主控权 |
| SignalTrace / Metrics | 审计/评价体系 |

### 外包给 RQAlpha

| 模块 | RQAlpha 对应模块 | 匹配度 |
|---|---|---|
| Order / Broker / Matcher | `sys_simulation` | ★★★★★ |
| T+1 | `StockPosition._non_closable` | ★★★★★ |
| Portfolio / Cash | `sys_accounts` | ★★★★★ |
| Transaction Cost | `sys_transaction_cost` | ★★★★★ |
| Corporate Action | `StockPosition.before_trading()` | ★★★★★ (优于 MVP) |
| Partial Fill | 6.3.0 新增 | ★★★★★ |
| Price/Volume Limit | `sys_simulation` | ★★★★★ |
| Suspension / ST | DataSource / Instrument | ★★★★☆ |

---

## Critical Technical Risks (Must Verify in PoC-0)

### P0: `T signal → T+1 open execution → T+1 close valuation`

**RQAlpha 默认日频语义：**
- `current_bar` = 当前 bar 收盘价
- 官方声明：日频不再支持 "next bar" 撮合
- **风险**：直接用 RQAlpha daily simulation 会导致 `T signal → T execution`

**解决方案必须验证：**
```
blinkquant: signal_date=T → execution_date=T+1
RQAlpha: 接收 intent → 在 T+1 开盘价成交 → T+1 收盘估值
```
**验证标准**：逐笔 diff `signal_date / execution_date / fill_price / fill_qty`

---

## PoC Phases (Strict Scope)

### PoC-0: Minimal Timing Verification (Week 1)

**Scope**: 单只股票、连续 5-10 个交易日、每天 1 个 signal

**目标**：证明 `T signal → T+1 open execution → T+1 close valuation` 语义成立

**测试用例**：
```python
# T1: BUY 100 @ T+1 open 10.00
# T2 close: equity = cash + 100 * close
# T2: SELL 100 @ T3 open 11.00
```

**验证字段**：`signal_date, execution_date, code, side, fill_qty, fill_price, fee, cash_after, position_after, equity`

**不包含**：Weekly / 多股票 / 真实 HF 数据 / Corporate Action / Fee 对账

**通过标准**：逐笔 diff `execution_date == signal_date + 1 trading day` 且 `fill_price == open`

---

### PoC-1: T+1 Invariant (Week 2)

**测试矩阵**：

| Test | Setup | Expected |
|---|---|---|
| A | T BUY 100, T+1 SELL 100 | SELL rejected (T+1 frozen) |
| B | T BUY 100, T+1 BUY 100, T+1 SELL 100 | 只能卖 T 日仓位 |
| C | T old_position=100, T+1 SELL 100 | 正常成交 |

---

### PoC-2: Fee & Cost Mapping (Week 2-3)

| Test | Verify |
|---|---|
| Small notional | commission == minimum (5.0) |
| Large notional | commission == notional × rate |
| SELL only | stamp_tax > 0, BUY == 0 |
| Transfer fee | included in total fee |
| Fill object mapping | RQAlpha Trade → blinkquant Fill 逐字段对账 |

---

### PoC-3: Corporate Action (Week 3)

| Test | Verify |
|---|---|
| Split 1:2 | position qty ×2, avg_cost /2, pending order qty ×2, price /2 |
| Bonus 10送3 | position qty ×1.3, avg_cost /1.3 |
| Cash dividend | cash ↑, avg_cost ↓, dividend tax by holding period |

**对账基准**：blinkquant v1.0.3 CorporateActionStore 结果

---

### PoC-4: Real Data Smoke (Week 4)

**配置**：
- 2024 Q1, node0 (~1778 stocks)
- N=20, weekly, cash=1M, `CLOSE > MA20`
- HF Parquet data via adapter

**验证**：逐笔 diff 30+ 字段，时间语义优先于收益一致性

---

## License Risk (Recorded)

**RQAlpha 不是纯 Apache 2.0**：
- 非商业用途 → Apache 2.0
- 商业用途 → 需米筐授权
- 定义宽泛：个人商业目的、法人使用均需授权

**当前状态**：blinkquant 个人/非商业/研究 → **符合非商业许可**

**ADR 记录**：若未来商业化/SaaS，必须重新评估或替换 Execution Kernel。

---

## Experiment Branch Structure

```
experiment/rqalpha-backend/
├── backends/
│   └── rqalpha/
│       ├── adapter.py          # SelectionResult → RQAlpha orders
│       ├── datasource.py       # BlinkquantParquetDataSource
│       ├── mapper.py           # Trade/Fill ↔ Fill/Trade
│       ├── result.py           # Normalized BacktestResult
│       └── config.py           # RQAlpha config builder
├── tests/
│   └── rqalpha/
│       ├── test_t1_timing.py           # PoC-0
│       ├── test_t1_invariant.py        # PoC-1
│       ├── test_fee_mapping.py         # PoC-2
│       ├── test_corporate_action.py    # PoC-3
│       ├── test_result_mapping.py      # PoC-4
│       └── conftest.py
├── requirements-rqalpha.txt
└── README.md
```

**不修改**：`main` 分支、blinkquant 核心代码

---

## Success Criteria (Go/No-Go)

| Phase | Go Criteria | No-Go (Drop RQAlpha) |
|---|---|---|
| PoC-0 | `execution_date == signal_date + 1` 100% | 任何 T signal → T execution |
| PoC-1 | T+1 freeze 100% 正确 | T+1 frozen 可卖 |
| PoC-2 | Fee 逐字段 ≤ 0.01 差异 | 任一 fee 字段不匹配 |
| PoC-3 | CA 状态 100% 对齐 blinkquant MVP | 任一 CA 字段不匹配 |
| PoC-4 | 真实数据 30+ 字段逐笔 diff | 任一时间/订单语义不匹配 |

---

## License ADR Addendum

**ADR-001-LICENSE**: 若 blinkquant 任意商业化路径确认，必须：
1. 联系米筐申请商业授权，或
2. 完成 Execution Kernel 自研替代（预计 4-6 周）

当前决策：**继续 PoC，风险已知并记录**。

---

## Next Actions

1. ✅ 创建 `experiment/rqalpha-backend` branch
2. ✅ `pip install rqalpha==6.3.0` + 依赖
3. ✅ 实现 `BlinkquantRQDataSource` (最小可用)
4. ✅ PoC-0: 单股票 5 日 timing verification
5. 决策点：PoC-0 通过 → 继续 PoC-1/2/3；失败 → 记录原因，评估自研成本

---

**Decision Recorded By**: blinkquant Architecture Review  
**Next Review**: After PoC-0 completion (estimated 1 week)
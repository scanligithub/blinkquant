# BlinkQuant BacktestResult Contract v1.0

## Purpose

定义 BacktestEngine 输出的规范数据契约。**Artifact 是数据契约，不是 Python 类的内部实现。** API/前端/报告层依赖此契约，引擎重构不应破坏此接口。

**设计原则：**

- 所有字段类型明确，null 规则明确
- 排序规则确定性
- 日期语义、价格语义、数量语义、费用语义全部冻结
- schema_version 用于前向兼容

---

## Artifact 总览

```
BacktestResult
├── equity_curve        # 每日权益曲线 (parquet)
├── trades              # 全部成交记录 (parquet)
├── positions_daily     # 每日持仓快照 (parquet)
├── metrics             # 聚合指标 (json)
├── execution_diagnostics  # 执行诊断 (json, 可选)
└── signal_traces       # 选股因果链 (json, 可选)
```

---

## 1. equity_curve

| 字段 | 类型 | Null | 说明 |
|------|------|------|------|
| date | Date | N | 交易日（执行日） |
| equity | Float64 | N | 总权益 = cash + positions_value |
| cash | Float64 | N | 可用现金 |
| positions_value | Float64 | N | 持仓市值（按 raw close 估值） |
| signal_date | Date | Y | 产生信号的日期（无信号日为 null） |

**排序：** `date` 升序

**日期语义：** `date` 是执行日（T+1），不是信号日。`signal_date` 是信号产生日。

**价格语义：** `positions_value` 按 **raw close**（不前复权）估值。

**null 规则：** `signal_date` 在无信号日（如非调仓日）为 null。其他字段非 null。

---

## 2. trades

| 字段 | 类型 | Null | 说明 |
|------|------|------|------|
| signal_date | Date | N | 信号产生日 |
| execution_date | Date | N | 执行日（= next_trade_day(signal_date)） |
| code | Utf8 | N | 股票代码（sh.600000 格式） |
| side | Utf8 | N | "BUY" 或 "SELL" |
| qty | Int64 | N | 成交数量（股，A 股 100 股整数倍） |
| price | Float64 | N | 成交价格（raw open，前复权信号 → raw 执行） |
| fee | Float64 | N | 总费用（佣金 + 过户费 + 印花税） |

**排序：** `(execution_date, code, side)` 升序

**数量语义：** A 股最小单位 100 股。BUY 必须 100 股整数倍；SELL 允许碎股（清仓时）。

**费用语义：**

| 费用项 | 计算基数 | 说明 |
|--------|----------|------|
| 佣金 | 成交金额 × commission_rate | 最低佣金 >= commission_min |
| 过户费 | 成交金额 × transfer_fee_rate | 仅沪市 |
| 印花税 | 成交金额 × stamp_tax_rate | 仅 SELL |

费率按 `execution_date` 从 FeeSchedule 查询（历史费率变动）。

---

## 3. positions_daily

| 字段 | 类型 | Null | 说明 |
|------|------|------|------|
| date | Date | N | 交易日 |
| code | Utf8 | N | 股票代码 |
| qty | Int64 | N | 持仓数量（含冻结） |
| cost | Float64 | N | 平均成本 |
| market_value | Float64 | N | 市值（qty × raw close） |

**排序：** `(date, code)` 升序

**null 规则：** 仅在该日持有该股时出现。空仓不出现在快照中。

---

## 4. metrics

JSON 对象，聚合指标：

| 字段 | 类型 | 说明 |
|------|------|------|
| total_return | Float64 | 总收益率 |
| annualized_return | Float64 | 年化收益率 |
| max_drawdown | Float64 | 最大回撤 |
| sharpe_ratio | Float64 | 夏普比率（无风险利率=0） |
| win_rate | Float64 | 胜率 |
| profit_factor | Float64 | 盈亏比 |
| total_trades | Int64 | 总成交笔数 |
| turnover_rate | Float64 | 换手率 |

---

## 5. execution_diagnostics（可选）

JSON 对象，执行层诊断：

| 字段 | 类型 | 说明 |
|------|------|------|
| intents_total | Int64 | 意图总数 |
| partial_fill_count | Int64 | 部分成交笔数 |
| carried_events | Int64 | 停牌 carry-forward 次数 |
| zero_price_trade_count | Int64 | 零价成交笔数 |
| t1_violation_count | Int64 | T+1 违规笔数 |
| negative_cash_count | Int64 | 负现金次数 |
| rej_counters | Dict[str, Int64] | 拒单分类计数 |

---

## 6. signal_traces（可选）

`Dict[signal_date_str, SignalTraceData]`，每个信号日的选股因果链。详见 `signal-trace-v1.md`。

---

## Deterministic Ordering

所有 DataFrame 输出的排序规则是**契约的一部分**。引擎实现必须保证：

- `equity_curve`：按 `date` 升序
- `trades`：按 `(execution_date, code, side)` 升序
- `positions_daily`：按 `(date, code)` 升序

排序不确定性（如 set/dict 遍历顺序）是引擎 bug。

---

## Frozen Semantics

以下语义在 v1.0 生命周期内不可变更：

1. **T+1 执行**：signal_date=T → execution_date=next_trade_day(T)
2. **raw 估值**：positions_value 按 raw close（不前复权）
3. **信号→执行不可逆**：信号产生后必须执行（除非被拒单）
4. **费用时变**：按 execution_date 查询历史费率
5. **确定性排序**：同日同股同方向的成交顺序确定

---

## Version History

| 版本 | 日期 | 变更 |
|------|------|------|
| v1.0.0 | 2026-08-31 | 初始冻结 |

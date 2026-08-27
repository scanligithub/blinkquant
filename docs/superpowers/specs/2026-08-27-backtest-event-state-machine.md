# Backtest Engine v1.1 — Event State Machine & Checkpoint Boundary Specification

> **Document Status**: Draft for Contract Review  
> **Target**: v1.1 Architecture Decision  
> **Depends on**: v1.0.3 stable baseline (527fe9c)

---

## 1. 核心设计原则

| 原则 | 说明 |
|---|---|
| **单向事件流** | 每个 trading day 内事件严格单向，不可逆、不可重入 |
| **事务边界 checkpoint** | checkpoint 只能落在“事务完成”的 boundary，不能落在事件执行中间 |
| **恢复即续跑** | restore 后从“最后一个已完成 event 的下一个”继续，不重复、不遗漏 |
| **游标显式化** | 所有进度用显式游标表示，不依赖隐式时间推断 |

---

## 2. Trading Day 事件顺序（单向流）

```
┌─────────────────────────────────────────────────────────────────┐
│                     TRADING DAY T                               │
├─────────────────────────────────────────────────────────────────┤
│ PRE_OPEN (开盘前)                                               │
│   ├── 1. CORPORATE_ACTION    ← 除权除息/送股/分红，现金+持仓调整 │
│   ├── 2. THAW                ← T+1 冻结解冻，available += frozen │
│   └── 3. EXECUTE_PENDING     ← 执行 T-1 signal 生成的待执行单   │
│                                                                  │
│ POST_EXECUTION (执行后持仓确定)                                 │
│   └── Portfolio state boundary（真实 T 开盘后持仓）             │
│                                                                  │
│ SIGNAL (收盘前信号生成)                                          │
│   ├── signal_date = T                                           │
│   ├── SELECTION                 ← 选股公式评估                   │
│   ├── TARGET_PORTFOLIO          ← allocator → 目标权重           │
│   └── CREATE_PENDING            ← 生成 T→T+1 的待执行意图       │
│                                                                  │
│ VALUATION (收盘估值)                                             │
│   └── T close raw valuation   ← equity curve 记录               │
│                                                                  │
│ CHECKPOINT (可保存点)                                            │
│   └── 状态序列化（仅允许在 VALUATION 完成后）                   │
└─────────────────────────────────────────────────────────────────┘
```

**关键契约：**

| Phase | 完成后状态 | 可否 checkpoint |
|---|---|---|
| PRE_OPEN | T-1 订单已执行，T-1 frozen 已解冻 | 否（仍有未完成事件） |
| POST_EXECUTION | Portfolio 反映真实 T 开盘后持仓 | 否（仍有未完成事件） |
| SIGNAL | T 信号已生成，pending 订单已创建 | 否（估值未完成） |
| VALUATION | equity/positions 已记录 | **是（day 事务完整完成）** |

---

## 3. 游标定义（替代现有隐式字段）

### 3.1 核心游标

```python
@dataclass
class BacktestCursors:
    """显式进度游标，替代 selected_thru / thru_thaw / pending"""
    
    # 最后完成的 valuation 日期（含该日）
    # None = 尚未开始
    valuation_through: Optional[date] = None
    
    # 最后完成的 signal 日期（含该日）
    # None = 尚未产生任何信号
    signal_through: Optional[date] = None
    
    # 最后完成的 execution 日期（含该日）
    # None = 尚未执行任何订单
    execution_through: Optional[date] = None
    
    # 当前在途订单（已生成、待执行）
    # signal_date → execution_date 的映射
    pending_orders: Dict[date, PendingOrderBundle] = field(default_factory=dict)
    
    # 最后一次 thaw 完成的日期
    # None = 尚未 thaw
    thaw_through: Optional[date] = None
```

### 3.2 与现有字段映射

| 现有字段 | 新游标 | 说明 |
|---|---|---|
| `selected_thru` | `signal_through` | 重命名，语义明确 |
| `thru_thaw` | `thaw_through` | 重命名，语义明确 |
| `pending` (signal_date + execution_date + intents) | `pending_orders[signal_date]` | 结构化，支持多信号日并存 |
| `_last_close` | 内部缓存，不序列化 | 估值用，checkpoint 不存 |

---

## 4. Checkpoint Boundary 定义

### 4.1 合法 Checkpoint 点

**唯一合法点：VALUATION 完成后**

```text
Day T 完整结束 → VALUATION 完成 → CHECKPOINT
```

**非法点（禁止）：**
- PRE_OPEN 中间（CorporateAction/Thaw/Execute 任一未完成）
- POST_EXECUTION（Portfolio 已变但 Signal/Valuation 未做）
- SIGNAL 完成但 VALUATION 未做

### 4.2 Checkpoint 结构

```python
@dataclass
class BacktestCheckpoint:
    """可序列化的 checkpoint，仅包含到 valuation_through 为止的完整状态"""
    
    # 游标（必填）
    cursors: BacktestCursors
    
    # Portfolio 完整状态
    portfolio: PortfolioState
    
    # 估值缓存（可选，用于 carry-forward）
    # 注意：不存 _last_close，恢复时从 raw data 重建
    last_close_cache: Optional[Dict[str, float]] = None
    
    # 元数据
    metadata: CheckpointMetadata = field(default_factory=CheckpointMetadata)
```

### 4.3 Restore 语义

```python
def restore_from_checkpoint(checkpoint: BacktestCheckpoint, 
                            calendar: TradingCalendar,
                            start_date: date) -> BacktestEngine:
    """
    恢复语义：从“最后一个已完成 valuation 的下一个 trading day”继续。
    
    步骤：
    1. 验证 checkpoint.cursors.valuation_through >= start_date - 1
    2. 恢复 Portfolio
    3. 恢复 pending_orders（这些订单的 execution_date > valuation_through）
    4. 恢复游标
    5. 计算恢复起始日：next_trade_day(valuation_through)
    6. 从该日开始 run()
    """
```

---

## 5. 状态机转换表

### 5.1 单日事件流转

| Current State | Event | Next State | 条件 |
|---|---|---|---|
| IDLE | run() 调用 | PRE_OPEN | 第一天 |
| PRE_OPEN | CorporateAction 完成 | PRE_OPEN |  |
| PRE_OPEN | Thaw 完成 | PRE_OPEN |  |
| PRE_OPEN | ExecutePending 完成 | POST_EXECUTION |  |
| POST_EXECUTION | Signal 完成 | SIGNAL | T ∈ allowed_signals |
| POST_EXECUTION | (无信号日) | VALUATION | T ∉ allowed_signals |
| SIGNAL | CreatePending 完成 | VALUATION |  |
| VALUATION | Valuation 完成 | CHECKPOINT |  |
| CHECKPOINT | 序列化完成 | IDLE (下一天) |  |

### 5.2 跨日游标推进

| Event | valuation_through | signal_through | execution_through | thaw_through | pending_orders |
|---|---|---|---|---|---|
| CorporateAction | 不变 | 不变 | 不变 | 不变 | 不变 |
| Thaw | 不变 | 不变 | 不变 | = T | 不变 |
| ExecutePending(T-1) | 不变 | 不变 | = T-1 | 不变 | 删除 T-1 |
| Signal(T) | 不变 | = T | 不变 | 不变 | 新增 T→T+1 |
| Valuation(T) | = T | 不变 | 不变 | 不变 | 不变 |

---

## 6. 关键场景处理

### 6.1 连续跑（无 checkpoint）

```python
def run(self, start_date, end_signal_date, ...):
    # 1. 初始化游标
    self.cursors = BacktestCursors()
    
    # 2. 计算所有 trading day
    all_days = calendar.trade_range(start_date, end_signal_date)
    
    # 3. 单日循环
    for T in all_days:
        self._process_trading_day(T)
    
    # 4. 最后一日的 valuation 已在循环内完成
    return self._build_result()
```

### 6.2 Checkpoint 续跑

```python
def run(self, start_date, end_signal_date, ..., initial_state=None):
    if initial_state:
        # 1. 从 checkpoint 恢复
        self.cursors = initial_state.cursors
        self.portfolio = initial_state.portfolio
        
        # 2. 计算恢复起始日
        resume_date = self.calendar.next_trade_day(
            self.cursors.valuation_through or start_date)
        
        # 3. 验证 pending 订单的 execution_date >= resume_date
        # （execution_date < resume_date 的订单应已执行完毕，不应存在）
        for sig_date, bundle in self.cursors.pending_orders.items():
            assert bundle.execution_date >= resume_date
        
        # 4. 计算新的 all_days
        all_days = calendar.trade_range(resume_date, end_signal_date)
    else:
        # 无 checkpoint，正常初始化
        ...
    
    # 5. 从 resume_date 开始循环
    for T in all_days:
        self._process_trading_day(T)
```

### 6.3 Weekly Rebalance

```text
Weekly 时：
- allowed_signals 只包含每周最后一个交易日
- signal_through 仍按自然日推进（只在信号日更新）
- execution_through 按实际执行日更新
- valuation_through 每日更新
```

---

## 7. 与现有代码的迁移映射

### 7.1 BacktestEngine 字段变更

| 现有字段 | 新字段 | 迁移 |
|---|---|---|
| `_selected_thru` | `cursors.signal_through` | 重命名 |
| `_thru_thaw` | `cursors.thaw_through` | 重命名 |
| `_pend_sig` + `_pend_exec` + `_pend_intents` + `_pend_prices` | `cursors.pending_orders` | 结构化合并 |
| `_last_close` | 内部缓存，不入 checkpoint | 估值时从 raw data 重建 |

### 7.2 export_state / import_state

```python
def export_state(self) -> BacktestCheckpoint:
    """仅允许在 VALUATION 完成后调用"""
    assert self._current_phase == Phase.VALUATION_COMPLETE
    return BacktestCheckpoint(
        cursors=self.cursors,
        portfolio=self.portfolio.export_state(),
        last_close_cache=dict(self._last_close),
        metadata=CheckpointMetadata(
            created_at=datetime.now(),
            engine_version="1.1.0",
        )
    )

def import_state(self, checkpoint: BacktestCheckpoint):
    """仅允许在 run() 入口调用"""
    self.cursors = checkpoint.cursors
    self.portfolio.import_state(checkpoint.portfolio)
    self._last_close = checkpoint.last_close_cache or {}
```

---

## 8. 验证测试矩阵

| 场景 | 验证点 |
|---|---|
| 单段连续跑 | cursors 推进正确，pending 生成/清理正确 |
| 两段 checkpoint 续跑 | C1 trades == A+B trades，equity/positions 逐日一致 |
| 跨段 pending 订单 | A 段最后信号的执行在 B 段正确执行 |
| Weekly rebalance | 只有周五产生信号，execution 在下周一 |
| CorporateAction + Thaw + Execute 同日 | 顺序正确， Portfolio 状态一致 |
| 空 Calendar | run() 入口 fail-fast |
| initial_state 时间倒退 | run() 入口 fail-fast |

---

## 9. 实施计划

| Phase | 任务 | 预估 |
|---|---|---|
| 1 | 定义 BacktestCursors / BacktestCheckpoint dataclass | 1d |
| 2 | 重构 BacktestEngine.run() 事件循环（execution before signal） | 2d |
| 3 | 重构 checkpoint 序列化/反序列化 | 1d |
| 4 | 更新 checkpoint 测试（test_backtest_continuity.py） | 1d |
| 5 | 全量回归 + 2024/2025 smoke + 跨年 checkpoint | 1d |

**总计：~6 天**

---

## 10. 遗留问题（暂不解决，列入 v1.2+）

| 问题 | 说明 |
|---|---|
| CorporateAction 日期语义细化 | 当前 MVP 用单一 date，v1.2 引入 ex_date/pay_date |
| SignalTrace wiring | v1.1 checkpoint 稳定后接入 |
| Intraday 事件扩展 | 需要更细粒度的 phase 定义 |
| 多账户/融资融券 | 需要 Portfolio 分层 |

---

## Appendix: 现有代码与本规约的差距清单

| 规约项 | 现状 | 工作量 |
|---|---|---|
| execution before signal | ❌ 当前 signal 在 execution 前 | 高 |
| 显式 cursors | ❌ 用 selected_thru/thru_thaw/pending | 中 |
| checkpoint 只在 valuation 后 | ❌ 当前可在任意点 export | 低 |
| restore 从 valuation_through+1 开始 | ❌ 当前从 start_date 重跑 | 中 |
| pending_orders 结构化 | ❌ 当前分散 4 个字段 | 低 |
| Calendar fail-fast | ✅ v1.0.3 已完成 | — |
| initial_state 时间校验 | ✅ v1.0.3 已完成 | — |

---

**文档结束。请审查 P0/P1/P2 + 状态机一致性 + checkpoint 可证明性。**
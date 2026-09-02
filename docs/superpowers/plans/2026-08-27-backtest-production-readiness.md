# Backtest Engine Production Readiness Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修补 blinkquant BacktestEngine 的生产级缺口，使其成为一个可信的 A 股日频回测引擎——覆盖公司行为、交易状态、成本追踪和可审计性。

**Architecture:** 在现有 Phase 0 基础上增量扩展：
- 公司行为层（CorporateActionsStore + Portfolio 调整）
- Universe 构造层（IPO/ST/退市过滤）
- 成本追踪修正（avg_cost 随公司行为调整）
- 可审计性（SignalTrace 完整决策链）
- 风险指标（Sortino/Calmar/Benchmark）

**Tech Stack:** Python 3.10+, Polars, pytest, 同已有架构

---

## Gap Analysis: 当前 MVP 距生产级的差距

### 🔴 Critical（必须修补，否则回测结果不可信）

| # | 缺口 | 影响 | 当前状态 |
|---|---|---|---|
| C1 | **公司行为：分红/送股/转增/配股** | 长期持仓的持股数量、成本、现金均未调整；total_return 是纯价格收益（不含分红） | ❌ 完全缺失 |
| C2 | **公司行为：除权除息** | raw price 除权日跳变未处理；Portfolio 持仓的 market_value 计算可能失真 | ❌ 完全缺失 |
| C3 | **Position avg_cost 调整** | 送股/转增后 avg_cost 应下降；分红后应调整；当前仅 buy 时更新 | ❌ 从未调整 |
| C4 | **IPO 新股过滤** | 新股上市首日/前 N 日不应参与选股（流动性/波动率异常） | ❌ 无此过滤 |

### 🟡 Important（应修补，影响可信度和可用性）

| # | 缺口 | 影响 | 当前状态 |
|---|---|---|---|
| I1 | **ST/*ST 交易限制** | ST 股涨跌停 5%、交易手数限制、费率差异 | ⚠️ limit_up_pct 提及但未暴露 IS_ST 字段 |
| I2 | **退市整理期** | 退市股票应被排除或特殊处理 | ❌ 无此逻辑 |
| I3 | **历史费率** | FeeConfig 为静态默认值；实际佣金/印花税随时间变化 | ⚠️ 文档提及但未实现 |
| I4 | **Benchmark 相对指标** | 无 alpha/beta/信息比率/跟踪误差 | ❌ 仅绝对收益 |
| I5 | **风险指标补全** | 仅 Sharpe，缺 Sortino/Calmar/下行标准差 | ⚠️ 引擎内有 Sharpe 但未入 BacktestMetrics |

### 🟢 Nice-to-have（可明确延期）

| # | 缺口 | 延期理由 |
|---|---|---|
| N1 | 滑点/市场冲击模型 | MVP 用 Open 价已足够；滑点是策略层关心的 |
| N2 | 涨跌停队列模拟 | 仅阻断已足够；队列深度需要 Level-2 数据 |
| N3 | 日内数据（分时/Tick） | 日频回测不需要 |
| N4 | 融资融券/做空 | 当前为纯多头 MVP |
| N5 | 多账户/组合 | 单组合 MVP 已够 |
| N6 | 最小交易金额 | A 股无此限制 |

---

## 实施计划

### Task 1: Corporate Actions 数据模型

**Files:**
- Create: `backend/core/corporate_actions.py`
- Create: `backend/tests/test_corporate_actions.py`

**目标:** 定义公司行为数据结构和 Store 接口。

- [ ] **Step 1: 写测试**

```python
# backend/tests/test_corporate_actions.py
"""Corporate Actions 数据模型测试。"""
import datetime
import polars as pl
from core.corporate_actions import (
    CorporateAction, CorporateActionStore,
    ActionType, adjust_avg_cost_for_split, adjust_qty_for_split,
)


def test_action_type_enum():
    assert ActionType.CASH_DIVIDEND.value == "cash_dividend"
    assert ActionType.STOCK_SPLIT.value == "stock_split"
    assert ActionType.BONUS_SHARES.value == "bonus_shares"
    assert ActionType.RIGHTS_ISSUE.value == "rights_issue"


def test_adjust_qty_for_split_2_to_1():
    """10 送 10（2:1 拆股）：qty × 2, avg_cost / 2"""
    qty, cost = adjust_qty_for_split(total_qty=1000, avg_cost=20.0, split_ratio=2.0)
    assert qty == 2000
    assert abs(cost - 10.0) < 1e-6


def test_adjust_qty_for_bonus_shares():
    """10 送 3（bonus_ratio=0.3）：qty × 1.3, avg_cost / 1.3"""
    qty, cost = adjust_qty_for_split(total_qty=1000, avg_cost=15.0, split_ratio=1.3)
    assert qty == 1300
    assert abs(cost - 15.0 / 1.3) < 1e-6


def test_adjust_avg_cost_for_cash_dividend():
    """现金分红：avg_cost -= dividend_per_share"""
    new_cost = adjust_avg_cost_for_dividend(avg_cost=20.0, dividend_per_share=0.5)
    assert abs(new_cost - 19.5) < 1e-6


def test_adjust_avg_cost_floor_at_zero():
    """分红后 avg_cost 不应为负"""
    new_cost = adjust_avg_cost_for_dividend(avg_cost=0.3, dividend_per_share=0.5)
    assert new_cost == 0.0


def test_store_query_by_code_and_date_range():
    """Store 按 code + 日期范围查询"""
    actions = [
        CorporateAction(
            date=datetime.date(2024, 7, 1), code="000001",
            action_type=ActionType.CASH_DIVIDEND,
            cash_dividend_per_share=0.5,
        ),
        CorporateAction(
            date=datetime.date(2024, 12, 25), code="000001",
            action_type=ActionType.STOCK_SPLIT,
            split_ratio=2.0,
        ),
    ]
    store = CorporateActionStore(actions)
    result = store.query(code="000001",
                         start_date=datetime.date(2024, 1, 1),
                         end_date=datetime.date(2024, 12, 31))
    assert len(result) == 1
    assert result[0].action_type == ActionType.CASH_DIVIDEND


def test_store_empty():
    store = CorporateActionStore([])
    result = store.query(code="000001",
                         start_date=datetime.date(2024, 1, 1),
                         end_date=datetime.date(2024, 12, 31))
    assert len(result) == 0
```

- [ ] **Step 2: 运行测试确认失败**

```bash
cd backend && python -m pytest tests/test_corporate_actions.py -v
```
预期：FAIL（module not found）

- [ ] **Step 3: 实现 corporate_actions.py**

```python
# backend/core/corporate_actions.py
"""公司行为数据模型与 Store。"""
import datetime
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List


class ActionType(Enum):
    CASH_DIVIDEND = "cash_dividend"
    STOCK_SPLIT = "stock_split"
    BONUS_SHARES = "bonus_shares"
    RIGHTS_ISSUE = "rights_issue"


@dataclass
class CorporateAction:
    """单条公司行为记录。"""
    date: datetime.date           # 公告日 / 除权除息日
    code: str
    action_type: ActionType
    # 现金分红
    cash_dividend_per_share: float = 0.0
    # 送股/转增/拆股：每 10 股送 N 股 → split_ratio = (10+N)/10
    split_ratio: float = 1.0
    # 配股
    rights_price: float = 0.0     # 配股价格
    rights_ratio: float = 0.0     # 每 10 股配 N 股 → ratio = (10+N)/10


def adjust_qty_for_split(total_qty: int, avg_cost: float,
                         split_ratio: float) -> tuple[int, float]:
    """送股/转增/拆股后调整持股数量和成本。

    Args:
        total_qty: 调整前总股数
        avg_cost: 调整前每股成本
        split_ratio: 拆股比例（2.0 = 10 送 10）

    Returns:
        (new_qty, new_avg_cost)
    """
    if split_ratio <= 0 or split_ratio == 1.0:
        return total_qty, avg_cost
    new_qty = int(total_qty * split_ratio)
    new_cost = avg_cost / split_ratio if split_ratio > 0 else 0.0
    return new_qty, new_cost


def adjust_avg_cost_for_dividend(avg_cost: float,
                                 dividend_per_share: float) -> float:
    """现金分红后调整每股成本。

    除息日：avg_cost -= dividend_per_share（不低于 0）
    """
    return max(0.0, avg_cost - dividend_per_share)


class CorporateActionStore:
    """公司行为存储：按 code + 日期范围查询。"""

    def __init__(self, actions: Optional[List[CorporateAction]] = None):
        self._actions = sorted(actions or [], key=lambda a: (a.code, a.date))

    def query(self, code: str, start_date: datetime.date,
              end_date: datetime.date) -> List[CorporateAction]:
        """查询指定 code 在 [start_date, end_date] 内的所有公司行为。"""
        result = []
        for a in self._actions:
            if a.code > code:
                break
            if a.code == code and start_date <= a.date <= end_date:
                result.append(a)
        return result

    def query_all(self, start_date: datetime.date,
                  end_date: datetime.date) -> List[CorporateAction]:
        """查询所有 code 在 [start_date, end_date] 内的公司行为。"""
        return [a for a in self._actions
                if start_date <= a.date <= end_date]
```

- [ ] **Step 4: 运行测试确认通过**

```bash
cd backend && python -m pytest tests/test_corporate_actions.py -v
```
预期：全部 PASS

- [ ] **Step 5: 提交**

```bash
git add backend/core/corporate_actions.py backend/tests/test_corporate_actions.py
git commit -m "feat: Corporate Actions 数据模型与 Store"
```

---

### Task 2: Portfolio 公司行为调整

**Files:**
- Modify: `backend/core/portfolio.py`（新增 `apply_corporate_action` 方法）
- Create: `backend/tests/test_portfolio_corporate_actions.py`

**目标:** Portfolio 在收到公司行为时正确调整持股数量、成本和现金。

- [ ] **Step 1: 写测试**

```python
# backend/tests/test_portfolio_corporate_actions.py
"""Portfolio 公司行为调整测试。"""
import datetime
from core.portfolio import Portfolio, Position
from core.corporate_actions import (
    CorporateAction, ActionType, CorporateActionStore,
)


def _make_portfolio():
    """构造测试用 Portfolio：code=000001, 1000 股, 成本 20.0, 现金 50000。"""
    p = Portfolio(initial_cash=50000.0)
    pos = Position(code="000001", total_qty=1000, available_qty=1000,
                   frozen_qty=0, avg_cost=20.0, market_value=0.0)
    p.positions["000001"] = pos
    return p


def test_cash_dividend():
    """现金分红：持股不变，现金增加，avg_cost 下降。"""
    p = _make_portfolio()
    action = CorporateAction(
        date=datetime.date(2024, 7, 1), code="000001",
        action_type=ActionType.CASH_DIVIDEND,
        cash_dividend_per_share=0.5,
    )
    p.apply_corporate_action(action)
    assert p.positions["000001"].total_qty == 1000
    assert abs(p.positions["000001"].avg_cost - 19.5) < 1e-6
    assert abs(p.cash - 50500.0) < 1e-6  # 50000 + 1000 * 0.5


def test_stock_split_2_for_1():
    """10 送 10（2:1 拆股）：qty × 2, avg_cost / 2, 现金不变。"""
    p = _make_portfolio()
    action = CorporateAction(
        date=datetime.date(2024, 12, 25), code="000001",
        action_type=ActionType.STOCK_SPLIT,
        split_ratio=2.0,
    )
    p.apply_corporate_action(action)
    assert p.positions["000001"].total_qty == 2000
    assert abs(p.positions["000001"].avg_cost - 10.0) < 1e-6
    assert abs(p.cash - 50000.0) < 1e-6


def test_bonus_shares_10_for_3():
    """10 送 3（bonus_ratio=1.3）：qty × 1.3, avg_cost / 1.3。"""
    p = _make_portfolio()
    action = CorporateAction(
        date=datetime.date(2024, 9, 15), code="000001",
        action_type=ActionType.BONUS_SHARES,
        split_ratio=1.3,
    )
    p.apply_corporate_action(action)
    assert p.positions["000001"].total_qty == 1300
    assert abs(p.positions["000001"].avg_cost - 20.0 / 1.3) < 1e-6


def test_no_position_ignored():
    """不在持仓中的 code，公司行为应被忽略。"""
    p = _make_portfolio()
    action = CorporateAction(
        date=datetime.date(2024, 7, 1), code="600000",
        action_type=ActionType.CASH_DIVIDEND,
        cash_dividend_per_share=1.0,
    )
    p.apply_corporate_action(action)  # 不应报错
    assert "600000" not in p.positions
    assert abs(p.cash - 50000.0) < 1e-6


def test_multiple_actions_sequential():
    """连续多个公司行为：先分红，再送股。"""
    p = _make_portfolio()
    # 分红 0.5
    p.apply_corporate_action(CorporateAction(
        date=datetime.date(2024, 7, 1), code="000001",
        action_type=ActionType.CASH_DIVIDEND,
        cash_dividend_per_share=0.5,
    ))
    assert p.positions["000001"].total_qty == 1000
    assert abs(p.positions["000001"].avg_cost - 19.5) < 1e-6
    assert abs(p.cash - 50500.0) < 1e-6

    # 10 送 10
    p.apply_corporate_action(CorporateAction(
        date=datetime.date(2024, 12, 25), code="000001",
        action_type=ActionType.STOCK_SPLIT,
        split_ratio=2.0,
    ))
    assert p.positions["000001"].total_qty == 2000
    assert abs(p.positions["000001"].avg_cost - 19.5 / 2.0) < 1e-6
    assert abs(p.cash - 50500.0) < 1e-6
```

- [ ] **Step 2: 运行测试确认失败**

```bash
cd backend && python -m pytest tests/test_portfolio_corporate_actions.py -v
```
预期：FAIL（`apply_corporate_action` 不存在）

- [ ] **Step 3: 在 Portfolio 中实现 `apply_corporate_action`**

在 `backend/core/portfolio.py` 中新增方法：

```python
def apply_corporate_action(self, action: 'CorporateAction'):
    """应用公司行为到持仓。

    - 现金分红：增加现金，调低 avg_cost
    - 送股/转增/拆股：调整持股数量和 avg_cost
    - 配股：暂不实现（raise NotImplementedError）
    """
    from core.corporate_actions import ActionType, adjust_qty_for_split, adjust_avg_cost_for_dividend

    if action.code not in self.positions:
        return  # 不在持仓中，忽略

    pos = self.positions[action.code]

    if action.action_type == ActionType.CASH_DIVIDEND:
        # 现金分红：持股不变，现金增加，avg_cost 下降
        dividend_cash = pos.total_qty * action.cash_dividend_per_share
        self.cash += dividend_cash
        pos.avg_cost = adjust_avg_cost_for_dividend(
            pos.avg_cost, action.cash_dividend_per_share)

    elif action.action_type in (ActionType.STOCK_SPLIT, ActionType.BONUS_SHARES):
        # 送股/转增/拆股：调整数量和成本
        new_qty, new_cost = adjust_qty_for_split(
            pos.total_qty, pos.avg_cost, action.split_ratio)
        pos.total_qty = new_qty
        pos.avg_cost = new_cost
        # T+1 语义：送股当日 available_qty 不变（下一日 thaw 后生效）
        # 但为简化，这里同步更新 available_qty
        pos.available_qty = new_qty

    elif action.action_type == ActionType.RIGHTS_ISSUE:
        raise NotImplementedError("配股暂未实现")
```

- [ ] **Step 4: 运行测试确认通过**

```bash
cd backend && python -m pytest tests/test_portfolio_corporate_actions.py -v
```
预期：全部 PASS

- [ ] **Step 5: 运行全量测试确认无回归**

```bash
cd backend && python -m pytest tests/ -v
```
预期：全部 PASS

- [ ] **Step 6: 提交**

```bash
git add backend/core/portfolio.py backend/tests/test_portfolio_corporate_actions.py
git commit -m "feat: Portfolio 公司行为调整（分红/送股/转增/拆股）"
```

---

### Task 3: BacktestEngine 公司行为集成

**Files:**
- Modify: `backend/core/backtest_engine.py`（run 方法中集成公司行为处理）
- Modify: `backend/tests/test_backtest_corporate_actions.py`（新建）

**目标:** 回测引擎在每个交易日处理当天发生的公司行为。

- [ ] **Step 1: 写测试**

```python
# backend/tests/test_backtest_corporate_actions.py
"""BacktestEngine 公司行为集成测试。"""
import datetime
import polars as pl
from core.portfolio import Portfolio, Position
from core.corporate_actions import CorporateAction, ActionType, CorporateActionStore
from core.backtest_engine import BacktestEngine, TradingCalendar


def test_portfolio_apply_dividend():
    """验证 Portfolio.apply_corporate_action 在分红场景正确。"""
    p = Portfolio(initial_cash=100000.0)
    p.positions["000001"] = Position(
        code="000001", total_qty=1000, available_qty=1000,
        frozen_qty=0, avg_cost=20.0, market_value=20000.0)
    action = CorporateAction(
        date=datetime.date(2024, 7, 1), code="000001",
        action_type=ActionType.CASH_DIVIDEND,
        cash_dividend_per_share=0.5)
    p.apply_corporate_action(action)
    assert p.positions["000001"].total_qty == 1000
    assert abs(p.cash - 100500.0) < 1e-6
    assert abs(p.positions["000001"].avg_cost - 19.5) < 1e-6


def test_store_query():
    """验证 CorporateActionStore 按日期范围查询。"""
    store = CorporateActionStore([
        CorporateAction(date=datetime.date(2024, 7, 1), code="000001",
                        action_type=ActionType.CASH_DIVIDEND,
                        cash_dividend_per_share=0.5),
        CorporateAction(date=datetime.date(2024, 12, 25), code="000001",
                        action_type=ActionType.STOCK_SPLIT,
                        split_ratio=2.0),
    ])
    q1 = store.query("000001", datetime.date(2024, 1, 1), datetime.date(2024, 7, 31))
    assert len(q1) == 1
    q2 = store.query("000001", datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))
    assert len(q2) == 2
```

- [ ] **Step 2: 运行测试确认通过**（使用已实现的 Portfolio 方法）

```bash
cd backend && python -m pytest tests/test_backtest_corporate_actions.py -v
```
预期：PASS

- [ ] **Step 3: 在 BacktestEngine.run 中集成公司行为**

在 `backtest_engine.py` 的 `run` 方法中，每个交易日 `t` 的处理循环里，在估值之前插入公司行为处理：

```python
# 在 daily_thaw 之后、估值之前
if corporate_action_store is not None:
    today_actions = corporate_action_store.query_all(t, t)
    for action in today_actions:
        self.portfolio.apply_corporate_action(action)
```

完整 run 方法签名增加参数：

```python
def run(
    self,
    formula: str,
    start_date: datetime.date,
    end_signal_date: datetime.date,
    initial_cash: float = 1_000_000,
    initial_positions: dict[str, Position] = None,
    initial_state: dict = None,
    rebalance_freq: str = "daily",
    ranking_fn=None,
    top_n: int = 20,
    corporate_action_store: 'CorporateActionStore' = None,  # 新增
) -> 'BacktestResult':
```

- [ ] **Step 4: 运行全量测试确认无回归**

```bash
cd backend && python -m pytest tests/ -v
```
预期：全部 PASS

- [ ] **Step 5: 提交**

```bash
git add backend/core/backtest_engine.py backend/tests/test_backtest_corporate_actions.py
git commit -m "feat: BacktestEngine 集成公司行为处理"
```

---

### Task 4: Universe 构造——IPO/ST 过滤

**Files:**
- Create: `backend/core/universe.py`
- Create: `backend/tests/test_universe.py`

**目标:** 提供 Universe 过滤器，在选股前排除 IPO 新股和 ST 股票。

- [ ] **Step 1: 写测试**

```python
# backend/tests/test_universe.py
"""Universe 构造过滤器测试。"""
import datetime
import polars as pl
from core.universe import UniverseFilter


def test_filter_ipo_new_stocks():
    """排除上市不足 N 日的新股。"""
    uf = UniverseFilter(min_listing_days=60)
    dates = pl.DataFrame({
        "date": [datetime.date(2024, 1, 1)] * 3,
        "code": ["000001", "300001", "600000"],
        "listing_date": [
            datetime.date(2023, 10, 1),  # 已上市 >60 天 → 通过
            datetime.date(2023, 12, 1),  # 已上市 31 天 → 排除
            datetime.date(2023, 6, 1),   # 已上市 >60 天 → 通过
        ],
    })
    result = uf.filter(dates, target_date=datetime.date(2024, 1, 1))
    assert set(result) == {"000001", "600000"}


def test_filter_st_stocks():
    """排除 ST/*ST 股票。"""
    uf = UniverseFilter(exclude_st=True)
    dates = pl.DataFrame({
        "date": [datetime.date(2024, 1, 1)] * 3,
        "code": ["000001", "000002", "000003"],
        "is_st": [False, True, False],
    })
    result = uf.filter(dates, target_date=datetime.date(2024, 1, 1))
    assert set(result) == {"000001", "000003"}


def test_filter_combined():
    """组合过滤：同时排除 IPO 和 ST。"""
    uf = UniverseFilter(min_listing_days=60, exclude_st=True)
    dates = pl.DataFrame({
        "date": [datetime.date(2024, 1, 1)] * 4,
        "code": ["000001", "000002", "000003", "000004"],
        "listing_date": [
            datetime.date(2023, 6, 1),   # 通过
            datetime.date(2023, 12, 1),  # IPO → 排除
            datetime.date(2023, 1, 1),   # 通过
            datetime.date(2023, 6, 1),   # 通过
        ],
        "is_st": [False, False, True, False],  # ST → 排除
    })
    result = uf.filter(dates, target_date=datetime.date(2024, 1, 1))
    assert set(result) == {"000001", "000004"}


def test_filter_no_listing_date_column():
    """无 listing_date 列时，跳过 IPO 过滤。"""
    uf = UniverseFilter(min_listing_days=60)
    dates = pl.DataFrame({
        "date": [datetime.date(2024, 1, 1)] * 2,
        "code": ["000001", "000002"],
    })
    result = uf.filter(dates, target_date=datetime.date(2024, 1, 1))
    assert set(result) == {"000001", "000002"}


def test_filter_disabled():
    """禁用所有过滤时，返回全部 codes。"""
    uf = UniverseFilter(min_listing_days=0, exclude_st=False)
    dates = pl.DataFrame({
        "date": [datetime.date(2024, 1, 1)] * 2,
        "code": ["000001", "000002"],
        "listing_date": [
            datetime.date(2023, 12, 30),
            datetime.date(2023, 12, 31),
        ],
        "is_st": [True, True],
    })
    result = uf.filter(dates, target_date=datetime.date(2024, 1, 1))
    assert set(result) == {"000001", "000002"}
```

- [ ] **Step 2: 运行测试确认失败**

```bash
cd backend && python -m pytest tests/test_universe.py -v
```
预期：FAIL（module not found）

- [ ] **Step 3: 实现 universe.py**

```python
# backend/core/universe.py
"""Universe 构造：选股前的预过滤。"""
import datetime
from dataclasses import dataclass
from typing import Optional
import polars as pl


@dataclass
class UniverseFilter:
    """Universe 过滤器。

    Attributes:
        min_listing_days: 最小上市天数（排除 IPO 新股）。0 = 不过滤。
        exclude_st: 是否排除 ST/*ST 股票。
    """
    min_listing_days: int = 60
    exclude_st: bool = True

    def filter(self, df: pl.DataFrame, target_date: datetime.date) -> list[str]:
        """从 df 中筛选 target_date 的 eligible codes。

        Args:
            df: 包含 'date', 'code' 列，可选 'listing_date', 'is_st' 列
            target_date: 选股目标日期

        Returns:
            过滤后的 code 列表
        """
        day_df = df.filter(pl.col("date") == target_date)
        if day_df.is_empty():
            return []

        codes = day_df["code"].to_list()

        # IPO 过滤
        if self.min_listing_days > 0 and "listing_date" in day_df.columns:
            cutoff = target_date - datetime.timedelta(days=self.min_listing_days)
            day_df = day_df.filter(
                pl.col("listing_date") <= cutoff
            )

        # ST 过滤
        if self.exclude_st and "is_st" in day_df.columns:
            day_df = day_df.filter(~pl.col("is_st"))

        return day_df["code"].to_list()
```

- [ ] **Step 4: 运行测试确认通过**

```bash
cd backend && python -m pytest tests/test_universe.py -v
```
预期：全部 PASS

- [ ] **Step 5: 提交**

```bash
git add backend/core/universe.py backend/tests/test_universe.py
git commit -m "feat: Universe 构造过滤器（IPO 新股 / ST 排除）"
```

---

### Task 5: 风险指标补全（Sortino/Calmar/Benchmark）

**Files:**
- Modify: `backend/core/metrics.py`（BacktestMetrics 新增字段）
- Modify: `backend/tests/test_metrics.py`

**目标:** BacktestMetrics 包含 Sortino、Calmar、Benchmark 相对指标。

- [ ] **Step 1: 写测试**

```python
# 在 backend/tests/test_metrics.py 中新增
def test_sortino_ratio():
    """Sortino ratio = (return - risk_free) / downside_deviation"""
    from core.metrics import _sortino_ratio
    # 正收益序列
    returns = [0.01, 0.02, -0.005, 0.015, -0.01, 0.008]
    sr = _sortino_ratio(returns, risk_free=0.0)
    assert sr > 0  # 正收益 → 正 Sortino


def test_calmar_ratio():
    """Calmar ratio = annualized_return / max_drawdown"""
    from core.metrics import _calmar_ratio
    calmar = _calmar_ratio(annualized_return=0.15, max_drawdown=0.20)
    assert abs(calmar - 0.75) < 1e-6


def test_benchmark_relative():
    """Benchmark 相对指标：alpha, beta, tracking_error, information_ratio"""
    from core.metrics import compute_benchmark_metrics
    port_returns = [0.01, 0.02, -0.005, 0.015, -0.01]
    bench_returns = [0.005, 0.01, -0.002, 0.008, -0.005]
    result = compute_benchmark_metrics(port_returns, bench_returns)
    assert "alpha" in result
    assert "beta" in result
    assert "tracking_error" in result
    assert "information_ratio" in result
```

- [ ] **Step 2: 运行测试确认失败**

```bash
cd backend && python -m pytest tests/test_metrics.py::test_sortino_ratio -v
```
预期：FAIL（function not defined）

- [ ] **Step 3: 在 metrics.py 中实现新指标**

在 `backend/core/metrics.py` 中新增：

```python
def _sortino_ratio(returns: list[float], risk_free: float = 0.0,
                   periods_per_year: int = 252) -> float:
    """Sortino ratio：仅惩罚下行波动。"""
    if not returns or len(returns) < 2:
        return 0.0
    excess = [r - risk_free for r in returns]
    mean_excess = sum(excess) / len(excess)
    downside = [min(0, r) ** 2 for r in excess]
    downside_dev = (sum(downside) / len(downside)) ** 0.5
    if downside_dev == 0:
        return 0.0
    return mean_excess / downside_dev * (periods_per_year ** 0.5)


def _calmar_ratio(annualized_return: float, max_drawdown: float) -> float:
    """Calmar ratio = annualized_return / |max_drawdown|"""
    if abs(max_drawdown) < 1e-10:
        return 0.0
    return annualized_return / abs(max_drawdown)


def compute_benchmark_metrics(portfolio_returns: list[float],
                               benchmark_returns: list[float]) -> dict:
    """计算 Benchmark 相对指标：alpha, beta, tracking_error, information_ratio。"""
    import numpy as np
    if not portfolio_returns or not benchmark_returns:
        return {"alpha": 0, "beta": 0, "tracking_error": 0, "information_ratio": 0}
    n = min(len(portfolio_returns), len(benchmark_returns))
    p = np.array(portfolio_returns[:n])
    b = np.array(benchmark_returns[:n])
    # Beta = Cov(p,b) / Var(b)
    cov_pb = np.cov(p, b)[0][1]
    var_b = np.var(b, ddof=1)
    beta = cov_pb / var_b if var_b > 0 else 0.0
    # Alpha (Jensen's) = mean(p) - beta * mean(b)，年化
    alpha_daily = np.mean(p) - beta * np.mean(b)
    alpha = alpha_daily * 252
    # Tracking error
    active = p - b
    tracking_error = float(np.std(active, ddof=1)) * (252 ** 0.5)
    # Information ratio
    ir = (alpha / tracking_error) if tracking_error > 0 else 0.0
    return {
        "alpha": float(alpha),
        "beta": float(beta),
        "tracking_error": float(tracking_error),
        "information_ratio": float(ir),
    }
```

在 `BacktestMetrics` dataclass 中新增字段：

```python
@dataclass
class BacktestMetrics:
    # ... 已有字段 ...
    sortino_ratio: float = 0.0
    calmar_ratio: float = 0.0
    benchmark_alpha: float = 0.0
    benchmark_beta: float = 0.0
    benchmark_tracking_error: float = 0.0
    benchmark_information_ratio: float = 0.0
```

在 `compute_metrics` 函数中填充这些字段。

- [ ] **Step 4: 运行测试确认通过**

```bash
cd backend && python -m pytest tests/test_metrics.py -v
```
预期：全部 PASS

- [ ] **Step 5: 运行全量测试确认无回归**

```bash
cd backend && python -m pytest tests/ -v
```
预期：全部 PASS

- [ ] **Step 6: 提交**

```bash
git add backend/core/metrics.py backend/tests/test_metrics.py
git commit -m "feat: 风险指标补全（Sortino/Calmar/Benchmark 相对指标）"
```

---

### Task 6: SignalTrace 可审计性

**Files:**
- Create: `backend/core/signal_trace.py`
- Create: `backend/tests/test_signal_trace.py`

**目标:** 记录每个交易决策的完整决策链：signal_date → eligible → ranking → target_weight → order → fill → position。

- [ ] **Step 1: 写测试**

```python
# backend/tests/test_signal_trace.py
"""SignalTrace 可审计性测试。"""
import datetime
from core.signal_trace import SignalTrace, TraceRecord


def test_trace_record_creation():
    """创建 TraceRecord 并验证字段。"""
    rec = TraceRecord(
        signal_date=datetime.date(2024, 7, 1),
        execution_date=datetime.date(2024, 7, 2),
        code="000001",
        formula="CLOSE > MA(CLOSE, 20)",
        eligible_count=150,
        ranking_score=1.23,
        ranking_position=5,
        target_weight=0.05,
        side="BUY",
        target_qty=500,
        fill_qty=500,
        fill_price=20.0,
        fee=5.0,
        post_qty=500,
        post_cost=20.0,
        post_cash=90000.0,
    )
    assert rec.signal_date == datetime.date(2024, 7, 1)
    assert rec.fill_qty == 500


def test_signal_trace_store_and_query():
    """SignalTrace 存储和查询。"""
    trace = SignalTrace()
    trace.record(TraceRecord(
        signal_date=datetime.date(2024, 7, 1),
        execution_date=datetime.date(2024, 7, 2),
        code="000001", formula="CLOSE > MA(CLOSE, 20)",
        eligible_count=150, ranking_score=1.23, ranking_position=5,
        target_weight=0.05, side="BUY", target_qty=500,
        fill_qty=500, fill_price=20.0, fee=5.0,
        post_qty=500, post_cost=20.0, post_cash=90000.0,
    ))
    trace.record(TraceRecord(
        signal_date=datetime.date(2024, 7, 1),
        execution_date=datetime.date(2024, 7, 2),
        code="000002", formula="CLOSE > MA(CLOSE, 20)",
        eligible_count=150, ranking_score=1.10, ranking_position=10,
        target_weight=0.05, side="BUY", target_qty=300,
        fill_qty=300, fill_price=15.0, fee=5.0,
        post_qty=300, post_cost=15.0, post_cash=85500.0,
    ))
    # 按 code 查询
    q1 = trace.query(code="000001")
    assert len(q1) == 1
    assert q1[0].fill_price == 20.0
    # 按 signal_date 查询
    q2 = trace.query(signal_date=datetime.date(2024, 7, 1))
    assert len(q2) == 2
    # 按 execution_date 查询
    q3 = trace.query(execution_date=datetime.date(2024, 7, 2))
    assert len(q3) == 2


def test_trace_to_dataframe():
    """转换为 Polars DataFrame。"""
    trace = SignalTrace()
    trace.record(TraceRecord(
        signal_date=datetime.date(2024, 7, 1),
        execution_date=datetime.date(2024, 7, 2),
        code="000001", formula="CLOSE > MA(CLOSE, 20)",
        eligible_count=150, ranking_score=1.23, ranking_position=5,
        target_weight=0.05, side="BUY", target_qty=500,
        fill_qty=500, fill_price=20.0, fee=5.0,
        post_qty=500, post_cost=20.0, post_cash=90000.0,
    ))
    df = trace.to_dataframe()
    assert df.shape == (1, 18)
    assert df["code"][0] == "000001"
```

- [ ] **Step 2: 运行测试确认失败**

```bash
cd backend && python -m pytest tests/test_signal_trace.py -v
```
预期：FAIL（module not found）

- [ ] **Step 3: 实现 signal_trace.py**

```python
# backend/core/signal_trace.py
"""SignalTrace：完整决策链审计。"""
import datetime
from dataclasses import dataclass, field
from typing import Optional, List
import polars as pl


@dataclass
class TraceRecord:
    """单条交易决策审计记录。"""
    signal_date: datetime.date
    execution_date: datetime.date
    code: str
    formula: str
    eligible_count: int = 0
    ranking_score: float = 0.0
    ranking_position: int = 0
    target_weight: float = 0.0
    side: str = ""           # BUY / SELL
    target_qty: int = 0
    fill_qty: int = 0
    fill_price: float = 0.0
    fee: float = 0.0
    rejection_reason: str = ""
    post_qty: int = 0        # 成交后持仓数量
    post_cost: float = 0.0   # 成交后成本
    post_cash: float = 0.0   # 成交后现金


class SignalTrace:
    """决策链审计存储。"""

    def __init__(self):
        self._records: List[TraceRecord] = []

    def record(self, rec: TraceRecord):
        self._records.append(rec)

    def query(self, code: str = None, signal_date: datetime.date = None,
              execution_date: datetime.date = None) -> List[TraceRecord]:
        """按条件过滤记录。"""
        result = self._records
        if code is not None:
            result = [r for r in result if r.code == code]
        if signal_date is not None:
            result = [r for r in result if r.signal_date == signal_date]
        if execution_date is not None:
            result = [r for r in result if r.execution_date == execution_date]
        return result

    def to_dataframe(self) -> pl.DataFrame:
        """转换为 Polars DataFrame。"""
        if not self._records:
            return pl.DataFrame()
        return pl.DataFrame([vars(r) for r in self._records])

    def __len__(self):
        return len(self._records)
```

- [ ] **Step 4: 运行测试确认通过**

```bash
cd backend && python -m pytest tests/test_signal_trace.py -v
```
预期：全部 PASS

- [ ] **Step 5: 提交**

```bash
git add backend/core/signal_trace.py backend/tests/test_signal_trace.py
git commit -m "feat: SignalTrace 决策链审计"
```

---

### Task 7: 历史费率支持

**Files:**
- Modify: `backend/core/backtest_types.py`（FeeConfig 支持时间查询）
- Modify: `backend/tests/test_fee_schedule.py`（新建）

**目标:** FeeConfig 支持按日期返回不同费率。

- [ ] **Step 1: 写测试**

```python
# backend/tests/test_fee_schedule.py
"""历史费率测试。"""
import datetime
from core.backtest_types import FeeConfig, FeeSchedule


def test_static_fee_config():
    """静态 FeeConfig 向后兼容。"""
    fc = FeeConfig()
    fee = fc.commission(10000.0, datetime.date(2024, 1, 1))
    assert fee >= fc.commission_min


def test_fee_schedule_by_date():
    """按日期返回不同费率。"""
    schedule = FeeSchedule([
        FeeConfig(commission_rate=0.0003, date_start=datetime.date(2023, 1, 1)),
        FeeConfig(commission_rate=0.00025, date_start=datetime.date(2024, 1, 1)),
    ])
    fee_2023 = schedule.get_fee_config(datetime.date(2023, 6, 1))
    fee_2024 = schedule.get_fee_config(datetime.date(2024, 6, 1))
    assert fee_2023.commission_rate == 0.0003
    assert fee_2024.commission_rate == 0.00025
```

- [ ] **Step 2: 运行测试确认失败**

```bash
cd backend && python -m pytest tests/test_fee_schedule.py -v
```
预期：FAIL（FeeSchedule not defined）

- [ ] **Step 3: 在 backtest_types.py 中实现 FeeSchedule**

```python
@dataclass
class FeeSchedule:
    """按日期返回 FeeConfig 的费率表。"""
    entries: list  # list of FeeConfig with date_start
    _sorted: bool = False

    def __post_init__(self):
        self.entries = sorted(self.entries, key=lambda e: e.date_start)
        self._sorted = True

    def get_fee_config(self, date: datetime.date) -> FeeConfig:
        """返回 date 对应的 FeeConfig（最近的 date_start ≤ date）。"""
        result = self.entries[0]
        for entry in self.entries:
            if entry.date_start <= date:
                result = entry
            else:
                break
        return result
```

在 `FeeConfig` 中新增 `date_start` 字段和 `commission` 方法：

```python
@dataclass
class FeeConfig:
    commission_rate: float = 0.00025
    commission_min: float = 5.0
    stamp_tax_rate: float = 0.0005
    transfer_fee_rate: float = 0.00001
    date_start: datetime.date = None  # 新增

    def commission(self, amount: float, date: datetime.date = None) -> float:
        """计算佣金。"""
        return max(amount * self.commission_rate, self.commission_min)
```

- [ ] **Step 4: 运行测试确认通过**

```bash
cd backend && python -m pytest tests/test_fee_schedule.py -v
```
预期：全部 PASS

- [ ] **Step 5: 运行全量测试确认无回归**

```bash
cd backend && python -m pytest tests/ -v
```
预期：全部 PASS

- [ ] **Step 6: 提交**

```bash
git add backend/core/backtest_types.py backend/tests/test_fee_schedule.py
git commit -m "feat: 历史费率支持（FeeSchedule 按日期查询）"
```

---

### Task 8: Position 成本追踪修正

**Files:**
- Modify: `backend/core/portfolio.py`（avg_cost 在公司行为时正确调整）
- Modify: `backend/tests/test_portfolio.py`（新增成本追踪测试）

**目标:** 确保 avg_cost 在所有公司行为场景下正确追踪。

- [ ] **Step 1: 写测试**

```python
# 在 backend/tests/test_portfolio.py 中新增
def test_avg_cost_after_dividend_and_split():
    """分红 + 送股后 avg_cost 正确。"""
    p = Portfolio(initial_cash=100000.0)
    p.positions["000001"] = Position(
        code="000001", total_qty=1000, available_qty=1000,
        frozen_qty=0, avg_cost=20.0, market_value=0.0)
    # 分红 0.5
    from core.corporate_actions import CorporateAction, ActionType
    p.apply_corporate_action(CorporateAction(
        date=datetime.date(2024, 7, 1), code="000001",
        action_type=ActionType.CASH_DIVIDEND,
        cash_dividend_per_share=0.5))
    # 10 送 10
    p.apply_corporate_action(CorporateAction(
        date=datetime.date(2024, 12, 25), code="000001",
        action_type=ActionType.STOCK_SPLIT,
        split_ratio=2.0))
    # 验证：avg_cost = (20 - 0.5) / 2 = 9.75
    assert p.positions["000001"].total_qty == 2000
    assert abs(p.positions["000001"].avg_cost - 9.75) < 1e-6
```

- [ ] **Step 2: 运行测试确认通过**

```bash
cd backend && python -m pytest tests/test_portfolio.py::test_avg_cost_after_dividend_and_split -v
```
预期：PASS（Task 2 已实现）

- [ ] **Step 3: 提交**

```bash
git add backend/tests/test_portfolio.py
git commit -m "test: 成本追踪集成测试（分红+送股）"
```

---

## 实施顺序与依赖

```
Task 1 (Corporate Actions 数据模型)
    ↓
Task 2 (Portfolio 公司行为调整)  ← 依赖 Task 1
    ↓
Task 3 (BacktestEngine 集成)    ← 依赖 Task 2
    ↓
Task 8 (Position 成本追踪)      ← 依赖 Task 2
    ↓
Task 4 (Universe 构造)          ← 独立
Task 5 (风险指标)               ← 独立
Task 6 (SignalTrace)            ← 独立
Task 7 (历史费率)               ← 独立
```

建议按 1→2→3→8→4→5→6→7 顺序执行。

---

## 验证方案

每个 Task 完成后：
1. `python -m pytest tests/ -v` 全量通过
2. 手动检查：`git diff` 确认无意外修改
3. 提交并附带明确的 commit message

全部完成后：
1. 用 `CLOSE > MA(CLOSE, 20)` + 2024-2025 全年回测验证无回归
2. 对比有/无公司行为的差异（预期：差异主要在长期持仓的成本追踪上）
3. 确认 225/225 测试通过

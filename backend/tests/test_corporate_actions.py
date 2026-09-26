"""Corporate Actions 数据模型测试。"""
import datetime
from core.corporate_actions import (
    CorporateAction, CorporateActionStore,
    ActionType, adjust_avg_cost_for_dividend, adjust_qty_for_split,
)


def test_action_type_enum():
    assert ActionType.CASH_DIVIDEND.value == "cash_dividend"
    assert ActionType.STOCK_SPLIT.value == "stock_split"
    assert ActionType.BONUS_SHARES.value == "bonus_shares"
    assert ActionType.RIGHTS_ISSUE.value == "rights_issue"


def test_adjust_qty_for_split_2_to_1():
    """10 送 10（2:1 拆股）：qty × 2, avg_cost / 2"""
    qty, cost, avail, frozen = adjust_qty_for_split(
        total_qty=1000, avg_cost=20.0, split_ratio=2.0, frozen_qty=100)
    assert qty == 2000
    assert avail == 1800
    assert frozen == 200
    assert abs(cost - 10.0) < 1e-6


def test_adjust_qty_for_bonus_shares():
    """10 送 3（bonus_ratio=0.3）：qty × 1.3, avg_cost / 1.3"""
    qty, cost, avail, frozen = adjust_qty_for_split(
        total_qty=1000, avg_cost=15.0, split_ratio=1.3, frozen_qty=50)
    assert qty == 1300
    assert avail + frozen == qty
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
                         end_date=datetime.date(2024, 7, 31))
    assert len(result) == 1
    assert result[0].action_type == ActionType.CASH_DIVIDEND


def test_store_empty():
    store = CorporateActionStore([])
    result = store.query(code="000001",
                         start_date=datetime.date(2024, 1, 1),
                         end_date=datetime.date(2024, 12, 31))
    assert len(result) == 0


def test_store_query_all():
    """query_all 返回指定日期范围内的所有公司行为（所有 code）。"""
    actions = [
        CorporateAction(date=datetime.date(2024, 7, 1), code="000001",
                        action_type=ActionType.CASH_DIVIDEND,
                        cash_dividend_per_share=0.5),
        CorporateAction(date=datetime.date(2024, 8, 1), code="600000",
                        action_type=ActionType.STOCK_SPLIT,
                        split_ratio=2.0),
        CorporateAction(date=datetime.date(2024, 12, 25), code="000001",
                        action_type=ActionType.STOCK_SPLIT,
                        split_ratio=2.0),
    ]
    store = CorporateActionStore(actions)
    # 查 2024H1~H2: 应返回前两条
    result = store.query_all(start_date=datetime.date(2024, 1, 1),
                             end_date=datetime.date(2024, 9, 30))
    assert len(result) == 2
    codes = {r.code for r in result}
    assert codes == {"000001", "600000"}

def test_portfolio_cash_dividend_updates_cash_and_cost():
    """现金分红：现金增加，同时每股成本扣减；持仓数量不变。"""
    from core.portfolio import Portfolio, Position
    portfolio = Portfolio(initial_cash=10_000.0)
    portfolio.load_initial_positions({"sh.600000": Position(
        code="sh.600000", total_qty=1000, available_qty=1000, avg_cost=20.0)})
    portfolio.apply_corporate_action(CorporateAction(
        date=datetime.date(2024, 6, 18), code="sh.600000",
        action_type=ActionType.CASH_DIVIDEND, cash_dividend_per_share=0.5))
    pos = portfolio.positions["sh.600000"]
    assert portfolio.cash == 10_500.0
    assert pos.total_qty == 1000 and pos.available_qty == 1000 and pos.frozen_qty == 0
    assert abs(pos.avg_cost - 19.5) < 1e-9


def test_portfolio_bonus_shares_preserve_t1_split():
    """送转：总股数和冻结股数按比例增加，成本按比例下降。"""
    from core.portfolio import Portfolio, Position
    portfolio = Portfolio(initial_cash=10_000.0)
    portfolio.load_initial_positions({"sh.600000": Position(
        code="sh.600000", total_qty=1000, available_qty=800, frozen_qty=200, avg_cost=15.0)})
    portfolio.apply_corporate_action(CorporateAction(
        date=datetime.date(2024, 6, 18), code="sh.600000",
        action_type=ActionType.BONUS_SHARES, split_ratio=1.3))
    pos = portfolio.positions["sh.600000"]
    assert pos.total_qty == 1300
    assert pos.frozen_qty == 260 and pos.available_qty == 1040
    assert pos.available_qty + pos.frozen_qty == pos.total_qty
    assert abs(pos.avg_cost - (15.0 / 1.3)) < 1e-9
    assert portfolio.cash == 10_000.0


def test_portfolio_same_day_dividend_and_bonus_are_both_applied():
    """同一除权除息日同时存在现金分红和送股时，两项都必须生效。"""
    from core.portfolio import Portfolio, Position
    portfolio = Portfolio(initial_cash=5_000.0)
    portfolio.load_initial_positions({"sh.600000": Position(
        code="sh.600000", total_qty=1000, available_qty=1000, avg_cost=20.0)})
    date = datetime.date(2024, 6, 18)
    portfolio.apply_corporate_action(CorporateAction(
        date=date, code="sh.600000", action_type=ActionType.CASH_DIVIDEND,
        cash_dividend_per_share=0.5))
    portfolio.apply_corporate_action(CorporateAction(
        date=date, code="sh.600000", action_type=ActionType.BONUS_SHARES,
        split_ratio=1.2))
    pos = portfolio.positions["sh.600000"]
    assert portfolio.cash == 5_500.0
    assert pos.total_qty == 1200 and pos.available_qty == 1200 and pos.frozen_qty == 0
    assert abs(pos.avg_cost - 19.5 / 1.2) < 1e-9


def test_portfolio_rights_issue_default_is_no_participation():
    """默认不参与配股：现金、数量、成本和 T+1 状态均不改变。"""
    from core.portfolio import Portfolio, Position
    portfolio = Portfolio(initial_cash=10_000.0)
    portfolio.load_initial_positions({"sh.600000": Position(
        code="sh.600000", total_qty=1000, available_qty=900, frozen_qty=100, avg_cost=12.0)})
    portfolio.apply_corporate_action(CorporateAction(
        date=datetime.date(2024, 6, 18), code="sh.600000",
        action_type=ActionType.RIGHTS_ISSUE, rights_price=8.0, rights_ratio=0.1))
    pos = portfolio.positions["sh.600000"]
    assert portfolio.cash == 10_000.0
    assert pos.total_qty == 1000 and pos.available_qty == 900 and pos.frozen_qty == 100
    assert pos.avg_cost == 12.0

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
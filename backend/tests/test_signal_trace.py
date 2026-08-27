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
    assert df.shape == (1, 17)
    assert df["code"][0] == "000001"

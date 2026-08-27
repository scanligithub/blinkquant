"""
直接运行 RQAlpha 验证 timing 语义
"""
from rqalpha import run_func
from rqalpha.api import order_shares


def init(context):
    context.stock = "600000.XSHG"
    context.ordered = False


def handle_bar(context, bar_dict):
    if not context.ordered:
        order_shares("600000.XSHG", 100)
        context.ordered = True
        print(f"Signal at: {context.now}")


config = {
    "base": {
        "start_date": "2024-01-02",
        "end_date": "2024-01-10",
        "frequency": "1d",
        "accounts": {"stock": 100000},
    },
    "mod": {
        "sys_simulation": {
            "signal": False,
            "matching_type": "current_bar",
        },
        "sys_accounts": {
            "stock_t_plus": True,
        }
    }
}

result = run_func(
    init=lambda ctx: setattr(ctx, "stock", "600000.XSHG") or setattr(ctx, "ordered", False),
    handle_bar=lambda ctx, bd: (setattr(ctx, "ordered", True) or order_shares("600000.XSHG", 100)) if not ctx.ordered else None,
    config=config
)

print("=== RQAlpha Timing Test Result ===")
trades = result.trades
print(f"Total trades: {len(trades)}")
for t in trades:
    print(f"  signal_date={t['signal_date']}, execution_date={t['execution_date']}, price={t['price']}, side={t['side']}, qty={t['qty']}")

# 验证 T signal -> T+1 open
for t in trades:
    sig = t['signal_date']
    exec_d = t['execution_date']
    diff = (exec_d - sig).days
    print(f"  signal_date={sig}, execution_date={exec_d}, diff={diff} days")
import datetime as dt

import pytest

from core.backtest_config import BacktestConfig, FeePolicy
from core.strategy import StrategyDefinition


def _strategy():
    return StrategyDefinition.from_dict({
        "universe": {"type": "all_a"},
        "entry": {"condition": "CLOSE > MA(CLOSE,20)", "trigger": "condition", "timeframe": "D"},
        "sizing": {"method": "top_n_equal_weight", "max_positions": 20},
        "rebalance": {"frequency": "weekly"},
        "mode": "target_portfolio",
    })


def _config(**kwargs):
    return BacktestConfig(
        start_date=dt.date(2024, 1, 2),
        end_signal_date=dt.date(2024, 3, 29),
        initial_cash=1_000_000,
        strategy=_strategy(),
        **kwargs,
    )


def test_fee_policy_defaults_to_historical():
    cfg = _config()
    assert cfg.fee_policy.mode == "historical"
    assert cfg.historical_fees is True
    assert cfg.to_dict()["fee_policy"]["mode"] == "historical"


def test_legacy_historical_fees_false_maps_to_fixed():
    cfg = _config(historical_fees=False)
    assert cfg.fee_policy.mode == "fixed"
    assert cfg.historical_fees is False


def test_fixed_fee_policy_round_trips():
    policy = FeePolicy(
        mode="fixed",
        commission_rate=0.0002,
        commission_min=3,
        stamp_tax_rate=0.001,
        transfer_fee_rate=0.00001,
    )
    cfg = _config(fee_policy=policy)
    restored = BacktestConfig.from_dict(cfg.to_dict())
    assert restored.fee_policy == policy
    assert restored.historical_fees is False


def test_fee_policy_rejects_negative_rates():
    with pytest.raises(ValueError, match="commission_rate"):
        FeePolicy(mode="fixed", commission_rate=-0.1)


def test_fee_policy_rejects_unknown_mode():
    with pytest.raises(ValueError, match="historical or fixed"):
        FeePolicy(mode="custom")


def test_conflicting_legacy_and_new_fee_policy_rejected():
    with pytest.raises(ValueError, match="conflicts"):
        _config(
            historical_fees=False,
            fee_policy=FeePolicy(mode="historical"),
        )


def test_old_backtest_config_dict_is_still_readable():
    data = _config(historical_fees=False).to_dict()
    data.pop("fee_policy")
    restored = BacktestConfig.from_dict(data)
    assert restored.fee_policy.mode == "fixed"
    assert restored.historical_fees is False

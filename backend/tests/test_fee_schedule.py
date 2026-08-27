import datetime
from core.backtest_types import FeeConfig, FeeSchedule


def test_static_fee_config():
    fc = FeeConfig()
    assert fc.commission_rate == 0.00025
    assert fc.commission_min == 5.0


def test_fee_schedule_by_date():
    schedule = FeeSchedule([
        FeeConfig(commission_rate=0.0003, date_start=datetime.date(2023, 1, 1)),
        FeeConfig(commission_rate=0.00025, date_start=datetime.date(2024, 1, 1)),
    ])
    fee_2023 = schedule.get_fee_config(datetime.date(2023, 6, 1))
    fee_2024 = schedule.get_fee_config(datetime.date(2024, 6, 1))
    assert fee_2023.commission_rate == 0.0003
    assert fee_2024.commission_rate == 0.00025


def test_fee_schedule_before_first_entry():
    schedule = FeeSchedule([
        FeeConfig(commission_rate=0.0003, date_start=datetime.date(2023, 1, 1)),
    ])
    fee = schedule.get_fee_config(datetime.date(2022, 1, 1))
    assert fee.commission_rate == 0.0003

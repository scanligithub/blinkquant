import pytest

from core.metrics import compute_benchmark_metrics


def test_benchmark_metrics_known_series():
    # Portfolio = benchmark + a constant 0.1% daily active return.
    benchmark = [0.01, -0.005, 0.008, 0.002, -0.003, 0.006]
    portfolio = [b + 0.001 for b in benchmark]
    m = compute_benchmark_metrics(portfolio, benchmark)

    assert m["beta"] == pytest.approx(1.0, abs=1e-10)
    assert m["alpha"] == pytest.approx(0.252, rel=1e-6)
    assert m["tracking_error"] == pytest.approx(0.0, abs=1e-12)
    assert m["information_ratio"] == 0.0


def test_benchmark_metrics_active_risk_and_ir():
    benchmark = [0.01, 0.00, -0.01, 0.02, -0.02]
    portfolio = [0.015, 0.005, -0.005, 0.015, -0.015]
    m = compute_benchmark_metrics(portfolio, benchmark)

    assert m["beta"] == pytest.approx(0.8, abs=1e-10)
    assert m["tracking_error"] > 0
    assert m["information_ratio"] != 0


def test_benchmark_metrics_short_series_returns_zeros():
    assert compute_benchmark_metrics([0.01], [0.02]) == {
        "alpha": 0.0,
        "beta": 0.0,
        "tracking_error": 0.0,
        "information_ratio": 0.0,
    }

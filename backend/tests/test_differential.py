"""Tests for DifferentialValidator comparison logic."""

import polars as pl
from core.differential_validator import DifferentialValidator, DiffCategory


def test_build_summary_empty():
    """Empty details should produce 100% match rate."""
    v = DifferentialValidator()
    v.details = []
    summary = v._build_summary()
    assert summary["true_mismatch_count"] == 0
    assert summary["match_rate"] == 1.0


def test_build_summary_with_semantic():
    """PRICE_SEMANTIC diffs should not count as mismatches."""
    from core.differential_validator import DiffDetail
    v = DifferentialValidator()
    v.details = [
        DiffDetail(level="TRADE", category=DiffCategory.PRICE_SEMANTIC.value,
                   date="2024-01-08", code="sh.600519",
                   blinkquant={"price": 100.0}, rqalpha={"price": 101.0},
                   diff={"price_delta": 1.0}),
    ]
    summary = v._build_summary()
    assert summary["true_mismatch_count"] == 0
    assert summary["mismatches_by_category"].get("PRICE_SEMANTIC", 0) == 1


def test_build_summary_with_true_mismatch():
    """FEE_MODEL diffs should count as mismatches."""
    from core.differential_validator import DiffDetail
    v = DifferentialValidator()
    v.details = [
        DiffDetail(level="TRADE", category=DiffCategory.FEE_MODEL.value,
                   date="2024-01-08", code="sh.600519",
                   blinkquant={"fee": 5.0}, rqalpha={"fee": 5.25},
                   diff={"fee_delta": 0.25}),
    ]
    summary = v._build_summary()
    assert summary["true_mismatch_count"] == 1

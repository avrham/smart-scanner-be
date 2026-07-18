"""Baseline buy&hold returns and signal-vs-baseline deltas."""

import pytest

from app.workers.outcomes.baselines import (
    baseline_delta,
    compute_benchmark_returns,
)


def test_benchmark_returns_labeled():
    out = compute_benchmark_returns(100, [101, 102], windows=[1, 3])
    assert out["1D"] == pytest.approx(1.0)
    assert out["3D"] is None  # not enough bars


def test_benchmark_returns_missing_price():
    out = compute_benchmark_returns(None, [101, 102], windows=[1, 3])
    assert out == {"1D": None, "3D": None}


def test_baseline_delta():
    signal = {1: 2.0, 3: 5.0}
    baseline = {"1D": 1.0, "3D": None}
    delta = baseline_delta(signal, baseline, windows=[1, 3])
    assert delta["1D"] == pytest.approx(1.0)
    assert delta["3D"] is None  # baseline missing => delta None


def test_baseline_delta_handles_missing_signal():
    signal = {1: None}
    baseline = {"1D": 1.0}
    delta = baseline_delta(signal, baseline, windows=[1])
    assert delta["1D"] is None

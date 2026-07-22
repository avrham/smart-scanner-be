"""Phase 9B: causal effort-vs-result measurements."""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from app.workers.strategies.wyckoff_v2.constants import (
    EFFORT_RESULT_VERSION,
    Phase9AConfigError,
    resolve_config,
)
from app.workers.strategies.wyckoff_v2.effort_result import (
    EffortResultError,
    measure_effort_result_at_index,
)


def _bars(n: int, *, end: str = "2024-06-28", close0: float = 100.0):
    end_ts = pd.Timestamp(end)
    dates = []
    cur = end_ts
    while len(dates) < n:
        if cur.weekday() < 5:
            dates.append(cur)
        cur -= pd.Timedelta(days=1)
    dates = list(reversed(dates))
    closes = close0 + np.linspace(0, 5, n)
    return pd.DataFrame(
        {
            "date": dates,
            "open": closes - 0.5,
            "high": closes + 1.0,
            "low": closes - 1.0,
            "close": closes,
            "volume": np.full(n, 1_000_000.0),
        }
    )


class TestEffortResultStates:
    def test_agreement_and_divergence_states(self):
        df = _bars(40)
        cfg = resolve_config(
            {
                "event_atr_window": 5,
                "event_volume_baseline_window": 10,
                "event_min_volume_baseline_bars": 5,
                "effort_high_volume_ratio": 1.2,
                "effort_low_volume_ratio": 0.8,
                "result_high_atr_ratio": 0.5,
                "result_low_atr_ratio": 0.1,
            }
        )
        as_of = df["date"].iloc[-1]
        # High effort / high result: spike volume + large move
        i = 30
        df.loc[i, "volume"] = 5_000_000.0
        df.loc[i, "close"] = float(df.loc[i - 1, "close"]) + 5.0
        df.loc[i, "high"] = float(df.loc[i, "close"]) + 0.5
        df.loc[i, "low"] = float(df.loc[i - 1, "close"]) - 0.2
        df.loc[i, "open"] = float(df.loc[i - 1, "close"])
        m = measure_effort_result_at_index(df, i, as_of_date=as_of, config=cfg)
        assert m.effort_result_version == EFFORT_RESULT_VERSION
        assert m.effort_state == "high"
        assert m.result_state == "high"
        assert m.effort_result_state == "agreement_high"

        # High effort / low result
        j = 31
        df.loc[j, "volume"] = 5_000_000.0
        prev = float(df.loc[j - 1, "close"])
        df.loc[j, "close"] = prev + 0.01
        df.loc[j, "high"] = prev + 0.05
        df.loc[j, "low"] = prev - 0.05
        df.loc[j, "open"] = prev
        m2 = measure_effort_result_at_index(df, j, as_of_date=as_of, config=cfg)
        assert m2.effort_state == "high"
        assert m2.result_state == "low"
        assert m2.effort_result_state == "high_effort_low_result"

        # Low effort / high result
        k = 32
        df.loc[k, "volume"] = 100_000.0
        prev = float(df.loc[k - 1, "close"])
        df.loc[k, "close"] = prev + 5.0
        df.loc[k, "high"] = float(df.loc[k, "close"]) + 0.2
        df.loc[k, "low"] = prev - 0.2
        df.loc[k, "open"] = prev
        m3 = measure_effort_result_at_index(df, k, as_of_date=as_of, config=cfg)
        assert m3.effort_state == "low"
        assert m3.result_state == "high"
        assert m3.effort_result_state == "low_effort_high_result"

        # Low / low
        t = 33
        df.loc[t, "volume"] = 100_000.0
        prev = float(df.loc[t - 1, "close"])
        df.loc[t, "close"] = prev + 0.01
        df.loc[t, "high"] = prev + 0.02
        df.loc[t, "low"] = prev - 0.02
        df.loc[t, "open"] = prev
        m4 = measure_effort_result_at_index(df, t, as_of_date=as_of, config=cfg)
        assert m4.effort_state == "low"
        assert m4.result_state == "low"
        assert m4.effort_result_state == "agreement_low"


class TestVolumeAndATR:
    def test_missing_measured_and_baseline_volume(self):
        df = _bars(40)
        cfg = resolve_config(
            {
                "event_atr_window": 5,
                "event_volume_baseline_window": 10,
                "event_min_volume_baseline_bars": 5,
            }
        )
        as_of = df["date"].iloc[-1]
        i = 25
        df.loc[i, "volume"] = np.nan
        m = measure_effort_result_at_index(df, i, as_of_date=as_of, config=cfg)
        assert m.relative_volume is None
        assert m.effort_state == "unknown"
        assert "missing_measured_volume" in m.missing_data

        # Insufficient baseline: early index
        m2 = measure_effort_result_at_index(df, 3, as_of_date=as_of, config=cfg)
        assert m2.relative_volume is None
        assert "insufficient_volume_baseline" in m2.missing_data

    def test_baseline_excludes_measured_bar(self):
        df = _bars(40)
        cfg = resolve_config(
            {
                "event_atr_window": 5,
                "event_volume_baseline_window": 10,
                "event_min_volume_baseline_bars": 5,
            }
        )
        as_of = df["date"].iloc[-1]
        i = 25
        # Inflate measured volume only — baseline mean must ignore it.
        df.loc[i, "volume"] = 50_000_000.0
        m = measure_effort_result_at_index(df, i, as_of_date=as_of, config=cfg)
        assert m.volume_baseline_mean is not None
        assert m.volume_baseline_mean < 2_000_000.0
        assert m.relative_volume is not None and m.relative_volume > 10

    def test_zero_range_bar(self):
        df = _bars(40)
        cfg = resolve_config({"event_atr_window": 5, "event_min_volume_baseline_bars": 5})
        as_of = df["date"].iloc[-1]
        i = 20
        px = float(df.loc[i, "close"])
        df.loc[i, "open"] = px
        df.loc[i, "high"] = px
        df.loc[i, "low"] = px
        df.loc[i, "close"] = px
        m = measure_effort_result_at_index(df, i, as_of_date=as_of, config=cfg)
        assert m.close_location_value is None
        assert "zero_range_bar" in m.missing_data

    def test_causal_atr_and_future_invariance(self):
        df = _bars(40)
        cfg = resolve_config({"event_atr_window": 5, "event_min_volume_baseline_bars": 5})
        as_of = df["date"].iloc[30]
        m1 = measure_effort_result_at_index(df, 30, as_of_date=as_of, config=cfg)
        # Mutate future bars after as_of
        df2 = df.copy()
        df2.loc[35:, "high"] = 999.0
        df2.loc[35:, "volume"] = 99_000_000.0
        m2 = measure_effort_result_at_index(df2, 30, as_of_date=as_of, config=cfg)
        assert m1.to_dict() == m2.to_dict()

    def test_non_finite_rejection(self):
        df = _bars(20)
        df.loc[10, "high"] = float("inf")
        with pytest.raises(EffortResultError) as exc:
            measure_effort_result_at_index(
                df, 10, as_of_date=df["date"].iloc[-1], config=resolve_config()
            )
        assert exc.value.reason_code == "non_finite_input"

    def test_json_safe(self):
        df = _bars(40)
        m = measure_effort_result_at_index(
            df, 25, as_of_date=df["date"].iloc[-1], config=resolve_config()
        )
        json.dumps(m.to_dict(), allow_nan=False, sort_keys=True)

    def test_config_rejects_bad_ratios(self):
        with pytest.raises(Phase9AConfigError):
            resolve_config({"effort_high_volume_ratio": 0.5, "effort_low_volume_ratio": 0.8})

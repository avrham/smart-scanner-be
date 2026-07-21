"""Deterministic OHLCV frame builders for the sma150.v3 tests (Phase 8).

Two synthetic-but-realistic geometries:

  * build_uptrend_frame(...) — rising SMA (positive slope). Baseline rides a
    linear ramp ~4-5% above the SMA; configurable single-bar dips to the SMA
    create independent bounce events; the last bar's proximity/trigger/volume
    are controlled per test.

  * build_jbl_like_frame(...) — the known regression geometry: SMA declining
    at the end (high plateau leaving the 150-bar window), three v2 bounce
    detections (two of them clustered), strong rebounds, final close ~2.3%
    above the SMA, volume ratio ~1.07, bearish last bar with no breakout.
    sma150.v2 classifies this ENTER; sma150.v3 must classify WATCH.

No provider calls, no randomness, business-day dates.
"""

from typing import Dict, List, Optional, Sequence

import pandas as pd

BASE_VOLUME = 1_000_000.0


def _sma_est(closes: List[float], window: int = 150) -> float:
    """Approximate SMA of the NEXT bar from the trailing built closes."""
    tail = closes[-(window - 1):]
    return sum(tail) / len(tail)


def _to_frame(
    closes: List[float],
    volumes: List[float],
    opens: Optional[List[Optional[float]]] = None,
    highs: Optional[List[Optional[float]]] = None,
    lows: Optional[List[Optional[float]]] = None,
) -> pd.DataFrame:
    n = len(closes)
    dates = pd.bdate_range("2023-01-02", periods=n)
    rows = []
    for i in range(n):
        close = closes[i]
        open_ = None if opens is None else opens[i]
        high = None if highs is None else highs[i]
        low = None if lows is None else lows[i]
        if open_ is None:
            open_ = closes[i - 1] if i > 0 else close
        if high is None:
            high = max(open_, close) * 1.002
        if low is None:
            low = min(open_, close) * 0.998
        rows.append({
            "date": dates[i],
            "open": round(open_, 4),
            "high": round(high, 4),
            "low": round(low, 4),
            "close": round(close, 4),
            "volume": volumes[i],
        })
    return pd.DataFrame(rows)


def build_uptrend_frame(
    *,
    n: int = 430,
    touch_offsets: Sequence[int] = (300, 340, 380),
    rebound_pct: float = 10.0,
    end_prox_pct: float = 2.0,
    vol_ratio: float = 1.30,
    trigger: bool = True,
    ramp: float = 0.07,
) -> pd.DataFrame:
    """Rising-SMA frame with configurable dips, end proximity and trigger.

    The linear ramp starts at bar 0 so that by the first SMA-valid bar the
    baseline already sits ~4-5% above the SMA (out of the touch band); only
    the engineered dips enter the band. Each dip rebounds to exactly
    `rebound_pct` above the touch and HOLDS there for the remaining rebound
    window, so the measured max rebound equals the configured one (a rising
    baseline can never manufacture a rebound).

    end_prox_pct is the final close vs SMA in percent (negative = below).
    trigger=True engineers a prior-high breakout + bullish close + strong CLV
    on the last bar; trigger=False makes the last bar bearish with no breakout.
    """
    closes: List[float] = []
    rebound_plan: Dict[int, float] = {}

    def baseline(t: int) -> float:
        return 100.0 + t * ramp

    for t in range(n - 4):  # last 4 bars engineered below
        if t in rebound_plan:
            closes.append(rebound_plan[t])
            continue
        if t in touch_offsets:
            dip = _sma_est(closes) * 1.005
            peak = dip * (1.0 + rebound_pct / 100.0)
            for k in range(1, 6):
                rebound_plan[t + k] = dip + (peak - dip) * k / 5.0
            for k in range(6, 11):
                rebound_plan[t + k] = peak
            closes.append(dip)
            continue
        closes.append(baseline(t))

    # Final approach + current bar, anchored to the estimated SMA.
    sma_now = _sma_est(closes)
    final_close = sma_now * (1.0 + end_prox_pct / 100.0)
    opens: List[Optional[float]] = [None] * n
    highs: List[Optional[float]] = [None] * n
    lows: List[Optional[float]] = [None] * n

    if trigger:
        # Approach from slightly below, then break the prior bar's high.
        closes.extend([
            final_close * 0.992,
            final_close * 0.987,
            final_close * 0.990,
            final_close,
        ])
        highs[n - 2] = final_close * 0.991    # prior high < final close
        opens[n - 1] = final_close * 0.990    # bullish close
        highs[n - 1] = final_close * 1.001
        lows[n - 1] = final_close * 0.988     # CLV ~0.92
    else:
        # Approach from above, bearish last bar under the prior high.
        closes.extend([
            final_close * 1.020,
            final_close * 1.012,
            final_close * 1.006,
            final_close,
        ])
        opens[n - 1] = final_close * 1.010    # close < open (bearish)
        highs[n - 1] = final_close * 1.012
        lows[n - 1] = final_close * 0.998

    volumes = [BASE_VOLUME] * n
    volumes[-1] = BASE_VOLUME * vol_ratio
    return _to_frame(closes, volumes, opens, highs, lows)


def build_jbl_like_frame(
    *,
    n: int = 430,
    end_prox_pct: float = 2.3,
    vol_ratio: float = 1.07,
    overshoot_pct: float = 8.0,
    touch_events: Sequence[int] = (300, 330, 338),
) -> pd.DataFrame:
    """Declining-SMA geometry with bounce rallies from below.

    Shape: flat 100 -> ramp to 130 -> plateau 130 -> decline. The plateau
    leaving the 150-bar window makes the SMA slope negative at the end.
    Bounce events are sharp rallies from below the band up to the SMA with an
    overshoot (the rebound), then a sharp drop back below the band. Events at
    bars 300 and 330+338 (the last two cluster for v3, count separately for
    v2). The final bars pop from below up to `end_prox_pct` above the SMA on
    a bearish, non-breakout last bar with `vol_ratio` volume.
    """
    closes: List[float] = []
    overrides: Dict[int, float] = {}

    def base(t: int) -> float:
        if t < 100:
            return 100.0
        if t < 180:
            return 100.0 + (t - 99) * 0.375       # ramp to 130
        if t < 260:
            return 130.0                          # plateau
        if t < 340:
            return 130.0 - (t - 260) * 0.30       # steep decline to 106
        return 106.0 - (t - 340) * 0.05           # gentle decline

    for t in range(n - 6):  # last 6 bars engineered below
        if t in overrides:
            closes.append(overrides[t])
            continue
        if t in touch_events:
            sma_now = _sma_est(closes)
            touch = sma_now * 0.995
            peak = touch * (1.0 + overshoot_pct / 100.0)
            # Overshoot for 3 bars (out of band above), then sharp drop back.
            overrides[t + 1] = sma_now * 1.045
            overrides[t + 2] = peak
            overrides[t + 3] = sma_now * 1.05
            # t+4 falls straight back to the declining base (below band).
            closes.append(touch)
            continue
        closes.append(base(t))

    # Final pop from below the band up to end_prox_pct above the SMA.
    sma_now = _sma_est(closes)
    final_close = sma_now * (1.0 + end_prox_pct / 100.0)
    start = closes[-1]
    opens: List[Optional[float]] = [None] * n
    highs: List[Optional[float]] = [None] * n
    lows: List[Optional[float]] = [None] * n

    for k in range(1, 6):  # 5 rising approach bars
        closes.append(start + (final_close * 0.995 - start) * k / 5.0)
    closes.append(final_close)

    highs[n - 2] = final_close * 1.002        # prior high above final close
    opens[n - 1] = final_close * 1.010        # bearish last bar
    highs[n - 1] = final_close * 1.012
    lows[n - 1] = final_close * 0.997         # weak close location

    volumes = [BASE_VOLUME] * n
    volumes[-1] = BASE_VOLUME * vol_ratio
    return _to_frame(closes, volumes, opens, highs, lows)

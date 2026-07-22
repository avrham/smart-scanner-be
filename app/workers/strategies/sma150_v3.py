"""sma150.v3 — SMA-150 bounce with a strict decision-layer separation (Phase 8).

Registered SEPARATELY from sma150_bounce (which stays on sma150.v2, untouched):

    strategy_code           sma150_bounce_v3
    strategy_family         sma150_bounce
    strategy_version        sma150.v3
    decision_policy_version sma150_bounce.policy.v1

Four layers, in authority order:

  A. Data readiness   — not enough completed bars => AVOID, everything unknown.
  B. Setup validity   — current SMA proximity + independent historical bounce
                        events (clustered, non-overlapping rebound windows) +
                        robust (median) rebound quality. Invalid => AVOID.
  C. Entry confirmation — close above SMA, positive SMA slope, deterministic
                        bullish trigger, volume confirmation. A valid setup
                        with any missing/failed confirmation => WATCH.
  D. Ranking score    — continuous, versioned, for ORDERING only. It can never
                        authorize ENTER, hide a failed gate, or convert an
                        AVOID/WATCH.

Every measurement is mapped to an evidence.v1 item (raw value + threshold +
operator + state) and the bundle is persisted inside the immutable Phase 7B
evidence snapshot. Unknown stays unknown; nothing is fabricated.
"""

import logging
import statistics
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from app.workers.indicators import sma, validate_dataframe
from app.workers.strategies.bar_completion import (
    BAR_COMPLETION_POLICY,
    assess_latest_bar_completion as _assess_latest_bar_completion_impl,
)
from app.workers.strategies.base import (
    Strategy,
    StrategyContext,
    StrategyDecision,
    StrategyResult,
    StrategySide,
)
from app.workers.strategies.evidence import (
    EVIDENCE_VERSION,
    EvidenceBundle,
    EvidenceItem,
)

logger = logging.getLogger(__name__)

PATTERN_CODE = "sma150_bounce_v3"
STRATEGY_FAMILY = "sma150_bounce"
STRATEGY_VERSION = "sma150.v3"
DECISION_POLICY_VERSION = "sma150_bounce.policy.v1"
# The ranking formula set has its own version so a future re-weighting can
# never be confused with v1 rankings. NOT a probability.
RANKING_VERSION = "sma150.v3.rank.v1"
INVALIDATION_RULE_CODE = "daily_close_below_sma150_pct"
# Completed-daily-bar policy (ny_session_close.v1): the implementation now
# lives in app.workers.strategies.bar_completion (extracted in Phase 9A so
# wyckoff_mtf.v2 can share it). BAR_COMPLETION_POLICY is re-exported above;
# assess_latest_bar_completion below preserves this module's import surface
# AND its injectable `_utc_now` clock, byte-for-byte in behavior.

# All decision-relevant values are configurable; DB pattern_configs override
# these defaults via the existing resolver. No magic constants in the logic.
DEFAULT_CONFIG: Dict[str, Any] = {
    "sma_window": 150,
    "min_history_bars": 200,
    "lookback_bars_for_history": 365,
    "volume_window_bars": 20,
    "slope_lookback_bars": 20,
    "rebound_window_bars": 10,
    # Setup: current proximity band (asymmetric: above vs below the SMA).
    "max_close_above_sma_pct": 3.0,
    "max_close_below_sma_pct": 1.0,
    # Setup: independent historical bounce events.
    "touch_tolerance_pct": 3.0,
    "min_event_separation_bars": 15,
    "min_independent_bounces": 2,
    "min_median_rebound_pct": 5.0,
    # Confirmations.
    "min_sma_slope_pct": 0.0,            # slope must be STRICTLY above this
    "min_close_location_value": 0.65,
    "min_trigger_volume_ratio": 1.20,
    # Risk / invalidation (deterministic; no invented targets).
    "invalidation_below_sma_pct": 2.0,
    # Ranking normalization scales (documented; ordering only).
    "recency_half_life_bars": 126,       # ~half a trading year
    "trend_quality_full_scale_slope_pct": 2.0,
    "bounce_quality_full_count": 4,
    "rebound_quality_full_pct": 10.0,
    # Completed-daily-bar policy (bars dated on the current exchange-session
    # date are partial until the session close; policy is versioned and
    # persisted with every signal via the config snapshot).
    "bar_completion_policy": BAR_COMPLETION_POLICY,
    "exchange_timezone": "America/New_York",
    "session_close_time": "16:00",
    # Hard filters shared with the funnel's cheap stages.
    "min_price": 5.0,
    "min_liquidity_filters": {
        "min_market_cap": 200000000,
        "min_daily_volume": 200000,
    },
}

# The fixed, documented soft-component set averaged into ranking_score.
RANKING_COMPONENTS = (
    "proximity_quality",
    "trend_quality",
    "independent_bounce_quality",
    "rebound_quality",
    "volume_quality",
    "trigger_quality",
    "bounce_recency_quality",
)


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _utc_now() -> datetime:
    """Injectable clock (module-level so tests can pin the evaluation time)."""
    return datetime.now(timezone.utc)


# --------------------------------------------------------------------------- #
# Completed-daily-bar policy (ny_session_close.v1) — shared implementation
# --------------------------------------------------------------------------- #

def assess_latest_bar_completion(
    df: Optional[pd.DataFrame],
    *,
    exchange_timezone: str = "America/New_York",
    session_close_time: str = "16:00",
    explicit_completed: Optional[bool] = None,
    now_utc: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Compatibility surface for the shared ny_session_close.v1 policy.

    The implementation was extracted verbatim to
    `app.workers.strategies.bar_completion` (Phase 9A). This wrapper keeps
    the exact public signature and return contract, and keeps THIS module's
    injectable `_utc_now` clock authoritative for its callers (tests pin
    `sma150_v3._utc_now` to freeze the evaluation time).
    """
    return _assess_latest_bar_completion_impl(
        df,
        exchange_timezone=exchange_timezone,
        session_close_time=session_close_time,
        explicit_completed=explicit_completed,
        now_utc=now_utc if now_utc is not None else _utc_now(),
    )


def _completion_item(
    completion: Dict[str, Any],
    excluded_partial_bar_date: Optional[str],
    reason_code: Optional[str] = None,
) -> EvidenceItem:
    """evidence.v1 item recording the completed-bar decision (never pruned
    away from the readiness picture; the exact policy is in metadata)."""
    state = "pass" if completion["state"] == "completed" else "unknown"
    return EvidenceItem(
        code="latest_bar_completion",
        category="data_readiness",
        source_type="market_data",
        state=state,
        raw_value=completion["state"],
        required=True,
        timeframe="1d",
        as_of=completion.get("bar_date"),
        reason_code=reason_code,
        metadata={
            "policy": completion["policy"],
            "reason": completion["reason"],
            "bar_date": completion.get("bar_date"),
            "excluded_partial_bar_date": excluded_partial_bar_date,
        },
    )


# --------------------------------------------------------------------------- #
# Pure measurement helpers (unit-tested directly)
# --------------------------------------------------------------------------- #

def find_independent_bounce_events(
    hist_df: pd.DataFrame,
    sma_col: str,
    *,
    touch_tolerance_pct: float,
    rebound_window_bars: int,
    min_event_separation_bars: int,
    current_index: int,
) -> List[Dict[str, Any]]:
    """Independent historical SMA-touch events with complete rebound windows.

    `hist_df` must be the HISTORICAL bars only (current bar excluded),
    positionally indexed 0..n-1, with a non-NaN `sma_col`. `current_index` is
    the positional index of the current bar in the full evaluated frame
    (== len(hist_df)) and is used only to report event ages.

    Determinism / independence rules:
      * contiguous in-band bars form ONE run;
      * runs whose edge-to-edge gap is below the EFFECTIVE separation —
        max(min_event_separation_bars, rebound_window_bars + 1) — merge into
        one cluster, so rebound windows can never overlap;
      * one representative bar per cluster: minimum |distance to SMA|, first
        (earliest) bar on exact ties (pandas idxmin);
      * an event whose rebound window is incomplete at the as-of boundary is
        EXCLUDED, never partially measured;
      * no bar beyond `hist_df` is ever read.
    """
    if hist_df is None or len(hist_df) == 0 or sma_col not in hist_df.columns:
        return []

    hist_df = hist_df.reset_index(drop=True)
    n = len(hist_df)
    sma_values = hist_df[sma_col]
    distance_pct = (hist_df["close"] - sma_values).abs() / sma_values * 100.0
    in_band = (distance_pct <= touch_tolerance_pct).to_numpy()

    # Contiguous in-band runs.
    runs: List[Tuple[int, int]] = []
    i = 0
    while i < n:
        if not in_band[i]:
            i += 1
            continue
        start = i
        while i < n and in_band[i]:
            i += 1
        runs.append((start, i - 1))

    # Merge nearby runs into clusters (edge-to-edge gap below effective sep).
    effective_separation = max(
        int(min_event_separation_bars), int(rebound_window_bars) + 1
    )
    clusters: List[Tuple[int, int]] = []
    for start, end in runs:
        if clusters and (start - clusters[-1][1]) < effective_separation:
            clusters[-1] = (clusters[-1][0], end)
        else:
            clusters.append((start, end))

    events: List[Dict[str, Any]] = []
    for start, end in clusters:
        segment = distance_pct.iloc[start:end + 1]
        rep = int(segment.idxmin())  # first minimal bar => deterministic

        # Complete rebound windows only: everything must fit inside hist_df.
        if rep + rebound_window_bars > n - 1:
            continue

        window = hist_df.iloc[rep + 1: rep + 1 + rebound_window_bars]
        touch_price = float(hist_df.iloc[rep]["close"])
        if touch_price <= 0:
            continue
        highs = window["high"].to_numpy(dtype=float)
        max_high = float(highs.max())
        max_rebound_pct = (max_high - touch_price) / touch_price * 100.0
        bars_to_max = int(highs.argmax()) + 1  # first maximal bar (argmax)

        touch_date = pd.to_datetime(hist_df.iloc[rep]["date"]).date().isoformat()
        events.append({
            "touch_index": rep,
            "touch_date": touch_date,
            "touch_price": touch_price,
            "sma_value": float(hist_df.iloc[rep][sma_col]),
            "distance_pct": round(float(segment.min()), 4),
            "max_rebound_pct": round(max_rebound_pct, 4),
            "bars_to_max_rebound": bars_to_max,
            "age_bars": int(current_index - rep),
            "cluster_span_bars": int(end - start + 1),
        })

    return events


def sma_slope_stats(
    sma_series: pd.Series, lookback_bars: int
) -> Optional[Dict[str, Any]]:
    """Normalized SMA slope over `lookback_bars`. None when not computable."""
    clean = sma_series.dropna()
    if len(clean) < lookback_bars + 1:
        return None
    current = float(clean.iloc[-1])
    comparison = float(clean.iloc[-1 - lookback_bars])
    if comparison == 0:
        return None
    return {
        "current_sma": current,
        "comparison_sma": comparison,
        "slope_abs": current - comparison,
        "slope_pct": (current - comparison) / comparison * 100.0,
        "lookback_bars": int(lookback_bars),
    }


def close_location_value(
    high: float, low: float, close: float
) -> Optional[float]:
    """(close - low) / (high - low); None (unknown) for a zero/invalid range."""
    bar_range = float(high) - float(low)
    if bar_range <= 0:
        return None
    return (float(close) - float(low)) / bar_range


def volume_ratio_stats(
    df: pd.DataFrame, window_bars: int
) -> Optional[Dict[str, Any]]:
    """Current volume vs the COMPLETED rolling average (current bar excluded)."""
    if df is None or len(df) < window_bars + 1 or "volume" not in df.columns:
        return None
    volumes = df["volume"].astype(float)
    average = float(volumes.iloc[-window_bars - 1:-1].mean())
    if not np.isfinite(average) or average <= 0:
        return None
    current = float(volumes.iloc[-1])
    return {
        "current_volume": current,
        "average_volume": average,
        "window_bars": int(window_bars),
        "ratio": current / average,
    }


def bounce_recency_quality(
    age_bars: Optional[int], half_life_bars: int
) -> Optional[float]:
    """Exponential decay: 0.5 ** (age / half_life). Explicit and unoptimized."""
    if age_bars is None or half_life_bars <= 0:
        return None
    return _clamp01(0.5 ** (float(age_bars) / float(half_life_bars)))


# --------------------------------------------------------------------------- #
# Strategy
# --------------------------------------------------------------------------- #

class Sma150BounceV3Strategy(Strategy):
    pattern_code = PATTERN_CODE
    version = STRATEGY_VERSION
    decision_policy_version = DECISION_POLICY_VERSION
    required_timeframes = ["1d"]
    min_daily_bars = int(DEFAULT_CONFIG["min_history_bars"])

    def default_config(self) -> Dict[str, Any]:
        return {
            k: (dict(v) if isinstance(v, dict) else v)
            for k, v in DEFAULT_CONFIG.items()
        }

    def evaluate(self, df: pd.DataFrame, context: StrategyContext) -> StrategyResult:
        config = dict(self.default_config())
        if context.config:
            config.update(context.config)
        symbol = context.symbol

        cfg = _resolved_thresholds(config)
        items: List[EvidenceItem] = []
        missing_data: List[str] = []
        contradictions: List[str] = []
        as_of_iso: Optional[str] = None

        # ----- A0. Completed-bar gate (ny_session_close.v1) ---------------- #
        # A provider daily aggregate may represent the still-open session.
        # One safe deterministic exclusion is allowed (drop the uncertain
        # latest bar); if completion still cannot be proven, readiness is
        # unknown and the verdict is AVOID — a partial bar can never confirm
        # a trigger or count into the volume baseline.
        meta = context.data_meta or {}
        excluded_partial_bar_date: Optional[str] = None
        completion: Optional[Dict[str, Any]] = None
        if df is not None and len(df) > 0:
            completion = assess_latest_bar_completion(
                df,
                exchange_timezone=cfg["exchange_timezone"],
                session_close_time=cfg["session_close_time"],
                explicit_completed=meta.get("latest_bar_completed"),
                now_utc=meta.get("evaluation_time_utc"),
            )
            if completion["state"] == "partial":
                excluded_partial_bar_date = completion["bar_date"]
                df = df.iloc[:-1].reset_index(drop=True)
                completion = assess_latest_bar_completion(
                    df,
                    exchange_timezone=cfg["exchange_timezone"],
                    session_close_time=cfg["session_close_time"],
                    now_utc=meta.get("evaluation_time_utc"),
                )
            if completion["state"] != "completed":
                items.append(_completion_item(
                    completion, excluded_partial_bar_date,
                    reason_code="unconfirmed_bar_completion",
                ))
                missing_data.append("unconfirmed_bar_completion")
                return self._finalize(
                    symbol=symbol,
                    verdict="AVOID",
                    setup_state="unknown",
                    trigger_state="unknown",
                    rejection_reason="unconfirmed_bar_completion",
                    items=items,
                    missing_data=missing_data,
                    contradictions=contradictions,
                    cfg=cfg,
                    as_of_iso=None,
                    snapshot_date=_snapshot_date(df),
                    measurements={"bars_available": len(df)},
                    components={name: None for name in RANKING_COMPONENTS},
                )
            items.append(_completion_item(completion, excluded_partial_bar_date))

        bars_available = 0 if df is None else len(df)

        # ----- A. Data readiness ------------------------------------------ #
        data_ready = (
            df is not None
            and validate_dataframe(df, min_bars=cfg["min_history_bars"])
        )

        items.append(EvidenceItem(
            code="history_bars",
            category="data_readiness",
            source_type="market_data",
            state="pass" if data_ready else "fail",
            raw_value=bars_available,
            unit="bars",
            threshold=cfg["min_history_bars"],
            operator=">=",
            required=True,
            timeframe="1d",
            reason_code=None if data_ready else "insufficient_history",
        ))

        if not data_ready:
            for code in ("sma_available", "volume_average_available",
                         "completed_rebound_windows"):
                items.append(EvidenceItem(
                    code=code,
                    category="data_readiness",
                    source_type="market_data",
                    state="unknown",
                    required=True,
                    timeframe="1d",
                    reason_code="insufficient_history",
                ))
            missing_data.append("insufficient_history")
            return self._finalize(
                symbol=symbol,
                verdict="AVOID",
                setup_state="unknown",
                trigger_state="unknown",
                rejection_reason="insufficient_history",
                items=items,
                missing_data=missing_data,
                contradictions=contradictions,
                cfg=cfg,
                as_of_iso=as_of_iso,
                snapshot_date=_snapshot_date(df),
                measurements={"bars_available": bars_available},
                components={name: None for name in RANKING_COMPONENTS},
            )

        # Indicators (SMA only; volume average handled explicitly below).
        df_ind = df.copy()
        sma_col = f"sma_{cfg['sma_window']}"
        df_ind[sma_col] = sma(df_ind["close"], cfg["sma_window"])
        df_clean = df_ind.dropna(subset=[sma_col]).reset_index(drop=True)

        sma_ok = len(df_clean) >= cfg["slope_lookback_bars"] + 1
        items.append(EvidenceItem(
            code="sma_available",
            category="data_readiness",
            source_type="market_data",
            state="pass" if sma_ok else "fail",
            raw_value=len(df_clean),
            unit="bars",
            threshold=cfg["slope_lookback_bars"] + 1,
            operator=">=",
            required=True,
            timeframe="1d",
            reason_code=None if sma_ok else "insufficient_history",
        ))

        vol_stats = volume_ratio_stats(df_clean, cfg["volume_window_bars"])
        items.append(EvidenceItem(
            code="volume_average_available",
            category="data_readiness",
            source_type="market_data",
            state="pass" if vol_stats is not None else "fail",
            raw_value=None if vol_stats is None else vol_stats["average_volume"],
            unit="shares",
            threshold=cfg["volume_window_bars"],
            operator="window_bars",
            required=True,
            timeframe="1d",
            reason_code=None if vol_stats is not None else "insufficient_history",
        ))

        if not sma_ok or vol_stats is None:
            missing_data.append("insufficient_history")
            return self._finalize(
                symbol=symbol,
                verdict="AVOID",
                setup_state="unknown",
                trigger_state="unknown",
                rejection_reason="insufficient_history",
                items=items,
                missing_data=missing_data,
                contradictions=contradictions,
                cfg=cfg,
                as_of_iso=as_of_iso,
                snapshot_date=_snapshot_date(df),
                measurements={"bars_available": bars_available},
                components={name: None for name in RANKING_COMPONENTS},
            )

        current = df_clean.iloc[-1]
        current_index = len(df_clean) - 1
        current_price = float(current["close"])
        sma_value = float(current[sma_col])
        price_vs_sma_pct = (current_price - sma_value) / sma_value * 100.0
        as_of_iso = pd.to_datetime(current["date"]).isoformat()

        # Hard filter: minimum price (shared with the funnel prefilter).
        price_ok = current_price >= cfg["min_price"]
        items.append(EvidenceItem(
            code="min_price",
            category="hard_filter",
            source_type="market_data",
            state="pass" if price_ok else "fail",
            raw_value=round(current_price, 4),
            unit="usd",
            threshold=cfg["min_price"],
            operator=">=",
            required=True,
            timeframe="1d",
            as_of=as_of_iso,
            reason_code=None if price_ok else "price_below_min",
        ))

        # ----- B. Setup validity ------------------------------------------ #
        # B1. Current proximity to the SMA (asymmetric band).
        in_band_above = price_vs_sma_pct <= cfg["max_close_above_sma_pct"]
        in_band_below = price_vs_sma_pct >= -cfg["max_close_below_sma_pct"]
        proximity_ok = in_band_above and in_band_below
        if proximity_ok:
            proximity_reason = None
        elif not in_band_above:
            proximity_reason = "too_far_above_sma"
        else:
            proximity_reason = "too_far_below_sma"
        items.append(EvidenceItem(
            code="sma_proximity",
            category="setup",
            source_type="strategy",
            state="pass" if proximity_ok else "fail",
            raw_value=round(price_vs_sma_pct, 4),
            normalized_value=_proximity_quality(price_vs_sma_pct, cfg),
            unit="pct",
            threshold={
                "max_above_pct": cfg["max_close_above_sma_pct"],
                "max_below_pct": cfg["max_close_below_sma_pct"],
            },
            operator="between",
            required=True,
            timeframe="1d",
            as_of=as_of_iso,
            reason_code=proximity_reason,
        ))

        # B2. Independent historical bounce events (bounded lookback,
        # current bar excluded).
        hist_start = max(0, current_index - cfg["lookback_bars_for_history"])
        hist_df = df_clean.iloc[hist_start:current_index].reset_index(drop=True)
        events = find_independent_bounce_events(
            hist_df,
            sma_col,
            touch_tolerance_pct=cfg["touch_tolerance_pct"],
            rebound_window_bars=cfg["rebound_window_bars"],
            min_event_separation_bars=cfg["min_event_separation_bars"],
            current_index=len(hist_df),
        )
        # Report ages relative to the CURRENT bar of the full clean frame.
        offset = current_index - len(hist_df)
        for event in events:
            event["age_bars"] = int(event["age_bars"] + offset)
        bounce_count = len(events)
        bounces_ok = bounce_count >= cfg["min_independent_bounces"]

        items.append(EvidenceItem(
            code="completed_rebound_windows",
            category="data_readiness",
            source_type="market_data",
            state="pass",
            raw_value=bounce_count,
            unit="count",
            required=True,
            timeframe="1d",
            as_of=as_of_iso,
            metadata={"note": "events with incomplete rebound windows excluded"},
        ))
        items.append(EvidenceItem(
            code="independent_bounce_count",
            category="setup",
            source_type="strategy",
            state="pass" if bounces_ok else "fail",
            raw_value=bounce_count,
            normalized_value=_clamp01(
                bounce_count / cfg["bounce_quality_full_count"]
            ),
            unit="count",
            threshold=cfg["min_independent_bounces"],
            operator=">=",
            required=True,
            timeframe="1d",
            as_of=as_of_iso,
            reason_code=None if bounces_ok else "insufficient_independent_bounces",
            metadata={
                "effective_separation_bars": max(
                    cfg["min_event_separation_bars"],
                    cfg["rebound_window_bars"] + 1,
                ),
            },
        ))
        items.append(EvidenceItem(
            code="bounce_event_separation",
            category="setup",
            source_type="strategy",
            state="pass",
            raw_value=[e["touch_date"] for e in events],
            threshold=max(
                cfg["min_event_separation_bars"], cfg["rebound_window_bars"] + 1
            ),
            operator=">=",
            unit="bars",
            timeframe="1d",
            as_of=as_of_iso,
            metadata={"event_ages_bars": [e["age_bars"] for e in events]},
        ))

        # B3. Rebound quality: robust (median) gate; mean kept for analysis.
        rebounds = [e["max_rebound_pct"] for e in events]
        median_rebound = round(statistics.median(rebounds), 4) if rebounds else None
        mean_rebound = round(statistics.fmean(rebounds), 4) if rebounds else None
        rebound_ok = (
            median_rebound is not None
            and median_rebound >= cfg["min_median_rebound_pct"]
        )
        items.append(EvidenceItem(
            code="median_rebound",
            category="setup",
            source_type="strategy",
            state=(
                "unknown" if median_rebound is None
                else ("pass" if rebound_ok else "fail")
            ),
            raw_value=median_rebound,
            normalized_value=(
                None if median_rebound is None
                else _clamp01(
                    max(0.0, median_rebound) / cfg["rebound_quality_full_pct"]
                )
            ),
            unit="pct",
            threshold=cfg["min_median_rebound_pct"],
            operator=">=",
            required=True,
            timeframe="1d",
            as_of=as_of_iso,
            reason_code=None if rebound_ok else "weak_median_rebound",
        ))
        items.append(EvidenceItem(
            code="mean_rebound",
            category="setup",
            source_type="strategy",
            state="neutral" if mean_rebound is not None else "unknown",
            raw_value=mean_rebound,
            unit="pct",
            required=False,
            timeframe="1d",
            as_of=as_of_iso,
        ))

        setup_valid = price_ok and proximity_ok and bounces_ok and rebound_ok
        if not price_ok:
            setup_rejection = "price_below_min"
        elif not proximity_ok:
            setup_rejection = proximity_reason
        elif not bounces_ok:
            setup_rejection = "insufficient_independent_bounces"
        elif not rebound_ok:
            setup_rejection = "weak_median_rebound"
        else:
            setup_rejection = None

        # ----- C. Entry confirmation -------------------------------------- #
        # Confirmations are always measured (evidence stays visible) but can
        # never override an invalid setup.
        failed_confirmations: List[str] = []

        # C1. Close above SMA.
        close_above = current_price > sma_value
        items.append(EvidenceItem(
            code="close_above_sma",
            category="confirmation",
            source_type="strategy",
            state="pass" if close_above else "fail",
            raw_value=round(price_vs_sma_pct, 4),
            unit="pct",
            threshold=0.0,
            operator=">",
            required=True,
            timeframe="1d",
            as_of=as_of_iso,
            reason_code=None if close_above else "close_below_sma",
        ))
        if not close_above:
            failed_confirmations.append("close_above_sma")
            contradictions.append("close_below_sma")

        # C2. Positive SMA slope.
        slope = sma_slope_stats(df_clean[sma_col], cfg["slope_lookback_bars"])
        slope_pct = None if slope is None else round(slope["slope_pct"], 4)
        slope_ok = slope is not None and slope["slope_pct"] > cfg["min_sma_slope_pct"]
        items.append(EvidenceItem(
            code="sma_slope",
            category="confirmation",
            source_type="strategy",
            state=(
                "unknown" if slope is None else ("pass" if slope_ok else "fail")
            ),
            raw_value=slope_pct,
            normalized_value=(
                None if slope_pct is None else _trend_quality(slope_pct, cfg)
            ),
            unit="pct",
            threshold=cfg["min_sma_slope_pct"],
            operator=">",
            required=True,
            timeframe="1d",
            as_of=as_of_iso,
            reason_code=(
                "slope_unavailable" if slope is None
                else (None if slope_ok else "sma_slope_not_positive")
            ),
            metadata={} if slope is None else {
                "current_sma": round(slope["current_sma"], 4),
                "comparison_sma": round(slope["comparison_sma"], 4),
                "slope_abs": round(slope["slope_abs"], 4),
                "lookback_bars": slope["lookback_bars"],
            },
        ))
        if not slope_ok:
            failed_confirmations.append("sma_slope")
            if slope is None:
                missing_data.append("sma_slope")
            elif slope["slope_pct"] < 0:
                contradictions.append("sma_slope_negative")

        # C3. Bullish price trigger (three deterministic sub-conditions).
        prev_bar = df_clean.iloc[-2] if len(df_clean) >= 2 else None
        trigger_level = None if prev_bar is None else float(prev_bar["high"])
        breakout_ok = trigger_level is not None and current_price > trigger_level
        bullish_close_ok = current_price > float(current["open"])
        clv = close_location_value(
            float(current["high"]), float(current["low"]), current_price
        )
        clv_ok = clv is not None and clv >= cfg["min_close_location_value"]
        trigger_ok = breakout_ok and bullish_close_ok and clv_ok

        items.append(EvidenceItem(
            code="close_above_prior_high",
            category="confirmation",
            source_type="strategy",
            state=(
                "unknown" if trigger_level is None
                else ("pass" if breakout_ok else "fail")
            ),
            raw_value=round(current_price, 4),
            unit="usd",
            threshold=None if trigger_level is None else round(trigger_level, 4),
            operator=">",
            required=True,
            timeframe="1d",
            as_of=as_of_iso,
            reason_code=(
                "no_prior_bar" if trigger_level is None
                else (None if breakout_ok else "no_prior_high_breakout")
            ),
        ))
        items.append(EvidenceItem(
            code="bullish_close",
            category="confirmation",
            source_type="strategy",
            state="pass" if bullish_close_ok else "fail",
            raw_value=round(current_price - float(current["open"]), 4),
            unit="usd",
            threshold=0.0,
            operator=">",
            required=True,
            timeframe="1d",
            as_of=as_of_iso,
            reason_code=None if bullish_close_ok else "bearish_close",
        ))
        items.append(EvidenceItem(
            code="close_location",
            category="confirmation",
            source_type="strategy",
            state=(
                "unknown" if clv is None else ("pass" if clv_ok else "fail")
            ),
            raw_value=None if clv is None else round(clv, 4),
            normalized_value=(
                None if clv is None
                else _clamp01(clv / cfg["min_close_location_value"])
            ),
            unit="ratio",
            threshold=cfg["min_close_location_value"],
            operator=">=",
            required=True,
            timeframe="1d",
            as_of=as_of_iso,
            reason_code=(
                "zero_range_bar" if clv is None
                else (None if clv_ok else "weak_close_location")
            ),
        ))
        if not trigger_ok:
            failed_confirmations.append("bullish_trigger")
            if not bullish_close_ok:
                contradictions.append("bearish_close")
            if clv is None:
                missing_data.append("close_location")

        # C4. Volume confirmation (vol_stats is non-None past readiness).
        volume_ratio = round(vol_stats["ratio"], 4)
        volume_ok = volume_ratio >= cfg["min_trigger_volume_ratio"]
        volume_quality = _clamp01(
            max(0.0, volume_ratio) / cfg["min_trigger_volume_ratio"]
        )
        items.append(EvidenceItem(
            code="trigger_volume_ratio",
            category="confirmation",
            source_type="strategy",
            state="pass" if volume_ok else "fail",
            raw_value=volume_ratio,
            normalized_value=volume_quality,
            unit="ratio",
            threshold=cfg["min_trigger_volume_ratio"],
            operator=">=",
            required=True,
            timeframe="1d",
            as_of=as_of_iso,
            reason_code=None if volume_ok else "weak_trigger_volume",
            metadata={
                "average_volume": round(vol_stats["average_volume"], 2),
                "current_volume": round(vol_stats["current_volume"], 2),
                "window_bars": vol_stats["window_bars"],
            },
        ))
        if not volume_ok:
            failed_confirmations.append("trigger_volume_ratio")

        # Completed-bar audit trail: which bar confirmed the trigger, and
        # where the completed volume baseline ends (it EXCLUDES the trigger
        # bar by construction — see volume_ratio_stats).
        trigger_bar_date = pd.to_datetime(current["date"]).date().isoformat()
        volume_baseline_end_date = (
            pd.to_datetime(df_clean.iloc[-2]["date"]).date().isoformat()
            if len(df_clean) >= 2 else None
        )
        items.append(EvidenceItem(
            code="trigger_bar_date",
            category="confirmation",
            source_type="market_data",
            state="neutral",
            raw_value=trigger_bar_date,
            unit="date",
            timeframe="1d",
            as_of=as_of_iso,
            metadata={"completion_policy": cfg["bar_completion_policy"]},
        ))
        items.append(EvidenceItem(
            code="volume_baseline_end_date",
            category="confirmation",
            source_type="market_data",
            state="neutral",
            raw_value=volume_baseline_end_date,
            unit="date",
            timeframe="1d",
            as_of=as_of_iso,
            metadata={
                "window_bars": vol_stats["window_bars"],
                "excludes_trigger_bar": True,
            },
        ))

        confirmations_pass = (
            close_above and slope_ok and trigger_ok and volume_ok
        )

        # ----- Risk / invalidation ---------------------------------------- #
        invalidation_level = round(
            sma_value * (1.0 - cfg["invalidation_below_sma_pct"] / 100.0), 4
        )
        items.append(EvidenceItem(
            code="invalidation_level",
            category="risk",
            source_type="risk",
            state="neutral",
            raw_value=invalidation_level,
            unit="usd",
            threshold=cfg["invalidation_below_sma_pct"],
            operator="daily_close_below",
            timeframe="1d",
            as_of=as_of_iso,
            metadata={"rule_code": INVALIDATION_RULE_CODE},
        ))
        invalidation_distance_pct = round(
            (current_price - invalidation_level) / current_price * 100.0, 4
        )
        items.append(EvidenceItem(
            code="invalidation_distance",
            category="risk",
            source_type="risk",
            state="neutral",
            raw_value=invalidation_distance_pct,
            unit="pct",
            timeframe="1d",
            as_of=as_of_iso,
        ))

        # ----- D. Ranking (ordering only; never a gate) -------------------- #
        most_recent_age = min((e["age_bars"] for e in events), default=None)
        components: Dict[str, Optional[float]] = {
            "proximity_quality": _proximity_quality(price_vs_sma_pct, cfg),
            "trend_quality": (
                None if slope_pct is None else _trend_quality(slope_pct, cfg)
            ),
            "independent_bounce_quality": _clamp01(
                bounce_count / cfg["bounce_quality_full_count"]
            ),
            "rebound_quality": (
                None if median_rebound is None
                else _clamp01(
                    max(0.0, median_rebound) / cfg["rebound_quality_full_pct"]
                )
            ),
            "volume_quality": volume_quality,
            "trigger_quality": _trigger_quality(
                breakout_ok if trigger_level is not None else None,
                bullish_close_ok,
                clv,
                cfg["min_close_location_value"],
            ),
            "bounce_recency_quality": bounce_recency_quality(
                most_recent_age, cfg["recency_half_life_bars"]
            ),
        }
        for name, value in components.items():
            items.append(EvidenceItem(
                code=name,
                category="ranking",
                source_type="strategy",
                state="neutral" if value is not None else "unknown",
                raw_value=value,
                normalized_value=value,
                unit="quality",
                timeframe="1d",
                as_of=as_of_iso,
                reason_code=None if value is not None else "not_computable",
            ))
        # Honest score: an unweighted arithmetic mean over the FULL fixed set,
        # NULL if any component is unknown. Ordering only — never probability.
        if all(components[name] is not None for name in RANKING_COMPONENTS):
            ranking_score = round(
                sum(components[name] for name in RANKING_COMPONENTS)
                / len(RANKING_COMPONENTS),
                6,
            )
        else:
            ranking_score = None
        items.append(EvidenceItem(
            code="ranking_score",
            category="ranking",
            source_type="strategy",
            state="neutral" if ranking_score is not None else "unknown",
            raw_value=ranking_score,
            normalized_value=ranking_score,
            unit="quality",
            timeframe="1d",
            as_of=as_of_iso,
            metadata={"ranking_version": RANKING_VERSION,
                      "aggregation": "arithmetic_mean"},
        ))

        # ----- Decision (layer authority: A > B > C; D never decides) ------ #
        if not setup_valid:
            verdict = "AVOID"
            setup_state = "invalid"
        elif confirmations_pass:
            verdict = "ENTER"
            setup_state = "valid"
        else:
            verdict = "WATCH"
            setup_state = "valid"

        if confirmations_pass:
            trigger_state = "confirmed"
        elif contradictions:
            trigger_state = "contradicted"
        else:
            trigger_state = "missing"

        measurements = {
            "bars_available": bars_available,
            "current_price": round(current_price, 4),
            "sma_value": round(sma_value, 4),
            "price_vs_sma_pct": round(price_vs_sma_pct, 4),
            "proximity_abs_pct": round(abs(price_vs_sma_pct), 4),
            "sma_slope_pct": slope_pct,
            "independent_bounce_count": bounce_count,
            "median_rebound_pct": median_rebound,
            "mean_rebound_pct": mean_rebound,
            "volume_ratio": volume_ratio,
            "close_location_value": None if clv is None else round(clv, 4),
            "trigger_level": (
                None if trigger_level is None else round(trigger_level, 4)
            ),
            "most_recent_bounce_age_bars": most_recent_age,
            "bounce_events": events,
            "invalidation_level": invalidation_level,
            "invalidation_distance_pct": invalidation_distance_pct,
            "failed_confirmations": list(failed_confirmations),
        }

        return self._finalize(
            symbol=symbol,
            verdict=verdict,
            setup_state=setup_state,
            trigger_state=trigger_state,
            rejection_reason=setup_rejection if verdict == "AVOID" else None,
            items=items,
            missing_data=missing_data,
            contradictions=contradictions,
            cfg=cfg,
            as_of_iso=as_of_iso,
            snapshot_date=_snapshot_date(df_clean),
            measurements=measurements,
            components=components,
            ranking_score=ranking_score,
        )

    # ------------------------------------------------------------------ #

    def _finalize(
        self,
        *,
        symbol: str,
        verdict: str,
        setup_state: str,
        trigger_state: str,
        rejection_reason: Optional[str],
        items: List[EvidenceItem],
        missing_data: List[str],
        contradictions: List[str],
        cfg: Dict[str, Any],
        as_of_iso: Optional[str],
        snapshot_date: str,
        measurements: Dict[str, Any],
        components: Dict[str, Optional[float]],
        ranking_score: Optional[float] = None,
    ) -> StrategyResult:
        """Assemble the evidence bundle, details, and StrategyResult."""
        hard_filter_summary = {
            item.code: item.state
            for item in items
            if item.category in ("data_readiness", "hard_filter") and item.required
        }
        bundle = EvidenceBundle(
            strategy_code=self.pattern_code,
            strategy_version=self.version,
            decision_policy_version=self.decision_policy_version,
            symbol=symbol,
            verdict=verdict,
            setup_state=setup_state,
            trigger_state=trigger_state,
            market_data_as_of=as_of_iso,
            items=items,
            hard_filter_summary=hard_filter_summary,
            missing_data=list(dict.fromkeys(missing_data)),
            contradictions=list(dict.fromkeys(contradictions)),
            timeframe_summary={"required_timeframes": list(self.required_timeframes)},
            ranking_components=dict(components),
            ranking_score=ranking_score,
        )

        failed_confirmations = measurements.get("failed_confirmations", [])
        # RAW measured values only (Phase 1 rule B2 still holds in v3).
        score_components = {
            k: measurements.get(k)
            for k in (
                "price_vs_sma_pct",
                "proximity_abs_pct",
                "sma_slope_pct",
                "independent_bounce_count",
                "median_rebound_pct",
                "mean_rebound_pct",
                "volume_ratio",
                "close_location_value",
                "trigger_level",
                "most_recent_bounce_age_bars",
            )
            if k in measurements
        }

        reason_parts = [f"setup {setup_state}", f"trigger {trigger_state}"]
        if rejection_reason:
            reason_parts.insert(0, rejection_reason)
        if failed_confirmations:
            reason_parts.append(
                "missing confirmations: " + ", ".join(sorted(failed_confirmations))
            )
        reason = "; ".join(reason_parts)

        details: Dict[str, Any] = {
            "symbol": symbol,
            "snapshot_date": snapshot_date,
            # ISO timestamp of the latest COMPLETED bar actually evaluated
            # (a truncated partial bar never appears here). The provenance
            # builders prefer this over the raw dataframe's last bar.
            "market_data_as_of": as_of_iso,
            "score_version": self.version,
            "decision_policy_version": self.decision_policy_version,
            "evidence_version": EVIDENCE_VERSION,
            "setup_state": setup_state,
            "trigger_state": trigger_state,
            "rejection_reason": rejection_reason,
            "thresholds_used": dict(cfg),
            "score_components": score_components,
            "failed_confirmations": list(failed_confirmations),
            "contradictions": list(dict.fromkeys(contradictions)),
            "invalidation": {
                "rule_code": INVALIDATION_RULE_CODE,
                "threshold_pct": cfg["invalidation_below_sma_pct"],
                "level": measurements.get("invalidation_level"),
            },
            "ranking": {
                "ranking_version": RANKING_VERSION,
                "components": dict(components),
                "score": ranking_score,
            },
            "bounce_events": measurements.get("bounce_events", []),
            "evidence": bundle.to_dict(),
        }
        # Additive UI-friendly keys (same names the v2 drawer reads).
        if "current_price" in measurements:
            details.update({
                "current_price": measurements["current_price"],
                "sma_value": measurements["sma_value"],
                "proximity_pct": measurements["proximity_abs_pct"],
                "sma_slope_pct": measurements["sma_slope_pct"],
                "bounce_count": measurements["independent_bounce_count"],
                "median_rebound_pct": measurements["median_rebound_pct"],
                "avg_rebound_pct": measurements["mean_rebound_pct"],
                "vol_ratio": measurements["volume_ratio"],
                "trigger_level": measurements["trigger_level"],
            })

        decision = {
            "ENTER": StrategyDecision.ENTER,
            "WATCH": StrategyDecision.WATCH,
            "AVOID": StrategyDecision.AVOID,
        }[verdict]

        return StrategyResult(
            decision=decision,
            symbol=symbol,
            pattern_code=self.pattern_code,
            score=ranking_score,
            side=StrategySide.LONG,
            reason=reason,
            rejection_reason=rejection_reason,
            details=details,
            score_components=score_components,
            required_timeframes=list(self.required_timeframes),
            entry_price=None,
            stop_price=None,
            target_price=None,
            invalidation=measurements.get("invalidation_level"),
            setup_type="sma150_bounce_v3",
            strategy_version=self.version,
        )


# --------------------------------------------------------------------------- #
# Normalization formulas (explicit + unit-tested; ordering only)
# --------------------------------------------------------------------------- #

def _proximity_quality(price_vs_sma_pct: float, cfg: Dict[str, Any]) -> float:
    """1.0 at the SMA, decaying linearly to 0.0 at each band edge; 0 outside."""
    if price_vs_sma_pct >= 0:
        scale = cfg["max_close_above_sma_pct"]
        return _clamp01(1.0 - price_vs_sma_pct / scale) if scale > 0 else 0.0
    scale = cfg["max_close_below_sma_pct"]
    return _clamp01(1.0 - (-price_vs_sma_pct) / scale) if scale > 0 else 0.0


def _trend_quality(slope_pct: float, cfg: Dict[str, Any]) -> float:
    """0 for flat/negative slope, linear to 1.0 at the full-scale slope."""
    full_scale = cfg["trend_quality_full_scale_slope_pct"]
    if slope_pct <= 0 or full_scale <= 0:
        return 0.0
    return _clamp01(slope_pct / full_scale)


def _trigger_quality(
    breakout_ok: Optional[bool],
    bullish_close_ok: bool,
    clv: Optional[float],
    min_clv: float,
) -> Optional[float]:
    """Mean of the three trigger sub-conditions; continuous via CLV.

    None (unknown) when the breakout condition cannot be evaluated at all.
    A missing CLV (zero-range bar) contributes 0, never a fabricated pass.
    """
    if breakout_ok is None:
        return None
    clv_part = 0.0 if clv is None or min_clv <= 0 else _clamp01(clv / min_clv)
    return _clamp01(
        ((1.0 if breakout_ok else 0.0)
         + (1.0 if bullish_close_ok else 0.0)
         + clv_part) / 3.0
    )


def _resolved_thresholds(config: Dict[str, Any]) -> Dict[str, Any]:
    """Typed view of every decision-relevant config value (persisted)."""
    return {
        "sma_window": int(config["sma_window"]),
        "min_history_bars": int(config["min_history_bars"]),
        "lookback_bars_for_history": int(config["lookback_bars_for_history"]),
        "volume_window_bars": int(config["volume_window_bars"]),
        "slope_lookback_bars": int(config["slope_lookback_bars"]),
        "rebound_window_bars": int(config["rebound_window_bars"]),
        "max_close_above_sma_pct": float(config["max_close_above_sma_pct"]),
        "max_close_below_sma_pct": float(config["max_close_below_sma_pct"]),
        "touch_tolerance_pct": float(config["touch_tolerance_pct"]),
        "min_event_separation_bars": int(config["min_event_separation_bars"]),
        "min_independent_bounces": int(config["min_independent_bounces"]),
        "min_median_rebound_pct": float(config["min_median_rebound_pct"]),
        "min_sma_slope_pct": float(config["min_sma_slope_pct"]),
        "min_close_location_value": float(config["min_close_location_value"]),
        "min_trigger_volume_ratio": float(config["min_trigger_volume_ratio"]),
        "invalidation_below_sma_pct": float(config["invalidation_below_sma_pct"]),
        "recency_half_life_bars": int(config["recency_half_life_bars"]),
        "trend_quality_full_scale_slope_pct": float(
            config["trend_quality_full_scale_slope_pct"]
        ),
        "bounce_quality_full_count": int(config["bounce_quality_full_count"]),
        "rebound_quality_full_pct": float(config["rebound_quality_full_pct"]),
        "bar_completion_policy": str(
            config.get("bar_completion_policy", BAR_COMPLETION_POLICY)
        ),
        "exchange_timezone": str(
            config.get("exchange_timezone", "America/New_York")
        ),
        "session_close_time": str(config.get("session_close_time", "16:00")),
        "min_price": float(config.get("min_price", 0.0)),
    }


def _snapshot_date(df: Optional[pd.DataFrame]) -> str:
    """ISO date of the latest evaluated bar; falls back to today only when the
    frame has no usable date (readiness AVOIDs on tiny/empty frames)."""
    try:
        if df is not None and len(df) > 0 and "date" in df.columns:
            return pd.to_datetime(df.iloc[-1]["date"]).date().isoformat()
    except Exception:
        pass
    from datetime import date as _date
    return _date.today().isoformat()

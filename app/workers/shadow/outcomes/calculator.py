"""PURE market-path outcome math for frozen shadow pairs (Phase 8.1B2).

No DB or network I/O anywhere in this module. The service layer fetches the
bounded forward range and passes plain payloads in.

Contracts enforced here:

  * The reference price comes EXCLUSIVELY from the frozen B1 pair — the
    close of the last bar of strategy_shadow_pairs.frame_snapshot. Never
    from the current provider response, created_at, market_data_as_of alone,
    a later WATCH trigger, a reconstructed evaluation or a hypothetical
    entry. The role is verdict-neutral: 'paired_decision_observation'.
  * Forward bars are COMPLETED trading bars STRICTLY after snapshot_date in
    trading-bar order (weekends/holidays never counted, no nearest-date
    substitution, no zero filling). 1D is the FIRST completed forward bar.
    The ny_session_close.v1 completion decision is applied BEFORE counting.
  * The provider bar ON snapshot_date is used only as a continuity/revision
    check against the frozen close — it is never part of the forward
    sequence and never replaces the frozen reference.
  * Return math is outcome.v1 reused verbatim where semantics are identical:
    HOLDING_WINDOWS, compute_forward_returns (LONG as raw upward-market
    math), compute_mfe_mae, benchmark helpers. No stop/target/R/trade-side
    semantics and no same-ticker buy-and-hold (tautologically identical to
    the pair return from the same frozen close).
"""

import math
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from app.workers.outcomes.baselines import compute_benchmark_returns
from app.workers.outcomes.calculator import (
    HOLDING_WINDOWS,
    LONG,
    MAX_WINDOW,
    compute_forward_returns,
    compute_mfe_mae,
    window_label,
)
from app.workers.shadow.frames import FrameRejection, _canonical_bar
from app.workers.shadow.outcomes.constants import (
    REFERENCE_ABS_TOL,
    REFERENCE_REL_TOL,
    STATUS_COMPLETE,
    STATUS_PARTIAL,
    STATUS_PENDING,
)
from app.workers.strategies.sma150_v3 import assess_latest_bar_completion


class ShadowOutcomeRejection(ValueError):
    """A pair whose outcome cannot be calculated trustworthily.

    Deterministic bounded reason code — never a raw payload.
    """

    def __init__(self, reason_code: str, detail: Optional[str] = None):
        self.reason_code = reason_code
        self.detail = detail
        super().__init__(f"{reason_code}" + (f": {detail}" if detail else ""))


def _as_iso_date(value: Any, field: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            raise ShadowOutcomeRejection("invalid_date", field)
    raise ShadowOutcomeRejection("invalid_date", field)


# --------------------------------------------------------------------------- #
# Reference price (frozen B1 frame only)
# --------------------------------------------------------------------------- #

def resolve_reference_price(
    *,
    frame_last_bar: Optional[Dict[str, Any]],
    frame_bar_count: Any,
    snapshot_date: Any,
    frame_last_date: Any,
) -> float:
    """The frozen reference: close of the LAST bar of the B1 frame_snapshot.

    Validations (all reject deterministically, never repair):
      * the frame is non-empty;
      * the last frame bar date equals snapshot_date;
      * the last frame bar date equals frame_last_date;
      * the frozen close is finite and positive.
    """
    try:
        bar_count = int(frame_bar_count)
    except (TypeError, ValueError):
        raise ShadowOutcomeRejection("invalid_frame_bar_count", "frame_bar_count")
    if frame_last_bar is None or bar_count <= 0:
        raise ShadowOutcomeRejection("empty_frozen_frame")
    if not isinstance(frame_last_bar, dict) or "date" not in frame_last_bar:
        raise ShadowOutcomeRejection("malformed_frozen_bar")

    last_bar_date = _as_iso_date(frame_last_bar["date"], "frame_last_bar.date")
    snap = _as_iso_date(snapshot_date, "snapshot_date")
    frame_last = _as_iso_date(frame_last_date, "frame_last_date")
    if last_bar_date != snap:
        raise ShadowOutcomeRejection(
            "frame_snapshot_date_mismatch",
            f"last_bar={last_bar_date.isoformat()} snapshot={snap.isoformat()}",
        )
    if last_bar_date != frame_last:
        raise ShadowOutcomeRejection(
            "frame_last_date_mismatch",
            f"last_bar={last_bar_date.isoformat()} frame_last={frame_last.isoformat()}",
        )

    close = frame_last_bar.get("close")
    if isinstance(close, bool) or not isinstance(close, (int, float)):
        raise ShadowOutcomeRejection("invalid_frozen_close", "non_numeric")
    close = float(close)
    if not math.isfinite(close) or close <= 0:
        raise ShadowOutcomeRejection("invalid_frozen_close", "non_finite_or_non_positive")
    return close


def check_reference_revision(
    frozen_close: float,
    snapshot_bar: Optional[Dict[str, Any]],
    *,
    provider: Optional[str] = None,
) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """Continuity check of the re-fetched snapshot-date bar's close.

    Returns (revision_detected, bounded_note). The fetched close NEVER
    replaces the frozen close — a divergence beyond numeric tolerance is
    recorded, not repaired. A missing snapshot-date bar is not a revision
    (providers may trim history); it simply cannot confirm continuity.
    """
    if snapshot_bar is None:
        return False, None
    fetched_close = snapshot_bar.get("close")
    if isinstance(fetched_close, bool) or not isinstance(fetched_close, (int, float)):
        return False, None
    fetched_close = float(fetched_close)
    if math.isclose(
        fetched_close,
        frozen_close,
        rel_tol=REFERENCE_REL_TOL,
        abs_tol=REFERENCE_ABS_TOL,
    ):
        return False, None
    note = {
        "reason_code": "reference_close_revision",
        "existing_value": frozen_close,
        "observed_value": fetched_close,
        "provider": provider,
    }
    return True, note


# --------------------------------------------------------------------------- #
# Canonical forward sequence (completed bars strictly after snapshot_date)
# --------------------------------------------------------------------------- #

def build_forward_sequence(
    historical: Optional[List[Dict[str, Any]]],
    snapshot_date: Any,
    *,
    now_utc: Optional[datetime] = None,
    explicit_completed: Optional[bool] = None,
    max_window: int = MAX_WINDOW,
) -> Dict[str, Any]:
    """Normalize a fetched daily range into the canonical forward sequence.

    Returns {"forward_bars": [...], "snapshot_bar": bar|None,
             "completion": record}. forward_bars is normalized,
    chronological, COMPLETED only, strictly after snapshot_date and capped
    to `max_window` (20). The snapshot-date bar (if fetched) is returned
    separately for the continuity check only.

    Deterministic rejections (never guessed around):
      * malformed / non-finite / non-positive OHLCV;
      * duplicate session dates;
      * unknown or future-dated latest-bar completion.

    A partial current-session bar is EXCLUDED (never counted); 0 remaining
    forward bars is NOT a rejection — horizons simply stay NULL.
    """
    snap = _as_iso_date(snapshot_date, "snapshot_date")

    try:
        bars = [_canonical_bar(raw) for raw in (historical or [])]
    except FrameRejection as rejection:
        raise ShadowOutcomeRejection(rejection.reason_code, rejection.detail)
    bars.sort(key=lambda b: b["date"])

    seen: set = set()
    for bar in bars:
        if bar["date"] in seen:
            raise ShadowOutcomeRejection("duplicate_session_date", bar["date"])
        seen.add(bar["date"])

    completion_record: Dict[str, Any] = {
        "state": "no_bars",
        "reason": None,
        "excluded_partial_bar_date": None,
    }

    if bars:
        # ONE ny_session_close.v1 completion decision on the LATEST fetched
        # bar, BEFORE counting forward bars. Exclusion can only remove the
        # single possibly-open session bar; completed history is untouched.
        df = pd.DataFrame(bars)
        df["date"] = pd.to_datetime(df["date"])
        completion = assess_latest_bar_completion(
            df, explicit_completed=explicit_completed, now_utc=now_utc
        )
        excluded: Optional[str] = None
        if completion["state"] == "partial":
            excluded = completion["bar_date"]
            bars = bars[:-1]
            if bars:
                df = pd.DataFrame(bars)
                df["date"] = pd.to_datetime(df["date"])
                completion = assess_latest_bar_completion(df, now_utc=now_utc)
        if bars and completion["state"] != "completed":
            # Unknown / future-dated completion: refuse honestly, never guess.
            raise ShadowOutcomeRejection(
                "unconfirmed_bar_completion", completion.get("reason")
            )
        completion_record = {
            "state": completion["state"] if bars else "no_completed_bars",
            "reason": completion.get("reason"),
            "excluded_partial_bar_date": excluded,
        }

    snapshot_bar: Optional[Dict[str, Any]] = None
    forward: List[Dict[str, Any]] = []
    for bar in bars:
        bar_date = date.fromisoformat(bar["date"])
        if bar_date == snap:
            snapshot_bar = bar
        elif bar_date > snap:
            forward.append(bar)
        # bars before snapshot_date are irrelevant here (never counted).

    return {
        "forward_bars": forward[:max_window],
        "snapshot_bar": snapshot_bar,
        "completion": completion_record,
    }


def status_for_bar_count(available_forward_bars: int) -> str:
    """Maturation state from the completed forward bar count."""
    if available_forward_bars <= 0:
        return STATUS_PENDING
    if available_forward_bars >= MAX_WINDOW:
        return STATUS_COMPLETE
    return STATUS_PARTIAL


# --------------------------------------------------------------------------- #
# Outcome values (raw market-path returns; PERCENT)
# --------------------------------------------------------------------------- #

def compute_outcome_values(
    reference_price: float,
    forward_bars: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Raw upward-market returns + excursions from the frozen reference.

    LONG is used strictly as raw upward-market-return math (there is no
    trade and no side). Incomplete horizons stay None. MFE/MAE cover the
    AVAILABLE completed bars and the bar count is always reported next to
    them.
    """
    closes = [b["close"] for b in forward_bars]
    highs = [b["high"] for b in forward_bars]
    lows = [b["low"] for b in forward_bars]

    ret_by_window = compute_forward_returns(reference_price, closes, LONG)
    mfe, mae = compute_mfe_mae(reference_price, highs, lows, LONG)
    bar_count = min(MAX_WINDOW, len(forward_bars))

    return {
        "ret_by_window": ret_by_window,
        "max_favorable_excursion": mfe,
        "max_adverse_excursion": mae,
        "mfe_mae_bar_count": bar_count if forward_bars else None,
        "available_forward_bars": len(forward_bars),
        "first_forward_date": forward_bars[0]["date"] if forward_bars else None,
        "last_forward_date": forward_bars[-1]["date"] if forward_bars else None,
    }


def compute_benchmark_returns_for_pair(
    benchmark_sequences: Dict[str, Optional[Dict[str, Any]]],
) -> Dict[str, Dict[str, Optional[float]]]:
    """Deterministic {"SPY": {"1D": num|None, ...}, "QQQ": {...}} map.

    `benchmark_sequences` maps benchmark symbol -> the build_forward_sequence
    result for that benchmark (or None when the fetch failed). The benchmark
    reference close comes from the BENCHMARK's own snapshot-date bar (never
    from the pair); a missing snapshot-date bar or missing data leaves that
    benchmark's horizons None — values are never fabricated.
    """
    out: Dict[str, Dict[str, Optional[float]]] = {}
    for name, sequence in benchmark_sequences.items():
        if sequence is None:
            out[name] = {window_label(w): None for w in HOLDING_WINDOWS}
            continue
        snapshot_bar = sequence.get("snapshot_bar")
        reference = snapshot_bar.get("close") if snapshot_bar else None
        forward_closes = [b["close"] for b in sequence.get("forward_bars") or []]
        out[name] = compute_benchmark_returns(reference, forward_closes)
    return out


def relative_return(
    pair_return: Optional[float],
    benchmark_return: Optional[float],
) -> Optional[float]:
    """pair_return - benchmark_return; None when either side is missing.

    Computed at read/metrics time — never persisted, never fabricated.
    """
    if pair_return is None or benchmark_return is None:
        return None
    return pair_return - benchmark_return

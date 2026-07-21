"""Canonical shared market frame for paired shadow evaluations (8.1B1).

The two arms must never evaluate different data. For each symbol the runner
fetches history ONCE, this module normalizes it into ONE canonical completed
daily frame, and both strategies then evaluate independent copies of that
exact frame.

Rules (all deterministic, all honest):
  * dates normalized to ISO, OHLCV coerced to finite floats;
  * chronological order; duplicate session dates reject the symbol; raw
    provider ordering (ascending, descending, shuffled) is irrelevant — the
    canonical frame and its hash depend only on the canonical bars;
  * malformed / non-finite / non-positive OHLCV rejects the symbol;
  * history depth is DERIVED from both arms' resolved configs
    (required_history_bars_v2/_v3 — the SMA warm-up counts: a bar can only
    join the bounce lookback once its SMA is valid), capped at
    FRAME_HARD_CAP_BARS; the frame keeps the most recent `max_bars`
    COMPLETED bars BEFORE hashing. A provider returning less history is
    recorded honestly by the runner, never fabricated and never rejected
    here — the strategies' own readiness rules decide;
  * ONE completed-bar decision (ny_session_close.v1 — the same policy
    sma150.v3 enforces internally) is applied BEFORE either arm evaluates:
    a current partial session bar is excluded for BOTH arms (before the
    depth cap, so exclusion never shrinks an otherwise-full frame); unknown
    or future-dated completion rejects the pair honestly;
  * the frame hash is computed over the complete canonical frame that both
    arms actually receive;
  * market_data_as_of equals the last canonical COMPLETED bar.
"""

import math
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd

from app.workers.provenance import canonical_json, _sha256
from app.workers.shadow.constants import (
    FRAME_HARD_CAP_BARS,
    FRAME_SNAPSHOT_VERSION,
    MAX_FRAME_SNAPSHOT_BYTES,
    TIMEFRAME,
)
# Reuse the exact completed-bar policy implementation sma150.v3 already uses
# (ny_session_close.v1) instead of duplicating session/timezone rules.
from app.workers.strategies.sma150_v3 import (
    BAR_COMPLETION_POLICY,
    assess_latest_bar_completion,
)


# --------------------------------------------------------------------------- #
# History-depth contract (derived from the RESOLVED arm configs, never
# hard-coded; the SMA warm-up is counted explicitly)
# --------------------------------------------------------------------------- #

def required_history_bars_v3(config: Dict[str, Any]) -> int:
    """Completed bars sma150.v3 needs for its FULL configured lookback.

    Historical bounce events use SMA-valid bars only, so the full lookback
    window needs the SMA warm-up in front of it:

        (sma_window - 1)               SMA warm-up (first valid SMA bar)
      + lookback_bars_for_history      fully SMA-valid historical bars
      + 1                              the current evaluated bar
      = 149 + 365 + 1 = 515 with defaults.

    The other decision-relevant windows (min_history_bars readiness gate,
    slope over SMA values, the volume average, rebound windows inside the
    lookback) are all smaller with any sane config, but each is accounted
    for and the maximum wins.
    """
    sma_window = int(config["sma_window"])
    warmup = sma_window - 1
    return max(
        warmup + int(config["lookback_bars_for_history"]) + 1,
        int(config["min_history_bars"]),
        # SMA slope needs slope_lookback_bars + 1 SMA-valid bars.
        warmup + int(config["slope_lookback_bars"]) + 1,
        # Volume ratio needs its window of SMA-valid bars.
        warmup + int(config["volume_window_bars"]) + 1,
        # A rebound window must fit inside the SMA-valid history.
        warmup + int(config["rebound_window_bars"]) + 1,
    )


def required_history_bars_v2(config: Dict[str, Any]) -> int:
    """Completed bars sma150.v2 needs for its FULL configured lookback.

    evaluate_sma150_bounce slices its historical window as the last
    `lookback_days_for_history` SMA-VALID bars before the current bar
    (despite the 'days' name it is a bar-count offset), so the same warm-up
    arithmetic applies. Its own hard readiness gate is sma_window + 50.
    """
    sma_window = int(config["sma_window"])
    warmup = sma_window - 1
    return max(
        warmup + int(config["lookback_days_for_history"]) + 1,
        sma_window + 50,   # validate_dataframe gate inside the evaluator
        warmup + int(config["rebound_window_days"]) + 1,
    )


def desired_history_bars(
    control_config: Dict[str, Any],
    candidate_config: Dict[str, Any],
) -> int:
    """The UNCAPPED shared requirement: the larger of the two arms.

    Both arms must see the same frame, so the desired depth is the MAXIMUM
    of the two configured requirements. This value is preserved verbatim in
    telemetry even when the hard cap prevents actually requesting/storing
    it — depth completeness is always judged against THIS number, so a
    600-bar cap filled to the brim can never masquerade as a complete
    800-bar lookback.
    """
    return max(
        required_history_bars_v2(control_config),
        required_history_bars_v3(candidate_config),
    )


def shared_required_history_bars(
    control_config: Dict[str, Any],
    candidate_config: Dict[str, Any],
) -> int:
    """The EFFECTIVE canonical depth: the desired requirement, hard-capped.

    This is what is requested from the provider and stored — bounded by the
    documented FRAME_HARD_CAP_BARS ceiling (a runaway config can never
    create an unbounded snapshot). The uncapped desired value is reported
    separately by desired_history_bars.
    """
    return min(
        desired_history_bars(control_config, candidate_config),
        FRAME_HARD_CAP_BARS,
    )


class FrameRejection(ValueError):
    """A symbol whose data cannot form a trustworthy canonical frame.

    Carries a deterministic bounded reason code — never a raw payload.
    """

    def __init__(self, reason_code: str, detail: Optional[str] = None):
        self.reason_code = reason_code
        self.detail = detail
        super().__init__(f"{reason_code}" + (f": {detail}" if detail else ""))


@dataclass
class CanonicalFrame:
    """One canonical completed daily frame shared by both arms."""

    symbol: str
    bars: List[Dict[str, Any]]          # exact canonical records, oldest first
    frame_hash: str
    frame_snapshot_version: str
    timeframe: str
    bar_count: int
    first_date: str                     # ISO date
    last_date: str                      # ISO date (last COMPLETED bar)
    snapshot_date: date
    market_data_as_of: datetime         # UTC; == last canonical completed bar
    completion: Dict[str, Any]          # policy decision record
    excluded_partial_bar_date: Optional[str]

    def dataframe(self) -> pd.DataFrame:
        """Fresh independent DataFrame copy of the canonical frame.

        Each arm gets its OWN copy so neither evaluation can mutate what the
        other sees (or what was hashed/persisted).
        """
        df = pd.DataFrame([dict(b) for b in self.bars])
        df["date"] = pd.to_datetime(df["date"])
        return df


_NUMERIC_FIELDS = ("open", "high", "low", "close", "volume")


def _canonical_bar(raw: Any) -> Dict[str, Any]:
    """Normalize one provider bar; raise FrameRejection on malformed input."""
    if not isinstance(raw, dict):
        raise FrameRejection("malformed_bar", "bar is not an object")
    if "date" not in raw:
        raise FrameRejection("malformed_bar", "missing date")
    try:
        bar_date = pd.to_datetime(raw["date"]).date()
    except Exception:
        raise FrameRejection("malformed_bar", "unparseable date")

    bar: Dict[str, Any] = {"date": bar_date.isoformat()}
    for field_name in _NUMERIC_FIELDS:
        if field_name not in raw or raw[field_name] is None:
            raise FrameRejection("malformed_ohlcv", f"missing {field_name}")
        if isinstance(raw[field_name], bool):
            raise FrameRejection("malformed_ohlcv", f"boolean {field_name}")
        try:
            value = float(raw[field_name])
        except (TypeError, ValueError):
            raise FrameRejection("malformed_ohlcv", f"non-numeric {field_name}")
        if not math.isfinite(value):
            raise FrameRejection("malformed_ohlcv", f"non-finite {field_name}")
        bar[field_name] = value

    if min(bar["open"], bar["high"], bar["low"], bar["close"]) <= 0:
        raise FrameRejection("malformed_ohlcv", "non-positive price")
    if bar["volume"] < 0:
        raise FrameRejection("malformed_ohlcv", "negative volume")
    return bar


def compute_frame_hash(bars: List[Dict[str, Any]]) -> str:
    """SHA-256 of the complete canonical frame (order-preserving).

    The bar list order is semantic (chronological) and enters the hash as-is;
    per-bar dict key order is irrelevant (canonical JSON sorts object keys).
    Any change to any OHLCV value, any date, the bar order or the snapshot
    algorithm version changes the hash.
    """
    return _sha256(canonical_json({
        "frame_snapshot_version": FRAME_SNAPSHOT_VERSION,
        "timeframe": TIMEFRAME,
        "bars": bars,
    }))


def build_canonical_frame(
    symbol: str,
    payload: Optional[Dict[str, Any]],
    *,
    max_bars: int = FRAME_HARD_CAP_BARS,
    explicit_completed: Optional[bool] = None,
    now_utc: Optional[datetime] = None,
) -> CanonicalFrame:
    """Build the single canonical completed frame for one symbol.

    `payload` is the provider-shaped {"historical": [...]} dict from ONE
    fetch. `max_bars` is the derived shared history requirement (see
    shared_required_history_bars), hard-capped at FRAME_HARD_CAP_BARS.
    `explicit_completed` is optional trustworthy caller/provider completion
    metadata for the latest bar (None => derive from the ny_session_close.v1
    policy). Raises FrameRejection with a bounded deterministic reason when
    the symbol cannot be compared honestly. A provider returning FEWER than
    `max_bars` completed bars is NOT a rejection: both arms evaluate the
    same available frame and their own readiness rules decide.
    """
    max_bars = min(int(max_bars), FRAME_HARD_CAP_BARS)
    if max_bars <= 0:
        raise FrameRejection("invalid_frame_cap", str(max_bars))
    historical = (payload or {}).get("historical") or []
    if not historical:
        raise FrameRejection("no_data")

    bars = [_canonical_bar(raw) for raw in historical]
    # Canonicalization BEFORE hashing: any raw provider ordering (ascending,
    # descending, shuffled) yields the same chronological canonical frame.
    bars.sort(key=lambda b: b["date"])

    seen_dates = set()
    for bar in bars:
        if bar["date"] in seen_dates:
            raise FrameRejection("duplicate_session_date", bar["date"])
        seen_dates.add(bar["date"])

    # ONE completed-bar decision for both arms (ny_session_close.v1) — made
    # BEFORE the depth cap, so excluding a partial current-session bar never
    # shrinks an otherwise-full completed frame.
    df = pd.DataFrame(bars)
    df["date"] = pd.to_datetime(df["date"])
    completion = assess_latest_bar_completion(
        df, explicit_completed=explicit_completed, now_utc=now_utc
    )
    excluded_partial_bar_date: Optional[str] = None
    if completion["state"] == "partial":
        excluded_partial_bar_date = completion["bar_date"]
        bars = bars[:-1]
        if not bars:
            raise FrameRejection(
                "no_completed_bars", "only bar was the open session's partial bar"
            )
        df = pd.DataFrame(bars)
        df["date"] = pd.to_datetime(df["date"])
        completion = assess_latest_bar_completion(df, now_utc=now_utc)
    if completion["state"] != "completed":
        # Unknown / future-dated completion: reject honestly, never guess.
        raise FrameRejection(
            "unconfirmed_bar_completion", completion.get("reason")
        )

    # Deterministic cap: most recent `max_bars` COMPLETED bars, chronological
    # order preserved (never re-sorted, never recursively reordered).
    bars = bars[-max_bars:]

    snapshot_size = len(canonical_json(bars).encode("utf-8"))
    if snapshot_size > MAX_FRAME_SNAPSHOT_BYTES:
        # A truncated frame could not reproduce the decision -> reject.
        raise FrameRejection(
            "frame_snapshot_too_large", f"{snapshot_size} bytes"
        )

    last_date = date.fromisoformat(bars[-1]["date"])
    completion_record = {
        "policy": BAR_COMPLETION_POLICY,
        "state": completion["state"],
        "reason": completion["reason"],
        "original_latest_date": (
            excluded_partial_bar_date or bars[-1]["date"]
        ),
        "excluded_partial_bar_date": excluded_partial_bar_date,
        "canonical_as_of_date": bars[-1]["date"],
    }

    return CanonicalFrame(
        symbol=symbol,
        bars=bars,
        frame_hash=compute_frame_hash(bars),
        frame_snapshot_version=FRAME_SNAPSHOT_VERSION,
        timeframe=TIMEFRAME,
        bar_count=len(bars),
        first_date=bars[0]["date"],
        last_date=bars[-1]["date"],
        snapshot_date=last_date,
        market_data_as_of=datetime(
            last_date.year, last_date.month, last_date.day, tzinfo=timezone.utc
        ),
        completion=completion_record,
        excluded_partial_bar_date=excluded_partial_bar_date,
    )

"""Canonical completed 4H frame for multi-timeframe shadow evaluations (9E2).

Sibling of the daily canonical frame (frames.py): ONE fetch per symbol is
normalized into ONE canonical completed 4H frame, and the candidate strategy
evaluates a fresh copy of exactly that frame. The frame is strategy-agnostic
and verdict-independent.

Canonical 4H contract — four_hour_frame.v1 (derived from the existing
wyckoff_4h_trigger.v1 semantics; the frame layer never contradicts them):

  * timestamps are BAR STARTS, timezone-aware, normalized to UTC;
  * bars are the provider's native aggregates — expected bucket calendars
    (holidays, shortened sessions, DST shifts) are NEVER synthesized; the
    frame reports OBSERVED coverage (per-session bar starts in the exchange
    timezone) and explicit staleness instead of fabricating buckets;
  * a bar is COMPLETED when start + bar_duration_hours <= evaluation time;
    the currently-forming bucket and future-dated buckets are excluded and
    counted, never guessed;
  * as-of alignment with the canonical daily frame is deterministic: when an
    as-of session date is pinned, any 4H bar whose END falls on a LATER
    exchange session is excluded and counted;
  * duplicate bar starts REJECT the symbol's 4H frame (same rule as the
    daily frame's duplicate session dates — silent reordering or merging
    would fake determinism);
  * malformed / non-finite / non-positive OHLC rejects the frame; volume may
    be legitimately missing (None) per the trigger contract, but negative or
    non-finite volume rejects;
  * the frame hash covers the complete canonical frame the strategy receives;
  * staleness is measured in SESSIONS against the pinned daily calendar
    (mirroring the trigger's session-aware freshness rule), reported as
    metadata — the strategy's own trigger analysis stays authoritative.
"""

import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
from zoneinfo import ZoneInfo

from app.workers.provenance import canonical_json, _sha256


FOUR_HOUR_FRAME_CONTRACT_VERSION = "four_hour_frame.v1"
FOUR_HOUR_TIMEFRAME = "4h"

# Hard ceiling on stored/evaluated canonical 4H bars. The trigger needs
# trigger_lookback_4h + 1 (11 with defaults); ~30 calendar days of regular
# 4H aggregates is well under this bound.
FOUR_HOUR_FRAME_HARD_CAP_BARS = 240

# Bounded fetch window used by shadow experiments that require a 4H frame:
# comfortably covers the trigger lookback plus staleness margin without ever
# requesting unbounded history.
FOUR_HOUR_FETCH_CALENDAR_DAYS = 30


class FourHourFrameRejection(ValueError):
    """A symbol whose intraday data cannot form a trustworthy 4H frame.

    Carries a deterministic bounded reason code — never a raw payload.
    """

    def __init__(self, reason_code: str, detail: Optional[str] = None):
        self.reason_code = reason_code
        self.detail = detail
        super().__init__(f"{reason_code}" + (f": {detail}" if detail else ""))


@dataclass
class FourHourFrame:
    """One canonical completed 4H frame."""

    symbol: str
    provider: Optional[str]
    bars: List[Dict[str, Any]]        # canonical records, oldest first
    frame_hash: str
    contract_version: str
    timeframe: str
    bar_count: int
    first_start_utc: str              # ISO
    last_start_utc: str               # ISO
    last_end_utc: str                 # ISO (completed-bar cutoff evidence)
    evaluation_time_utc: str          # ISO
    bar_duration_hours: float
    requested_start: Optional[str]
    requested_end: Optional[str]
    as_of_session_date: Optional[str]
    excluded_incomplete_count: int
    excluded_after_as_of_count: int
    sessions_covered: List[str]       # sorted ISO exchange-session dates
    staleness_sessions: Optional[int]
    exchange_timezone: str

    def dataframe(self) -> pd.DataFrame:
        """Fresh independent DataFrame copy (tz-aware UTC 'date' column).

        Shape matches what wyckoff_mtf_v2 reads from data_meta["df_4h"]
        (normalize_4h_ohlcv accepts a 'date' column with tz-aware stamps).
        """
        records = []
        for bar in self.bars:
            records.append({
                "date": datetime.fromisoformat(bar["start_utc"]),
                "open": bar["open"],
                "high": bar["high"],
                "low": bar["low"],
                "close": bar["close"],
                "volume": bar["volume"],
            })
        return pd.DataFrame(records)

    def metadata(self) -> Dict[str, Any]:
        """Bounded JSON-safe frame metadata (never the raw bars)."""
        return {
            "contract_version": self.contract_version,
            "timeframe": self.timeframe,
            "state": "built",
            "symbol": self.symbol,
            "provider": self.provider,
            "frame_hash": self.frame_hash,
            "bar_count": self.bar_count,
            "first_start_utc": self.first_start_utc,
            "last_start_utc": self.last_start_utc,
            "last_end_utc": self.last_end_utc,
            "evaluation_time_utc": self.evaluation_time_utc,
            "bar_duration_hours": self.bar_duration_hours,
            "requested_start": self.requested_start,
            "requested_end": self.requested_end,
            "as_of_session_date": self.as_of_session_date,
            "excluded_incomplete_count": self.excluded_incomplete_count,
            "excluded_after_as_of_count": self.excluded_after_as_of_count,
            "session_count": len(self.sessions_covered),
            "first_session_date": (
                self.sessions_covered[0] if self.sessions_covered else None
            ),
            "last_session_date": (
                self.sessions_covered[-1] if self.sessions_covered else None
            ),
            "staleness_sessions": self.staleness_sessions,
            "exchange_timezone": self.exchange_timezone,
        }


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _canonical_intraday_bar(raw: Any, index: int) -> Dict[str, Any]:
    """Normalize one provider intraday bar; reject malformed input."""
    if not isinstance(raw, dict):
        raise FourHourFrameRejection("malformed_bar", f"row {index} not an object")
    start = raw.get("start_utc")
    if start is None:
        raise FourHourFrameRejection("malformed_bar", f"row {index} missing start")
    if isinstance(start, datetime):
        if start.tzinfo is None or start.tzinfo.utcoffset(start) is None:
            raise FourHourFrameRejection(
                "naive_timestamp", f"row {index} start is timezone-naive"
            )
        start_dt = start.astimezone(timezone.utc)
    else:
        try:
            parsed = datetime.fromisoformat(str(start))
        except ValueError:
            raise FourHourFrameRejection(
                "malformed_bar", f"row {index} unparseable start"
            )
        if parsed.tzinfo is None:
            raise FourHourFrameRejection(
                "naive_timestamp", f"row {index} start is timezone-naive"
            )
        start_dt = parsed.astimezone(timezone.utc)

    bar: Dict[str, Any] = {"start_utc": _iso_utc(start_dt)}
    for field_name in ("open", "high", "low", "close"):
        value = raw.get(field_name)
        if value is None or isinstance(value, bool):
            raise FourHourFrameRejection(
                "malformed_ohlc", f"row {index} {field_name}"
            )
        try:
            f = float(value)
        except (TypeError, ValueError):
            raise FourHourFrameRejection(
                "malformed_ohlc", f"row {index} {field_name}"
            )
        if not math.isfinite(f) or f <= 0:
            raise FourHourFrameRejection(
                "malformed_ohlc", f"row {index} {field_name}"
            )
        bar[field_name] = f
    if bar["high"] < bar["low"] or not (
        bar["low"] <= bar["open"] <= bar["high"]
        and bar["low"] <= bar["close"] <= bar["high"]
    ):
        raise FourHourFrameRejection("ohlc_envelope", f"row {index}")

    volume = raw.get("volume")
    if volume is None or (isinstance(volume, float) and math.isnan(volume)):
        bar["volume"] = None
    else:
        try:
            v = float(volume)
        except (TypeError, ValueError):
            raise FourHourFrameRejection("malformed_volume", f"row {index}")
        if not math.isfinite(v) or v < 0:
            raise FourHourFrameRejection("malformed_volume", f"row {index}")
        bar["volume"] = v
    return bar


def compute_four_hour_frame_hash(bars: List[Dict[str, Any]]) -> str:
    """SHA-256 of the complete canonical 4H frame (order-preserving)."""
    return _sha256(canonical_json({
        "contract_version": FOUR_HOUR_FRAME_CONTRACT_VERSION,
        "timeframe": FOUR_HOUR_TIMEFRAME,
        "bars": bars,
    }))


def _session_date(dt_utc: datetime, exchange_timezone: str) -> date:
    return dt_utc.astimezone(ZoneInfo(exchange_timezone)).date()


def build_four_hour_frame(
    symbol: str,
    payload: Optional[Dict[str, Any]],
    *,
    evaluation_time_utc: Optional[datetime] = None,
    as_of_session_date: Optional[date] = None,
    daily_session_dates: Optional[List[date]] = None,
    bar_duration_hours: float = 4.0,
    exchange_timezone: str = "America/New_York",
    max_bars: int = FOUR_HOUR_FRAME_HARD_CAP_BARS,
) -> FourHourFrame:
    """Build the single canonical completed 4H frame for one symbol.

    `payload` is the get_intraday_history result dict ({"bars": [...]}).
    Raises FourHourFrameRejection with a bounded deterministic reason when
    the symbol's intraday data cannot be trusted. A provider returning FEWER
    bars than the trigger lookback is NOT a rejection — the strategy's own
    trigger analysis reports insufficient_4h_history explicitly.
    """
    max_bars = min(int(max_bars), FOUR_HOUR_FRAME_HARD_CAP_BARS)
    if max_bars <= 0:
        raise FourHourFrameRejection("invalid_frame_cap", str(max_bars))
    duration = float(bar_duration_hours)
    if not math.isfinite(duration) or duration <= 0:
        raise FourHourFrameRejection("invalid_bar_duration", str(bar_duration_hours))

    eval_time = evaluation_time_utc or datetime.now(timezone.utc)
    if eval_time.tzinfo is None or eval_time.tzinfo.utcoffset(eval_time) is None:
        raise FourHourFrameRejection("naive_evaluation_time")
    eval_time = eval_time.astimezone(timezone.utc)

    raw_bars = (payload or {}).get("bars") or []
    if not raw_bars:
        raise FourHourFrameRejection("no_data")

    bars = [
        _canonical_intraday_bar(raw, i) for i, raw in enumerate(raw_bars)
    ]
    bars.sort(key=lambda b: b["start_utc"])
    seen_starts = set()
    for bar in bars:
        if bar["start_utc"] in seen_starts:
            raise FourHourFrameRejection("duplicate_bar_start", bar["start_utc"])
        seen_starts.add(bar["start_utc"])

    span = timedelta(hours=duration)
    completed: List[Dict[str, Any]] = []
    excluded_incomplete = 0
    excluded_after_as_of = 0
    for bar in bars:
        start_dt = datetime.fromisoformat(bar["start_utc"])
        end_dt = start_dt + span
        if end_dt > eval_time:
            excluded_incomplete += 1
            continue
        if (
            as_of_session_date is not None
            and _session_date(end_dt, exchange_timezone) > as_of_session_date
        ):
            excluded_after_as_of += 1
            continue
        completed.append(bar)

    if not completed:
        raise FourHourFrameRejection(
            "no_completed_bars",
            "no bar is completed at the pinned evaluation time",
        )

    completed = completed[-max_bars:]

    sessions = sorted({
        _session_date(
            datetime.fromisoformat(b["start_utc"]) + span, exchange_timezone
        ).isoformat()
        for b in completed
    })

    staleness: Optional[int] = None
    if daily_session_dates and as_of_session_date is not None:
        last_session = date.fromisoformat(sessions[-1])
        if last_session <= as_of_session_date:
            staleness = sum(
                1 for d in daily_session_dates
                if last_session < d <= as_of_session_date
            )

    last_start = datetime.fromisoformat(completed[-1]["start_utc"])
    return FourHourFrame(
        symbol=symbol,
        provider=(payload or {}).get("provider"),
        bars=completed,
        frame_hash=compute_four_hour_frame_hash(completed),
        contract_version=FOUR_HOUR_FRAME_CONTRACT_VERSION,
        timeframe=FOUR_HOUR_TIMEFRAME,
        bar_count=len(completed),
        first_start_utc=completed[0]["start_utc"],
        last_start_utc=completed[-1]["start_utc"],
        last_end_utc=_iso_utc(last_start + span),
        evaluation_time_utc=_iso_utc(eval_time),
        bar_duration_hours=duration,
        requested_start=(payload or {}).get("requested_start"),
        requested_end=(payload or {}).get("requested_end"),
        as_of_session_date=(
            as_of_session_date.isoformat()
            if as_of_session_date is not None else None
        ),
        excluded_incomplete_count=excluded_incomplete,
        excluded_after_as_of_count=excluded_after_as_of,
        sessions_covered=sessions,
        staleness_sessions=staleness,
        exchange_timezone=exchange_timezone,
    )

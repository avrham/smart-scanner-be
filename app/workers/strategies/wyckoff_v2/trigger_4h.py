"""Deterministic 4H trigger analysis — wyckoff_4h_trigger.v1 (Phase 9C1).

Price-only first contract. Timestamps are bar starts; a bar is complete when
bar_end <= evaluation_time_utc. Freshness is session-aware via completed
daily session dates, not wall-clock hours.

Pure functions only — no I/O, providers, DB or LLM.
"""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

import pandas as pd
from zoneinfo import ZoneInfo

from app.workers.strategies.wyckoff_v2.constants import (
    FOUR_HOUR_TRIGGER_VERSION,
    resolve_config,
)
from app.workers.strategies.wyckoff_v2.models import FourHourTriggerResult


class FourHourTriggerError(ValueError):
    """Deterministic rejection of malformed 4H trigger input."""

    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


_OHLCV = ("open", "high", "low", "close", "volume")


def _as_utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.to_pydatetime().astimezone(timezone.utc)


def _parse_bar_start(value: Any, tz_name: str) -> datetime:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize(tz_name)
    return ts.to_pydatetime().astimezone(timezone.utc)


def _finite_positive(value: Any, where: str) -> float:
    try:
        f = float(value)
    except (TypeError, ValueError) as exc:
        raise FourHourTriggerError("malformed_ohlc", f"{where} not numeric") from exc
    if not math.isfinite(f) or f <= 0:
        raise FourHourTriggerError("malformed_ohlc", f"{where} not finite positive")
    return f


def normalize_4h_ohlcv(
    df: pd.DataFrame,
    *,
    timestamp_timezone: str = "UTC",
) -> pd.DataFrame:
    """Normalize 4H OHLCV. Does not mutate the input. Volume may remain missing."""
    if df is None or len(df) == 0:
        return pd.DataFrame(
            columns=["timestamp", "open", "high", "low", "close", "volume"]
        )
    required = {"open", "high", "low", "close"}
    cols = set(df.columns)
    if "timestamp" not in cols and "date" not in cols and "datetime" not in cols:
        raise FourHourTriggerError("missing_timestamp", "4H frame missing timestamp")
    ts_col = (
        "timestamp"
        if "timestamp" in cols
        else ("date" if "date" in cols else "datetime")
    )
    if not required.issubset(cols):
        raise FourHourTriggerError("missing_ohlc", "4H frame missing OHLC columns")

    working = df.copy()
    starts = [_parse_bar_start(v, timestamp_timezone) for v in working[ts_col].tolist()]
    opens = [_finite_positive(v, "open") for v in working["open"].tolist()]
    highs = [_finite_positive(v, "high") for v in working["high"].tolist()]
    lows = [_finite_positive(v, "low") for v in working["low"].tolist()]
    closes = [_finite_positive(v, "close") for v in working["close"].tolist()]
    volumes = []
    for v in working["volume"].tolist() if "volume" in working.columns else [None] * len(working):
        if v is None or (isinstance(v, float) and math.isnan(v)) or pd.isna(v):
            volumes.append(None)
            continue
        f = float(v)
        if not math.isfinite(f):
            raise FourHourTriggerError("malformed_volume", "non-finite volume")
        if f < 0:
            raise FourHourTriggerError("negative_volume", "negative volume")
        volumes.append(f)

    for i, (h, l, o, c) in enumerate(zip(highs, lows, opens, closes)):
        if h < l:
            raise FourHourTriggerError("ohlc_envelope", f"high < low at row {i}")
        if o > h or o < l or c > h or c < l:
            raise FourHourTriggerError("ohlc_envelope", f"open/close outside HL at {i}")

    out = pd.DataFrame(
        {
            "timestamp": starts,
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volumes,
        }
    )
    out = out.sort_values("timestamp").reset_index(drop=True)
    if out["timestamp"].duplicated().any():
        raise FourHourTriggerError("duplicate_timestamps", "duplicate 4H timestamps")
    return out


def _completed_frame(
    frame: pd.DataFrame,
    *,
    evaluation_time_utc: datetime,
    duration_hours: float,
) -> Tuple[pd.DataFrame, int]:
    if len(frame) == 0:
        return frame.copy(), 0
    duration = timedelta(hours=float(duration_hours))
    ends = [ts + duration for ts in frame["timestamp"].tolist()]
    mask = [end <= evaluation_time_utc for end in ends]
    completed = frame.loc[mask].copy().reset_index(drop=True)
    excluded = int(len(frame) - len(completed))
    return completed, excluded


def _session_date(dt_utc: datetime, exchange_timezone: str) -> date:
    return dt_utc.astimezone(ZoneInfo(exchange_timezone)).date()


def _staleness_sessions(
    *,
    latest_4h_end: datetime,
    daily_frame: Optional[pd.DataFrame],
    daily_as_of: Optional[str],
    exchange_timezone: str,
) -> Tuple[Optional[int], Tuple[str, ...]]:
    if daily_frame is None or len(daily_frame) == 0 or daily_as_of is None:
        return None, ("unconfirmed_4h_freshness",)
    session = _session_date(latest_4h_end, exchange_timezone)
    daily_dates = [
        pd.Timestamp(d).date() for d in daily_frame["date"].tolist()
    ]
    pinned = pd.Timestamp(daily_as_of).date()
    # Reconcile: the 4H session date must not be after pinned daily as_of,
    # and should be reconcilable with the daily calendar (not invent a session).
    if session > pinned:
        return None, ("unconfirmed_4h_freshness",)
    if session not in daily_dates and not any(d <= session for d in daily_dates):
        return None, ("unconfirmed_4h_freshness",)
    count = sum(1 for d in daily_dates if session < d <= pinned)
    return int(count), ()


def analyze_4h_trigger(
    df_4h: Optional[pd.DataFrame],
    *,
    side: str,
    evaluation_time_utc: Any,
    daily_frame: Optional[pd.DataFrame] = None,
    daily_market_data_as_of: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    enabled: Optional[bool] = None,
) -> FourHourTriggerResult:
    """Analyze the completed 4H trigger for LONG or SHORT."""
    cfg = resolve_config(config)
    enabled_flag = bool(cfg["enable_4h_trigger"] if enabled is None else enabled)
    eval_utc = _as_utc(evaluation_time_utc)
    lookback = int(cfg["trigger_lookback_4h"])
    required = lookback + 1
    duration = float(cfg["four_hour_bar_duration_hours"])
    tz_name = str(cfg["four_hour_timestamp_timezone"])
    exchange_tz = str(cfg["exchange_timezone"])
    max_stale = int(cfg["max_4h_staleness_sessions"])
    side_u = str(side or "UNKNOWN").upper()
    config_used = {
        "enable_4h_trigger": enabled_flag,
        "trigger_lookback_4h": lookback,
        "four_hour_bar_duration_hours": duration,
        "four_hour_timestamp_timezone": tz_name,
        "max_4h_staleness_sessions": max_stale,
        "exchange_timezone": exchange_tz,
    }

    def _result(**kwargs: Any) -> FourHourTriggerResult:
        base = dict(
            trigger_version=FOUR_HOUR_TRIGGER_VERSION,
            enabled=enabled_flag,
            state="unknown",
            reason_codes=(),
            side=side_u,
            evaluation_time_utc=eval_utc.isoformat().replace("+00:00", "Z"),
            daily_market_data_as_of=daily_market_data_as_of,
            available_input_bars=0,
            available_completed_bars=0,
            required_completed_bars=required,
            excluded_incomplete_bar_count=0,
            latest_completed_4h_start=None,
            latest_completed_4h_end=None,
            latest_completed_4h_session_date=None,
            staleness_sessions=None,
            local_high=None,
            local_low=None,
            trigger_level=None,
            contradiction_level=None,
            current_close=None,
            trigger_price=None,
            triggered=False,
            contradicted=False,
            missing_data=(),
            config_used=config_used,
        )
        base.update(kwargs)
        return FourHourTriggerResult(**base)

    if not enabled_flag:
        return _result(
            state="unknown",
            reason_codes=("four_hour_trigger_disabled",),
            missing_data=("four_hour_disabled",),
        )

    if df_4h is None:
        return _result(
            state="unknown",
            reason_codes=("four_hour_data_missing",),
            missing_data=("four_hour_frame",),
        )

    normalized = normalize_4h_ohlcv(df_4h, timestamp_timezone=tz_name)
    # Future-start aggregates are not observable at the pinned evaluation time.
    normalized = normalized.loc[
        normalized["timestamp"].map(lambda ts: ts < eval_utc)
    ].reset_index(drop=True)
    completed, excluded = _completed_frame(
        normalized, evaluation_time_utc=eval_utc, duration_hours=duration
    )
    input_bars = int(len(normalized))
    completed_bars = int(len(completed))

    if completed_bars == 0:
        return _result(
            state="unknown",
            reason_codes=("unconfirmed_4h_bar_completion",),
            available_input_bars=input_bars,
            available_completed_bars=0,
            excluded_incomplete_bar_count=excluded,
            missing_data=("completed_4h_bars",),
        )

    latest = completed.iloc[-1]
    latest_start = latest["timestamp"]
    latest_end = latest_start + timedelta(hours=duration)
    session = _session_date(latest_end, exchange_tz)
    stale, stale_reasons = _staleness_sessions(
        latest_4h_end=latest_end,
        daily_frame=daily_frame,
        daily_as_of=daily_market_data_as_of,
        exchange_timezone=exchange_tz,
    )
    if stale is None:
        return _result(
            state="unknown",
            reason_codes=stale_reasons,
            available_input_bars=input_bars,
            available_completed_bars=completed_bars,
            excluded_incomplete_bar_count=excluded,
            latest_completed_4h_start=latest_start.isoformat().replace("+00:00", "Z"),
            latest_completed_4h_end=latest_end.isoformat().replace("+00:00", "Z"),
            latest_completed_4h_session_date=session.isoformat(),
            missing_data=("four_hour_freshness",),
        )
    if stale > max_stale:
        return _result(
            state="unknown",
            reason_codes=("four_hour_trigger_stale",),
            available_input_bars=input_bars,
            available_completed_bars=completed_bars,
            excluded_incomplete_bar_count=excluded,
            latest_completed_4h_start=latest_start.isoformat().replace("+00:00", "Z"),
            latest_completed_4h_end=latest_end.isoformat().replace("+00:00", "Z"),
            latest_completed_4h_session_date=session.isoformat(),
            staleness_sessions=stale,
            current_close=float(latest["close"]),
            missing_data=("four_hour_freshness",),
        )

    if completed_bars < required:
        return _result(
            state="unknown",
            reason_codes=("insufficient_4h_history",),
            available_input_bars=input_bars,
            available_completed_bars=completed_bars,
            excluded_incomplete_bar_count=excluded,
            latest_completed_4h_start=latest_start.isoformat().replace("+00:00", "Z"),
            latest_completed_4h_end=latest_end.isoformat().replace("+00:00", "Z"),
            latest_completed_4h_session_date=session.isoformat(),
            staleness_sessions=stale,
            current_close=float(latest["close"]),
            missing_data=("four_hour_lookback",),
        )

    if side_u not in ("LONG", "SHORT"):
        return _result(
            state="unknown",
            reason_codes=("trigger_side_unknown",),
            available_input_bars=input_bars,
            available_completed_bars=completed_bars,
            excluded_incomplete_bar_count=excluded,
            latest_completed_4h_start=latest_start.isoformat().replace("+00:00", "Z"),
            latest_completed_4h_end=latest_end.isoformat().replace("+00:00", "Z"),
            latest_completed_4h_session_date=session.isoformat(),
            staleness_sessions=stale,
            current_close=float(latest["close"]),
            missing_data=("trigger_side",),
        )

    window = completed.iloc[-(required):]
    prior = window.iloc[:-1]
    local_high = float(prior["high"].max())
    local_low = float(prior["low"].min())
    current_close = float(window.iloc[-1]["close"])

    if side_u == "LONG":
        trigger_level = local_high
        contradiction_level = local_low
        confirmed = current_close > local_high
        contradicted = current_close < local_low
    else:
        trigger_level = local_low
        contradiction_level = local_high
        confirmed = current_close < local_low
        contradicted = current_close > local_high

    if confirmed:
        state = "confirmed"
        reasons: Tuple[str, ...] = ()
        trigger_price: Optional[float] = current_close
    elif contradicted:
        state = "contradicted"
        reasons = ("four_hour_trigger_contradicted",)
        trigger_price = None
    else:
        state = "missing"
        reasons = ("four_hour_trigger_missing",)
        trigger_price = None

    return _result(
        state=state,
        reason_codes=reasons,
        available_input_bars=input_bars,
        available_completed_bars=completed_bars,
        excluded_incomplete_bar_count=excluded,
        latest_completed_4h_start=latest_start.isoformat().replace("+00:00", "Z"),
        latest_completed_4h_end=latest_end.isoformat().replace("+00:00", "Z"),
        latest_completed_4h_session_date=session.isoformat(),
        staleness_sessions=stale,
        local_high=local_high,
        local_low=local_low,
        trigger_level=trigger_level,
        contradiction_level=contradiction_level,
        current_close=current_close,
        trigger_price=trigger_price,
        triggered=bool(confirmed),
        contradicted=bool(contradicted),
        missing_data=(),
    )

"""Shared completed-daily-bar policy — ny_session_close.v1 (Phase 9A).

Extracted VERBATIM from sma150.v3 (Phase 8) so wyckoff_mtf.v2 can reuse the
exact same policy without duplicating it. The behavior contract is frozen:

  * explicit caller/provider metadata wins;
  * a bar dated strictly before the current exchange-session date is
    completed;
  * a bar dated on the current session date is completed only at/after the
    configured session close in the exchange timezone (early-close days are
    treated conservatively as incomplete until the regular close, which can
    only exclude, never include);
  * a future-dated bar (or unusable policy input) is UNKNOWN — the caller
    must refuse rather than guess.

`app.workers.strategies.sma150_v3` re-exposes this function (with its own
injectable module clock) so every existing import surface keeps working.
"""

from datetime import date, datetime, timezone
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

import pandas as pd

BAR_COMPLETION_POLICY = "ny_session_close.v1"


def _utc_now() -> datetime:
    """Injectable clock (module-level so tests can pin the evaluation time)."""
    return datetime.now(timezone.utc)


def assess_latest_bar_completion(
    df: Optional[pd.DataFrame],
    *,
    exchange_timezone: str = "America/New_York",
    session_close_time: str = "16:00",
    explicit_completed: Optional[bool] = None,
    now_utc: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Decide whether the LATEST daily bar is a completed session bar.

    Deterministic rules, in priority order:
      1. explicit caller/provider metadata (`explicit_completed`) wins;
      2. a bar dated strictly BEFORE the current exchange-session date is
         completed (past sessions cannot still be open);
      3. a bar dated ON the current exchange-session date is completed only
         at/after the configured session close in the exchange timezone
         (never a bare wall-clock check — timezone + session close always
         apply; early-close days are treated conservatively as incomplete
         until the regular close, which can only exclude, never include);
      4. a future-dated bar (or an unusable policy input) is UNKNOWN — the
         caller must refuse rather than guess.

    Returns {"state": "completed"|"partial"|"unknown", "reason": str,
             "bar_date": ISO date or None, "policy": BAR_COMPLETION_POLICY}.
    """
    result: Dict[str, Any] = {"policy": BAR_COMPLETION_POLICY, "bar_date": None}

    if df is None or len(df) == 0 or "date" not in getattr(df, "columns", []):
        result.update(state="unknown", reason="no_bars")
        return result
    try:
        bar_date: date = pd.to_datetime(df.iloc[-1]["date"]).date()
    except Exception:
        result.update(state="unknown", reason="unparseable_bar_date")
        return result
    result["bar_date"] = bar_date.isoformat()

    if explicit_completed is True:
        result.update(state="completed", reason="explicit_metadata")
        return result
    if explicit_completed is False:
        result.update(state="partial", reason="explicit_metadata")
        return result

    try:
        tz = ZoneInfo(exchange_timezone)
        close_hour, close_minute = (
            int(p) for p in str(session_close_time).split(":")[:2]
        )
    except Exception:
        result.update(state="unknown", reason="invalid_completion_policy")
        return result

    now_exchange = (now_utc or _utc_now()).astimezone(tz)
    if bar_date < now_exchange.date():
        result.update(state="completed", reason="prior_session_date")
    elif bar_date == now_exchange.date():
        if (now_exchange.hour, now_exchange.minute) >= (close_hour, close_minute):
            result.update(state="completed", reason="after_session_close")
        else:
            result.update(state="partial", reason="session_in_progress")
    else:
        result.update(state="unknown", reason="future_dated_bar")
    return result

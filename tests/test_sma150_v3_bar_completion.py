"""Phase 8: completed-daily-bar semantics (ny_session_close.v1).

A provider daily aggregate can represent the still-open US session. sma150.v3
may only use completed bars: a partial latest bar is excluded (one safe
deterministic exclusion), unknown completion AVOIDs with
reason_code=unconfirmed_bar_completion, and a partial bar can never create
ENTER or pass volume confirmation.
"""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from app.workers.provenance import (
    market_data_as_of_from_details,
    market_data_as_of_from_df,
)
from app.workers.strategies import get_strategy
from app.workers.strategies.base import StrategyContext, StrategyDecision
from app.workers.strategies.sma150_v3 import (
    BAR_COMPLETION_POLICY,
    assess_latest_bar_completion,
)
from tests.sma150_v3_frames import build_uptrend_frame

NY = ZoneInfo("America/New_York")


def _evaluate(df, data_meta=None, config_overrides=None):
    strategy = get_strategy("sma150_bounce_v3")
    config = strategy.default_config()
    if config_overrides:
        config.update(config_overrides)
    return strategy.evaluate(
        df,
        StrategyContext(symbol="TEST", pattern_code="sma150_bounce_v3",
                        config=config, data_meta=data_meta),
    )


def _completion_item(result):
    return next(
        i for i in result.details["evidence"]["items"]
        if i["code"] == "latest_bar_completion"
    )


def _ny(dt_str):
    """Naive 'YYYY-MM-DD HH:MM' in New York -> aware UTC datetime."""
    return datetime.strptime(dt_str, "%Y-%m-%d %H:%M").replace(tzinfo=NY).astimezone(
        timezone.utc
    )


class TestAssessLatestBarCompletion:
    def _frame(self, last_date):
        return pd.DataFrame({
            "date": pd.to_datetime([last_date]),
            "open": [100.0], "high": [101.0], "low": [99.0],
            "close": [100.5], "volume": [1e6],
        })

    def test_prior_session_date_is_completed(self):
        out = assess_latest_bar_completion(
            self._frame("2026-07-16"), now_utc=_ny("2026-07-17 10:00")
        )
        assert out["state"] == "completed"
        assert out["reason"] == "prior_session_date"
        assert out["policy"] == BAR_COMPLETION_POLICY

    def test_same_date_during_session_is_partial(self):
        out = assess_latest_bar_completion(
            self._frame("2026-07-17"), now_utc=_ny("2026-07-17 12:30")
        )
        assert out["state"] == "partial"
        assert out["reason"] == "session_in_progress"

    def test_same_date_after_close_is_completed(self):
        out = assess_latest_bar_completion(
            self._frame("2026-07-17"), now_utc=_ny("2026-07-17 16:00")
        )
        assert out["state"] == "completed"
        assert out["reason"] == "after_session_close"

    def test_timezone_matters_not_bare_wall_clock(self):
        """20:30 UTC on the bar date is 16:30 New York — completed; the same
        instant misread as NY wall-clock would still be in-session. The rule
        must convert through the exchange timezone."""
        out = assess_latest_bar_completion(
            self._frame("2026-07-17"),
            now_utc=datetime(2026, 7, 17, 20, 30, tzinfo=timezone.utc),
        )
        assert out["state"] == "completed"
        # And 15:30 UTC (11:30 NY) is still in-session.
        out2 = assess_latest_bar_completion(
            self._frame("2026-07-17"),
            now_utc=datetime(2026, 7, 17, 15, 30, tzinfo=timezone.utc),
        )
        assert out2["state"] == "partial"

    def test_future_dated_bar_is_unknown(self):
        out = assess_latest_bar_completion(
            self._frame("2026-07-20"), now_utc=_ny("2026-07-17 12:00")
        )
        assert out["state"] == "unknown"
        assert out["reason"] == "future_dated_bar"

    def test_explicit_metadata_wins(self):
        frame = self._frame("2026-07-10")
        forced_partial = assess_latest_bar_completion(
            frame, explicit_completed=False, now_utc=_ny("2026-07-17 12:00")
        )
        assert forced_partial["state"] == "partial"
        forced_completed = assess_latest_bar_completion(
            frame, explicit_completed=True, now_utc=_ny("2026-07-01 12:00")
        )
        assert forced_completed["state"] == "completed"

    def test_invalid_policy_is_unknown_never_a_guess(self):
        out = assess_latest_bar_completion(
            self._frame("2026-07-16"),
            exchange_timezone="Not/AZone",
            now_utc=_ny("2026-07-17 12:00"),
        )
        assert out["state"] == "unknown"
        assert out["reason"] == "invalid_completion_policy"


class TestEvaluateWithCompletion:
    def test_completed_trigger_bar_may_enter(self):
        """Frames dated in the past are completed bars: ENTER stays possible."""
        result = _evaluate(build_uptrend_frame(trigger=True, vol_ratio=1.30))
        assert result.decision == StrategyDecision.ENTER
        item = _completion_item(result)
        assert item["state"] == "pass"
        assert item["raw_value"] == "completed"
        assert item["metadata"]["policy"] == BAR_COMPLETION_POLICY

    def test_same_bar_marked_partial_cannot_enter(self):
        """The exact same OHLCV frame, with the last bar explicitly marked
        partial, must not ENTER off that bar."""
        df = build_uptrend_frame(trigger=True, vol_ratio=1.30)
        completed = _evaluate(df)
        assert completed.decision == StrategyDecision.ENTER

        partial = _evaluate(df, data_meta={"latest_bar_completed": False})
        assert partial.decision != StrategyDecision.ENTER
        item = _completion_item(partial)
        assert item["metadata"]["excluded_partial_bar_date"] == str(
            pd.to_datetime(df.iloc[-1]["date"]).date()
        )

    def test_same_bar_inferred_partial_by_session_clock_cannot_enter(self):
        """Same frame, evaluation time pinned DURING the session on the last
        bar's date: the bar is inferred partial and excluded."""
        df = build_uptrend_frame(trigger=True, vol_ratio=1.30)
        last_date = pd.to_datetime(df.iloc[-1]["date"]).date()
        in_session = datetime(
            last_date.year, last_date.month, last_date.day, 11, 0, tzinfo=NY
        ).astimezone(timezone.utc)
        result = _evaluate(df, data_meta={"evaluation_time_utc": in_session})
        assert result.decision != StrategyDecision.ENTER
        item = _completion_item(result)
        assert item["metadata"]["excluded_partial_bar_date"] == str(last_date)
        # After the close on the same date, the same frame may ENTER again.
        after_close = datetime(
            last_date.year, last_date.month, last_date.day, 16, 30, tzinfo=NY
        ).astimezone(timezone.utc)
        assert _evaluate(
            df, data_meta={"evaluation_time_utc": after_close}
        ).decision == StrategyDecision.ENTER

    def test_partial_high_volume_bar_does_not_pass_volume_confirmation(self):
        """A partial bar with huge intraday volume is excluded entirely: the
        evaluation matches the frame WITHOUT that bar (weak completed volume
        fails the 1.20 gate; the partial 5x volume never counts)."""
        base = build_uptrend_frame(trigger=False, vol_ratio=1.0)
        partial = base.copy()
        partial.loc[len(partial) - 1, "volume"] = 5_000_000.0
        result = _evaluate(partial, data_meta={"latest_bar_completed": False})
        assert result.decision != StrategyDecision.ENTER
        vol_item = next(
            i for i in result.details["evidence"]["items"]
            if i["code"] == "trigger_volume_ratio"
        )
        assert vol_item["raw_value"] < 1.20  # 5x partial volume never counted

        truncated = _evaluate(base.iloc[:-1].reset_index(drop=True))
        assert result.details["vol_ratio"] == truncated.details["vol_ratio"]

    def test_rolling_volume_average_excludes_trigger_bar(self):
        df = build_uptrend_frame(trigger=True, vol_ratio=1.30)
        result = _evaluate(df)
        volumes = df["volume"].astype(float)
        expected = float(volumes.iloc[-1]) / float(volumes.iloc[-21:-1].mean())
        assert result.details["vol_ratio"] == pytest.approx(expected, abs=1e-4)
        baseline = next(
            i for i in result.details["evidence"]["items"]
            if i["code"] == "volume_baseline_end_date"
        )
        # The completed baseline ends at the bar BEFORE the trigger bar.
        assert baseline["raw_value"] == str(
            pd.to_datetime(df.iloc[-2]["date"]).date()
        )
        assert baseline["metadata"]["excludes_trigger_bar"] is True

    def test_unknown_completion_avoids_with_reason_code(self):
        """A future-dated latest bar (relative to the pinned evaluation time)
        cannot be proven completed and cannot be safely excluded."""
        from datetime import timedelta
        df = build_uptrend_frame()
        last_date = pd.to_datetime(df.iloc[-1]["date"]).date()
        before = last_date - timedelta(days=2)
        eval_before = datetime(
            before.year, before.month, before.day, 12, 0, tzinfo=NY
        ).astimezone(timezone.utc)
        result = _evaluate(df, data_meta={"evaluation_time_utc": eval_before})
        assert result.decision == StrategyDecision.AVOID
        assert result.rejection_reason == "unconfirmed_bar_completion"
        bundle = result.details["evidence"]
        assert bundle["setup_state"] == "unknown"
        assert bundle["trigger_state"] == "unknown"
        assert "unconfirmed_bar_completion" in bundle["missing_data"]
        item = _completion_item(result)
        assert item["state"] == "unknown"
        assert item["reason_code"] == "unconfirmed_bar_completion"

    def test_double_partial_is_unknown_not_double_excluded(self):
        """Only ONE safe deterministic exclusion: two bars sharing the current
        session date (corrupt feed) end as unknown -> AVOID."""
        df = build_uptrend_frame()
        dup = df.copy()
        dup.loc[len(dup) - 2, "date"] = dup.loc[len(dup) - 1, "date"]
        last_date = pd.to_datetime(dup.iloc[-1]["date"]).date()
        in_session = datetime(
            last_date.year, last_date.month, last_date.day, 11, 0, tzinfo=NY
        ).astimezone(timezone.utc)
        result = _evaluate(dup, data_meta={"evaluation_time_utc": in_session})
        assert result.decision == StrategyDecision.AVOID
        assert result.rejection_reason == "unconfirmed_bar_completion"

    def test_trigger_bar_date_evidence_present(self):
        df = build_uptrend_frame(trigger=True, vol_ratio=1.30)
        result = _evaluate(df)
        item = next(
            i for i in result.details["evidence"]["items"]
            if i["code"] == "trigger_bar_date"
        )
        assert item["raw_value"] == str(pd.to_datetime(df.iloc[-1]["date"]).date())

    def test_completion_policy_persisted_in_config_snapshot_path(self):
        result = _evaluate(build_uptrend_frame())
        thresholds = result.details["thresholds_used"]
        assert thresholds["bar_completion_policy"] == BAR_COMPLETION_POLICY
        assert thresholds["exchange_timezone"] == "America/New_York"
        assert thresholds["session_close_time"] == "16:00"


class TestMarketDataAsOf:
    def test_as_of_points_to_completed_evaluated_bar(self):
        df = build_uptrend_frame(trigger=True, vol_ratio=1.30)
        result = _evaluate(df, data_meta={"latest_bar_completed": False})
        declared = market_data_as_of_from_details(result.details)
        expected = market_data_as_of_from_df(df.iloc[:-1])
        assert declared == expected
        # The raw frame's last (partial) bar is NOT the declared as-of.
        assert declared != market_data_as_of_from_df(df)

    def test_as_of_matches_frame_when_all_bars_completed(self):
        df = build_uptrend_frame()
        result = _evaluate(df)
        assert market_data_as_of_from_details(result.details) == \
            market_data_as_of_from_df(df)

    def test_v2_details_declare_no_as_of(self):
        from app.workers.patterns.sma150 import evaluate_sma150_bounce
        df = build_uptrend_frame(trigger=True, vol_ratio=1.30)
        raw = evaluate_sma150_bounce("TEST", df, None)
        assert market_data_as_of_from_details(raw["details"]) is None

    def test_funnel_and_legacy_paths_share_the_completion_policy(self, monkeypatch):
        """Both paths execute the SAME strategy method; pinning the module
        clock to an in-session time affects both identically."""
        import app.workers.strategies.sma150_v3 as v3mod
        from app.workers.scan_runner import _evaluate_pattern

        df = build_uptrend_frame(trigger=True, vol_ratio=1.30)
        last_date = pd.to_datetime(df.iloc[-1]["date"]).date()
        in_session = datetime(
            last_date.year, last_date.month, last_date.day, 11, 0, tzinfo=NY
        ).astimezone(timezone.utc)
        monkeypatch.setattr(v3mod, "_utc_now", lambda: in_session)

        strategy = get_strategy("sma150_bounce_v3")
        cfg = strategy.default_config()
        funnel_result = strategy.evaluate(
            df, StrategyContext(symbol="TEST", pattern_code="sma150_bounce_v3",
                                config=cfg, scanner_mode="funnel"),
        )
        legacy_result, _ = _evaluate_pattern("TEST", df, "sma150_bounce_v3",
                                             cfg, None)
        assert funnel_result.verdict == legacy_result["verdict"]
        assert funnel_result.verdict != "ENTER"
        f_item = _completion_item(funnel_result)
        l_item = next(
            i for i in legacy_result["details"]["evidence"]["items"]
            if i["code"] == "latest_bar_completion"
        )
        assert f_item["metadata"] == l_item["metadata"]
        assert funnel_result.details["market_data_as_of"] == \
            legacy_result["details"]["market_data_as_of"]

    def test_v2_behavior_unchanged_by_completion_policy(self):
        """sma150.v2 keeps evaluating the raw frame (no truncation, no
        completion gate) — its output is identical regardless of any
        completion metadata concept."""
        from app.workers.patterns.sma150 import evaluate_sma150_bounce
        df = build_uptrend_frame(trigger=True, vol_ratio=1.30)
        raw = evaluate_sma150_bounce("TEST", df, None)
        assert raw["details"]["snapshot_date"] == str(
            pd.to_datetime(df.iloc[-1]["date"]).date()
        )
        assert "market_data_as_of" not in raw["details"]

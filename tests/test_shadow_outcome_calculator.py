"""Phase 8.1B2: pure pair-outcome math — reference semantics, forward-bar
alignment, outcome.v1 math reuse and the versioned fingerprints.

Deterministic unit tests only — no DB, no providers, no live calls.
"""

from datetime import date, datetime, timedelta, timezone

import pytest

from app.workers.shadow.outcomes.calculator import (
    ShadowOutcomeRejection,
    build_forward_sequence,
    check_reference_revision,
    compute_benchmark_returns_for_pair,
    compute_outcome_values,
    relative_return,
    resolve_reference_price,
    status_for_bar_count,
)
from app.workers.shadow.outcomes.constants import (
    CALCULATION_VERSION,
    FORWARD_FRAME_VERSION,
    OUTCOME_COVERAGE_VERSION,
    OUTCOME_FINGERPRINT_VERSION,
    REFERENCE_PRICE_ROLE,
    STATUS_COMPLETE,
    STATUS_PARTIAL,
    STATUS_PENDING,
)
from app.workers.shadow.outcomes.fingerprints import (
    compute_forward_bars_hash,
    compute_outcome_fingerprint,
)


# Friday 2026-06-12: the next trading days are Mon 15, Tue 16, ... (weekend
# 13/14 must never count). All bars end well before NOW_UTC, so the latest
# bar is a completed prior session under ny_session_close.v1.
SNAPSHOT = date(2026, 6, 12)
NOW_UTC = datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)


def _bar(d: date, close: float = 100.0, high: float = None, low: float = None):
    return {
        "date": d.isoformat(),
        "open": close,
        "high": high if high is not None else close,
        "low": low if low is not None else close,
        "close": close,
        "volume": 1_000_000.0,
    }


def forward_weekdays(start: date, n: int):
    """The first n weekdays STRICTLY after `start` (calendar-honest)."""
    out = []
    d = start
    while len(out) < n:
        d = d + timedelta(days=1)
        if d.weekday() < 5:
            out.append(d)
    return out


def payload_with_forward(n: int, closes=None, include_snapshot=True,
                         snapshot_close: float = 100.0):
    bars = []
    if include_snapshot:
        bars.append(_bar(SNAPSHOT, snapshot_close))
    days = forward_weekdays(SNAPSHOT, n)
    for i, d in enumerate(days):
        close = closes[i] if closes else 100.0
        bars.append(_bar(d, close))
    return bars


FROZEN_LAST_BAR = _bar(SNAPSHOT, 100.0)


# --------------------------------------------------------------------------- #
# Reference semantics
# --------------------------------------------------------------------------- #

class TestReferencePrice:
    def test_frozen_frame_close_is_the_reference(self):
        price = resolve_reference_price(
            frame_last_bar=FROZEN_LAST_BAR,
            frame_bar_count=500,
            snapshot_date=SNAPSHOT,
            frame_last_date=SNAPSHOT,
        )
        assert price == 100.0

    def test_reference_role_is_verdict_neutral_constant(self):
        # One role for ENTER, WATCH and AVOID pairs — the metrics layer may
        # interpret arms, the shared outcome row never simulates a trade.
        assert REFERENCE_PRICE_ROLE == "paired_decision_observation"

    def test_empty_frame_rejects(self):
        with pytest.raises(ShadowOutcomeRejection) as exc:
            resolve_reference_price(
                frame_last_bar=None,
                frame_bar_count=0,
                snapshot_date=SNAPSHOT,
                frame_last_date=SNAPSHOT,
            )
        assert exc.value.reason_code == "empty_frozen_frame"

    def test_last_bar_date_must_equal_snapshot_date(self):
        with pytest.raises(ShadowOutcomeRejection) as exc:
            resolve_reference_price(
                frame_last_bar=_bar(SNAPSHOT - timedelta(days=1)),
                frame_bar_count=500,
                snapshot_date=SNAPSHOT,
                frame_last_date=SNAPSHOT - timedelta(days=1),
            )
        assert exc.value.reason_code == "frame_snapshot_date_mismatch"

    def test_last_bar_date_must_equal_frame_last_date(self):
        with pytest.raises(ShadowOutcomeRejection) as exc:
            resolve_reference_price(
                frame_last_bar=FROZEN_LAST_BAR,
                frame_bar_count=500,
                snapshot_date=SNAPSHOT,
                frame_last_date=SNAPSHOT + timedelta(days=3),
            )
        assert exc.value.reason_code == "frame_last_date_mismatch"

    @pytest.mark.parametrize("bad_close", [0.0, -5.0, float("nan"),
                                           float("inf"), "100", None, True])
    def test_invalid_frozen_close_rejects_safely(self, bad_close):
        bar = dict(FROZEN_LAST_BAR)
        bar["close"] = bad_close
        with pytest.raises(ShadowOutcomeRejection) as exc:
            resolve_reference_price(
                frame_last_bar=bar,
                frame_bar_count=500,
                snapshot_date=SNAPSHOT,
                frame_last_date=SNAPSHOT,
            )
        assert exc.value.reason_code == "invalid_frozen_close"

    def test_matching_fetched_close_is_not_a_revision(self):
        detected, note = check_reference_revision(100.0, _bar(SNAPSHOT, 100.0))
        assert detected is False and note is None

    def test_tiny_float_noise_is_not_a_revision(self):
        detected, _ = check_reference_revision(
            100.0, _bar(SNAPSHOT, 100.0 + 1e-10)
        )
        assert detected is False

    def test_diverging_fetched_close_sets_revision(self):
        detected, note = check_reference_revision(
            100.0, _bar(SNAPSHOT, 101.5), provider="massive"
        )
        assert detected is True
        assert note["reason_code"] == "reference_close_revision"
        assert note["existing_value"] == 100.0
        assert note["observed_value"] == 101.5
        assert note["provider"] == "massive"

    def test_missing_snapshot_bar_is_not_a_revision(self):
        detected, note = check_reference_revision(100.0, None)
        assert detected is False and note is None


# --------------------------------------------------------------------------- #
# Forward-bar alignment
# --------------------------------------------------------------------------- #

class TestForwardAlignment:
    def test_bars_strictly_after_snapshot_only(self):
        historical = payload_with_forward(3)
        seq = build_forward_sequence(historical, SNAPSHOT, now_utc=NOW_UTC)
        dates = [b["date"] for b in seq["forward_bars"]]
        assert all(date.fromisoformat(d) > SNAPSHOT for d in dates)
        # The snapshot-date bar is separated for the continuity check only.
        assert seq["snapshot_bar"]["date"] == SNAPSHOT.isoformat()
        assert seq["snapshot_bar"] not in seq["forward_bars"]

    def test_1d_is_first_forward_trading_bar_after_weekend(self):
        # Friday snapshot: 1D must be MONDAY (trading-bar order, not
        # calendar distance; the weekend never counts).
        historical = payload_with_forward(2)
        seq = build_forward_sequence(historical, SNAPSHOT, now_utc=NOW_UTC)
        assert seq["forward_bars"][0]["date"] == "2026-06-15"

    def test_holiday_gap_handled_by_order_not_dates(self):
        # Remove 2026-06-15 (as if a holiday): 1D becomes the NEXT session.
        historical = [b for b in payload_with_forward(3)
                      if b["date"] != "2026-06-15"]
        seq = build_forward_sequence(historical, SNAPSHOT, now_utc=NOW_UTC)
        assert seq["forward_bars"][0]["date"] == "2026-06-16"

    def test_partial_current_session_bar_excluded(self):
        last_day = forward_weekdays(SNAPSHOT, 2)[-1]
        historical = payload_with_forward(2)
        # 13:00 New York on the last bar's own date -> session in progress.
        during_session = datetime(
            last_day.year, last_day.month, last_day.day, 17, 0,
            tzinfo=timezone.utc,
        )
        seq = build_forward_sequence(historical, SNAPSHOT,
                                     now_utc=during_session)
        dates = [b["date"] for b in seq["forward_bars"]]
        assert last_day.isoformat() not in dates
        assert seq["completion"]["excluded_partial_bar_date"] == (
            last_day.isoformat()
        )

    def test_future_dated_bar_rejects_honestly(self):
        future = NOW_UTC.date() + timedelta(days=30)
        historical = payload_with_forward(2) + [_bar(future)]
        with pytest.raises(ShadowOutcomeRejection) as exc:
            build_forward_sequence(historical, SNAPSHOT, now_utc=NOW_UTC)
        assert exc.value.reason_code == "unconfirmed_bar_completion"

    def test_duplicate_forward_date_rejects(self):
        historical = payload_with_forward(2)
        historical.append(dict(historical[-1]))
        with pytest.raises(ShadowOutcomeRejection) as exc:
            build_forward_sequence(historical, SNAPSHOT, now_utc=NOW_UTC)
        assert exc.value.reason_code == "duplicate_session_date"

    @pytest.mark.parametrize("mutation", [
        {"close": None}, {"close": float("nan")}, {"close": float("inf")},
        {"high": "abc"}, {"low": -1.0}, {"volume": -5.0},
    ])
    def test_malformed_ohlcv_rejects(self, mutation):
        historical = payload_with_forward(2)
        historical[-1] = {**historical[-1], **mutation}
        with pytest.raises(ShadowOutcomeRejection) as exc:
            build_forward_sequence(historical, SNAPSHOT, now_utc=NOW_UTC)
        assert exc.value.reason_code == "malformed_ohlcv"

    def test_zero_forward_bars_is_not_a_rejection(self):
        historical = [_bar(SNAPSHOT)]
        seq = build_forward_sequence(historical, SNAPSHOT, now_utc=NOW_UTC)
        assert seq["forward_bars"] == []
        assert seq["snapshot_bar"]["date"] == SNAPSHOT.isoformat()

    def test_forward_sequence_capped_at_20(self):
        historical = payload_with_forward(25)
        seq = build_forward_sequence(historical, SNAPSHOT, now_utc=NOW_UTC)
        assert len(seq["forward_bars"]) == 20

    def test_provider_ordering_is_irrelevant(self):
        historical = payload_with_forward(5)
        seq_asc = build_forward_sequence(historical, SNAPSHOT, now_utc=NOW_UTC)
        seq_desc = build_forward_sequence(
            list(reversed(historical)), SNAPSHOT, now_utc=NOW_UTC
        )
        assert seq_asc["forward_bars"] == seq_desc["forward_bars"]


# --------------------------------------------------------------------------- #
# Maturity states and horizon NULL honesty
# --------------------------------------------------------------------------- #

class TestMaturity:
    @pytest.mark.parametrize("n,expected_status,null_windows,filled_windows", [
        (0, STATUS_PENDING, [1, 3, 5, 10, 20], []),
        (1, STATUS_PARTIAL, [3, 5, 10, 20], [1]),
        (4, STATUS_PARTIAL, [5, 10, 20], [1, 3]),
        (19, STATUS_PARTIAL, [20], [1, 3, 5, 10]),
        (20, STATUS_COMPLETE, [], [1, 3, 5, 10, 20]),
    ])
    def test_maturity_states_and_windows(self, n, expected_status,
                                         null_windows, filled_windows):
        historical = payload_with_forward(n)
        seq = build_forward_sequence(historical, SNAPSHOT, now_utc=NOW_UTC)
        values = compute_outcome_values(100.0, seq["forward_bars"])
        assert status_for_bar_count(values["available_forward_bars"]) == (
            expected_status
        )
        for w in null_windows:
            assert values["ret_by_window"][w] is None      # never zero-filled
        for w in filled_windows:
            assert values["ret_by_window"][w] is not None

    def test_no_nearest_date_substitution(self):
        # 2 forward bars: 3D must stay None even though a "close" 2nd bar
        # exists — the 3rd trading bar simply has not happened yet.
        historical = payload_with_forward(2)
        seq = build_forward_sequence(historical, SNAPSHOT, now_utc=NOW_UTC)
        values = compute_outcome_values(100.0, seq["forward_bars"])
        assert values["ret_by_window"][3] is None


# --------------------------------------------------------------------------- #
# Outcome math (raw upward-market returns from the frozen reference)
# --------------------------------------------------------------------------- #

class TestOutcomeMath:
    def test_returns_per_window(self):
        closes = [102.0, 104.0, 101.0, 99.0, 103.0] + [105.0] * 15
        historical = payload_with_forward(20, closes=closes)
        seq = build_forward_sequence(historical, SNAPSHOT, now_utc=NOW_UTC)
        values = compute_outcome_values(100.0, seq["forward_bars"])
        ret = values["ret_by_window"]
        assert ret[1] == pytest.approx(2.0)
        assert ret[3] == pytest.approx(1.0)
        assert ret[5] == pytest.approx(3.0)
        assert ret[10] == pytest.approx(5.0)
        assert ret[20] == pytest.approx(5.0)

    def test_mfe_mae_and_bar_count(self):
        days = forward_weekdays(SNAPSHOT, 3)
        historical = [_bar(SNAPSHOT, 100.0)] + [
            _bar(days[0], 101.0, high=106.0, low=97.0),
            _bar(days[1], 102.0, high=104.0, low=99.0),
            _bar(days[2], 100.5, high=103.0, low=95.0),
        ]
        seq = build_forward_sequence(historical, SNAPSHOT, now_utc=NOW_UTC)
        values = compute_outcome_values(100.0, seq["forward_bars"])
        assert values["max_favorable_excursion"] == pytest.approx(6.0)
        assert values["max_adverse_excursion"] == pytest.approx(-5.0)
        # A 3-bar excursion is labeled as exactly 3 bars — never 20.
        assert values["mfe_mae_bar_count"] == 3

    def test_no_forward_bars_leaves_excursions_null(self):
        values = compute_outcome_values(100.0, [])
        assert values["max_favorable_excursion"] is None
        assert values["max_adverse_excursion"] is None
        assert values["mfe_mae_bar_count"] is None

    def test_no_trade_semantics_in_outcome_values(self):
        values = compute_outcome_values(100.0, [])
        for forbidden in ("hit_stop", "hit_target", "simulated_r", "side",
                          "stop_price", "target_price",
                          "same_ticker_buy_hold"):
            assert forbidden not in values

    def test_benchmark_alignment_uses_benchmark_own_reference(self):
        spy_bars = payload_with_forward(2, closes=[402.0, 404.0],
                                        snapshot_close=400.0)
        spy_seq = build_forward_sequence(spy_bars, SNAPSHOT, now_utc=NOW_UTC)
        result = compute_benchmark_returns_for_pair({"SPY": spy_seq})
        assert result["SPY"]["1D"] == pytest.approx(0.5)   # 402/400
        assert result["SPY"]["3D"] is None

    def test_missing_benchmark_stays_null(self):
        result = compute_benchmark_returns_for_pair({"SPY": None, "QQQ": None})
        for bench in ("SPY", "QQQ"):
            assert set(result[bench]) == {"1D", "3D", "5D", "10D", "20D"}
            assert all(v is None for v in result[bench].values())

    def test_benchmark_without_snapshot_bar_stays_null(self):
        spy_bars = payload_with_forward(2, include_snapshot=False)
        spy_seq = build_forward_sequence(spy_bars, SNAPSHOT, now_utc=NOW_UTC)
        result = compute_benchmark_returns_for_pair({"SPY": spy_seq})
        assert all(v is None for v in result["SPY"].values())

    def test_relative_return(self):
        assert relative_return(3.0, 1.0) == pytest.approx(2.0)
        assert relative_return(None, 1.0) is None
        assert relative_return(3.0, None) is None


# --------------------------------------------------------------------------- #
# Fingerprints and forward hash
# --------------------------------------------------------------------------- #

class TestOutcomeFingerprint:
    def test_stable_across_maturation(self):
        # The fingerprint depends only on contract versions + the pair
        # fingerprint: it must NOT change while horizons mature.
        fp = compute_outcome_fingerprint(
            pair_fingerprint="pf-abc",
            pair_fingerprint_version="shadow_pair_fingerprint.v1",
        )
        again = compute_outcome_fingerprint(
            pair_fingerprint="pf-abc",
            pair_fingerprint_version="shadow_pair_fingerprint.v1",
        )
        assert fp == again

    def test_changes_with_pair_or_contract_identity(self):
        base = compute_outcome_fingerprint(
            pair_fingerprint="pf-abc",
            pair_fingerprint_version="shadow_pair_fingerprint.v1",
        )
        assert base != compute_outcome_fingerprint(
            pair_fingerprint="pf-other",
            pair_fingerprint_version="shadow_pair_fingerprint.v1",
        )
        assert base != compute_outcome_fingerprint(
            pair_fingerprint="pf-abc",
            pair_fingerprint_version="shadow_pair_fingerprint.v1",
            outcome_coverage_version="shadow_pair_outcomes.v2",
        )

    def test_contract_versions(self):
        assert CALCULATION_VERSION == "outcome.v1"
        assert OUTCOME_COVERAGE_VERSION == "shadow_pair_outcomes.v1"
        assert OUTCOME_FINGERPRINT_VERSION == "shadow_pair_outcome_fingerprint.v1"
        assert FORWARD_FRAME_VERSION == "shadow_forward_bars.v1"


class TestForwardBarsHash:
    def _bars(self):
        historical = payload_with_forward(3)
        return build_forward_sequence(
            historical, SNAPSHOT, now_utc=NOW_UTC
        )["forward_bars"]

    def _hash(self, bars, **overrides):
        kwargs = dict(symbol="DHR", provider="massive",
                      snapshot_date=SNAPSHOT, forward_bars=bars)
        kwargs.update(overrides)
        return compute_forward_bars_hash(**kwargs)

    def test_deterministic(self):
        bars = self._bars()
        assert self._hash(bars) == self._hash([dict(b) for b in bars])

    def test_changes_when_a_forward_date_changes(self):
        bars = self._bars()
        mutated = [dict(b) for b in bars]
        mutated[0]["date"] = "2026-06-16"
        assert self._hash(bars) != self._hash(mutated)

    def test_changes_when_any_ohlcv_value_changes(self):
        bars = self._bars()
        mutated = [dict(b) for b in bars]
        mutated[1]["low"] = mutated[1]["low"] - 0.01
        assert self._hash(bars) != self._hash(mutated)

    def test_changes_when_bar_order_changes(self):
        bars = self._bars()
        assert self._hash(bars) != self._hash(list(reversed(bars)))

    def test_changes_with_contract_version(self):
        bars = self._bars()
        assert self._hash(bars) != self._hash(
            bars, forward_frame_version="shadow_forward_bars.v2"
        )

    def test_changes_as_bars_mature(self):
        bars = self._bars()
        assert self._hash(bars[:2]) != self._hash(bars)

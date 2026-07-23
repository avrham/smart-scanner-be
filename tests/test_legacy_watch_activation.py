"""Phase 8 activation fix — legacy/manual WATCH persistence and telemetry.

Proves (fakes + deterministic fixtures only; no DB, no providers):
  * legacy v3 WATCH is evaluated but NOT persisted by default
  * persist_watch_candidates=True persists WATCH through save_signal with
    full Phase 7B provenance and a scan_run_signals occurrence link
  * WATCH increments watch_count, never rejected_count/"avoided"
  * immutable-signal accounting: non-persisted WATCH is never "linked";
    persisted new WATCH -> signals_created; duplicate -> signals_deduplicated
  * telemetry strategy identity is dynamic (sma150.v2 vs sma150.v3 + policy)
  * config telemetry is strategy-aware, bounded and secret-free
  * ENTER persistence, v2 output, funnel and scheduler defaults unchanged
  * no migration 009 was added
"""

import asyncio
import inspect
from pathlib import Path

import app.workers.scan_runner as scan_runner
from app.workers.strategies import get_strategy
from tests.sma150_v3_frames import build_uptrend_frame

from test_signal_provenance_persistence import _FakeConn, _patch_conn


MIGRATIONS = Path(__file__).resolve().parents[1] / "app" / "db" / "migrations"


def _async(value):
    async def _f(*args, **kwargs):
        return value
    return _f


def _frame_to_history(df):
    """FMP-shaped payload (newest first) from a deterministic test frame."""
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "date": r["date"].strftime("%Y-%m-%d"),
            "open": float(r["open"]), "high": float(r["high"]),
            "low": float(r["low"]), "close": float(r["close"]),
            "volume": float(r["volume"]),
        })
    rows.reverse()
    return {"historical": rows}


class _FakeProvider:
    """Deterministic in-memory provider; any real provider call would fail."""
    name = "fake"

    def __init__(self, payloads):
        self._payloads = payloads

    async def batch_historical_data(self, symbols, timeseries=350):
        return {s: self._payloads.get(s, {"historical": []}) for s in symbols}


# WATCH geometry: valid setup, failed trigger confirmation (v3).
_WATCH_DF = build_uptrend_frame(trigger=False, vol_ratio=1.30)
# ENTER geometry: valid setup + all confirmations (v2 ENTER and v3 ENTER).
_ENTER_DF = build_uptrend_frame(trigger=True, vol_ratio=1.30)


def _run_batch(
    monkeypatch,
    *,
    pattern_code,
    df,
    persist_watch=None,
    save_created_new=True,
    config_overrides=None,
    real_persistence=False,
):
    """Run run_scan_batch over one symbol with faked run/seen/save layers.

    real_persistence=True keeps the REAL save_signal and fakes only the DB
    connection (proves Phase 7B provenance + occurrence link end to end).
    """
    runs = {"created": [], "finalized": []}
    saved = []

    async def fake_create(**kwargs):
        runs["created"].append(kwargs)
        return kwargs["scan_run_id"]

    async def fake_finalize(**kwargs):
        runs["finalized"].append(kwargs)

    async def fake_save(**kwargs):
        saved.append(kwargs)
        return {
            "signal_id": f"sig-{len(saved)}",
            "created_new_signal": save_created_new,
            "deduplicated": not save_created_new,
        }

    async def fake_resolve(pattern, defaults):
        cfg = dict(defaults)
        if config_overrides:
            cfg.update(config_overrides)
        return cfg

    monkeypatch.setattr(scan_runner, "create_scan_run", fake_create)
    monkeypatch.setattr(scan_runner, "finalize_scan_run", fake_finalize)
    monkeypatch.setattr(scan_runner, "resolve_pattern_config", fake_resolve)
    monkeypatch.setattr(scan_runner, "was_seen_today", _async(False))
    monkeypatch.setattr(scan_runner, "mark_seen_today", _async(None))

    conn = None
    if real_persistence:
        conn = _FakeConn()
        _patch_conn(monkeypatch, conn)
    else:
        monkeypatch.setattr(scan_runner, "save_signal", fake_save)

    kwargs = {}
    if persist_watch is not None:
        kwargs["persist_watch_candidates"] = persist_watch

    summary = asyncio.run(
        scan_runner.run_scan_batch(
            _FakeProvider({"AAA": _frame_to_history(df)}),
            batch_size=1,
            pattern_code=pattern_code,
            symbols=["AAA"],
            ignore_seen=True,
            **kwargs,
        )
    )
    telemetry = runs["finalized"][0]["telemetry"]
    return summary, telemetry, saved, runs, conn


# --------------------------------------------------------------------------- #
# 1. Opt-in WATCH persistence
# --------------------------------------------------------------------------- #

class TestWatchPersistenceOptIn:
    def test_v3_watch_not_persisted_by_default(self, monkeypatch):
        summary, telemetry, saved, _, _ = _run_batch(
            monkeypatch, pattern_code="sma150_bounce_v3", df=_WATCH_DF
        )
        assert summary["success"] is True
        assert saved == []                       # evaluated, never persisted
        assert summary["watch_count"] == 1
        assert summary["watch_saved_count"] == 0
        # Non-persisted WATCH is never counted as linked.
        assert telemetry["signals_linked"] == 0
        assert telemetry["signals_created"] == 0
        assert telemetry["signals_deduplicated"] == 0
        # Still reported (bounded), just without a signal identity.
        assert summary["watch_signals"][0]["symbol"] == "AAA"
        assert "signal_id" not in summary["watch_signals"][0]

    def test_v3_watch_persisted_when_opted_in(self, monkeypatch):
        summary, telemetry, saved, _, _ = _run_batch(
            monkeypatch, pattern_code="sma150_bounce_v3", df=_WATCH_DF,
            persist_watch=True,
        )
        assert len(saved) == 1
        assert saved[0]["verdict"] == "WATCH"
        assert summary["watch_count"] == 1
        assert summary["watch_saved_count"] == 1
        assert telemetry["watch_saved_count"] == 1
        assert telemetry["signals_created"] == 1
        assert telemetry["signals_linked"] == 1
        entry = summary["watch_signals"][0]
        assert entry["signal_id"] == "sig-1"
        assert entry["signal_created_new"] is True
        # Bounded summary: never the full evidence bundle.
        assert "evidence" not in entry and "details" not in entry

    def test_explicit_false_matches_omitted(self, monkeypatch):
        summary, telemetry, saved, _, _ = _run_batch(
            monkeypatch, pattern_code="sma150_bounce_v3", df=_WATCH_DF,
            persist_watch=False,
        )
        assert saved == []
        assert summary["watch_count"] == 1
        assert telemetry["signals_linked"] == 0

    def test_persisted_watch_gets_provenance_and_occurrence_link(
        self, monkeypatch
    ):
        summary, telemetry, saved, runs, conn = _run_batch(
            monkeypatch, pattern_code="sma150_bounce_v3", df=_WATCH_DF,
            persist_watch=True, real_persistence=True,
        )
        # Immutable signal with full Phase 7B provenance.
        assert len(conn.signals_by_fp) == 1
        signal_id = next(iter(conn.signals_by_fp.values()))
        prov_args = conn.provenance_by_signal[signal_id]
        assert prov_args[5] == "sma150_bounce_v3"          # strategy_code
        assert prov_args[6] == "sma150.v3"                 # strategy_version
        assert prov_args[7] == "sma150_bounce.policy.v1"   # decision policy
        # scan_run_signals occurrence link to the SAME canonical run.
        assert len(conn.links) == 1
        link_scan_run_id, link_signal_id = conn.links[0][0], conn.links[0][1]
        assert str(link_scan_run_id) == summary["scan_id"]
        assert link_signal_id == signal_id
        assert telemetry["watch_saved_count"] == 1
        assert telemetry["signals_created"] == 1

    def test_duplicate_persisted_watch_counts_deduplicated(self, monkeypatch):
        summary, telemetry, saved, _, _ = _run_batch(
            monkeypatch, pattern_code="sma150_bounce_v3", df=_WATCH_DF,
            persist_watch=True, save_created_new=False,
        )
        assert telemetry["signals_created"] == 0
        assert telemetry["signals_deduplicated"] == 1
        assert telemetry["signals_linked"] == 1
        assert summary["watch_saved_count"] == 1
        assert summary["watch_signals"][0]["signal_created_new"] is False


# --------------------------------------------------------------------------- #
# 2. Verdict accounting
# --------------------------------------------------------------------------- #

class TestVerdictAccounting:
    def test_watch_is_never_a_rejection(self, monkeypatch):
        summary, telemetry, _, _, _ = _run_batch(
            monkeypatch, pattern_code="sma150_bounce_v3", df=_WATCH_DF
        )
        assert summary["watch_count"] == 1
        assert summary["rejected_count"] == 0
        assert telemetry["watch_count"] == 1
        assert telemetry["rejected_count"] == 0
        assert "avoided" not in telemetry["top_rejection_reasons"]

    def test_v3_enter_counts_and_persists_as_before(self, monkeypatch):
        summary, telemetry, saved, _, _ = _run_batch(
            monkeypatch, pattern_code="sma150_bounce_v3", df=_ENTER_DF
        )
        assert len(saved) == 1 and saved[0]["verdict"] == "ENTER"
        assert summary["enter_count"] == 1
        assert summary["watch_count"] == 0
        assert summary["rejected_count"] == 0
        assert summary["enter_signals"][0]["symbol"] == "AAA"

    def test_error_count_stays_separate(self, monkeypatch):
        summary, telemetry, saved, _, _ = _run_batch(
            monkeypatch, pattern_code="sma150_bounce_v3",
            df=_WATCH_DF.iloc[:50],   # too little history -> processed error
        )
        assert saved == []
        assert summary["error_count"] == 1
        assert summary["watch_count"] == 0
        assert summary["rejected_count"] == 0


# --------------------------------------------------------------------------- #
# 3. Dynamic strategy + config telemetry
# --------------------------------------------------------------------------- #

class TestDynamicTelemetry:
    def test_v3_telemetry_reports_v3_identity(self, monkeypatch):
        _, telemetry, _, _, _ = _run_batch(
            monkeypatch, pattern_code="sma150_bounce_v3", df=_WATCH_DF
        )
        assert telemetry["pattern"] == "sma150_bounce_v3"
        assert telemetry["strategy_version"] == "sma150.v3"
        assert telemetry["decision_policy_version"] == "sma150_bounce.policy.v1"
        # Backward-compatible key is now dynamic, not hard-coded.
        assert telemetry["score_version"] == "sma150.v3"

    def test_v2_telemetry_reports_v2_identity(self, monkeypatch):
        _, telemetry, saved, _, _ = _run_batch(
            monkeypatch, pattern_code="sma150_bounce", df=_ENTER_DF
        )
        assert telemetry["strategy_version"] == "sma150.v2"
        assert telemetry["score_version"] == "sma150.v2"
        # v2 keeps its implicit legacy decision policy identity.
        assert telemetry["decision_policy_version"] == "strategy_decision.v1"

    def test_v3_config_telemetry_exposes_v3_keys(self, monkeypatch):
        _, telemetry, _, _, _ = _run_batch(
            monkeypatch, pattern_code="sma150_bounce_v3", df=_WATCH_DF
        )
        cfg = telemetry["config_used"]
        for key in (
            "sma_window", "min_history_bars", "max_close_above_sma_pct",
            "max_close_below_sma_pct", "min_independent_bounces",
            "min_median_rebound_pct", "min_sma_slope_pct",
            "min_close_location_value", "min_trigger_volume_ratio",
            "bar_completion_policy", "exchange_timezone", "session_close_time",
        ):
            assert key in cfg, key
        # Strategy-aware: v2-only keys are absent from the v3 summary.
        assert "score_threshold" not in cfg

    def test_v2_config_telemetry_keeps_v2_keys(self, monkeypatch):
        _, telemetry, _, _, _ = _run_batch(
            monkeypatch, pattern_code="sma150_bounce", df=_ENTER_DF
        )
        cfg = telemetry["config_used"]
        for key in (
            "touch_tolerance_pct", "min_bounces", "min_avg_rebound_pct",
            "min_volume_sma_ratio", "min_price", "score_threshold",
        ):
            assert key in cfg, key

    def test_config_telemetry_excludes_secrets_and_is_bounded(
        self, monkeypatch
    ):
        _, telemetry, _, _, _ = _run_batch(
            monkeypatch, pattern_code="sma150_bounce_v3", df=_WATCH_DF,
            config_overrides={
                "api_key": "SECRET-VALUE",
                "database_dsn": "postgres://u:p@h/db",
                "huge_blob": "x" * 100_000,
            },
        )
        cfg = telemetry["config_used"]
        flat = str(cfg)
        assert "SECRET-VALUE" not in flat
        assert "postgres://" not in flat
        # Whitelisted keys only: non-decision keys never leak in.
        assert "huge_blob" not in cfg and "api_key" not in cfg

    def test_config_telemetry_is_deterministic(self, monkeypatch):
        _, t1, _, _, _ = _run_batch(
            monkeypatch, pattern_code="sma150_bounce_v3", df=_WATCH_DF
        )
        _, t2, _, _, _ = _run_batch(
            monkeypatch, pattern_code="sma150_bounce_v3", df=_WATCH_DF
        )
        assert t1["config_used"] == t2["config_used"]
        assert list(t1["config_used"]) == list(t2["config_used"])


# --------------------------------------------------------------------------- #
# 4. Existing behavior unchanged
# --------------------------------------------------------------------------- #

class TestUnchangedBehavior:
    def test_v2_enter_persistence_unchanged(self, monkeypatch):
        summary, telemetry, saved, _, _ = _run_batch(
            monkeypatch, pattern_code="sma150_bounce", df=_ENTER_DF
        )
        assert len(saved) == 1
        assert saved[0]["verdict"] == "ENTER"
        assert saved[0]["provenance"]["strategy_version"] == "sma150.v2"
        assert summary["enter_count"] == 1
        assert summary["enter_signals"][0]["symbol"] == "AAA"

    def test_v2_avoid_still_counts_rejected(self, monkeypatch):
        # Deterministic v2 AVOID (v2 emits only ENTER/AVOID, never WATCH).
        def fake_eval(symbol, df, config):
            return {
                "verdict": "AVOID", "score": 0.1, "reason": "weak",
                "details": {"snapshot_date": "2024-06-03",
                            "rejection_reason": "score_below_threshold"},
            }

        monkeypatch.setattr(scan_runner, "evaluate_sma150_bounce", fake_eval)
        summary, telemetry, saved, _, _ = _run_batch(
            monkeypatch, pattern_code="sma150_bounce", df=_ENTER_DF
        )
        assert saved == []
        assert summary["rejected_count"] == 1
        assert summary["watch_count"] == 0
        assert telemetry["rejected_count"] == 1
        assert telemetry["top_rejection_reasons"] == {
            "score_below_threshold": 1
        }

    def test_funnel_watch_default_unchanged(self):
        from app.workers.scanner.funnel import DEFAULT_SCANNER_CONFIG
        assert DEFAULT_SCANNER_CONFIG["persist_watch_candidates"] is True

    def test_legacy_and_scheduler_defaults_are_opted_out(self):
        sig = inspect.signature(scan_runner.run_scan_batch)
        assert sig.parameters["persist_watch_candidates"].default is False
        for fn in (scan_runner.process_single_symbol,
                   scan_runner.process_single_symbol_with_data):
            assert (inspect.signature(fn)
                    .parameters["persist_watch_candidates"].default is False)
        # The scheduler never opts in.
        scheduler_src = Path(
            inspect.getfile(__import__("app.workers.scheduler",
                                       fromlist=["scheduled_scan_job"]))
        ).read_text()
        assert "persist_watch" not in scheduler_src

    def test_admin_endpoint_passes_flag_to_legacy_path(self):
        import app.routers.admin as admin
        src = inspect.getsource(admin.start_scan)
        assert "persist_watch_candidates=legacy_persist_watch" in src
        # Opt-in requires an explicit true; None/False stay off.
        assert "persist_watch is True" in src

    def test_strategy_identity_helper_never_infers_from_name(self):
        # Identity comes from the registered strategy object itself.
        v3 = scan_runner._strategy_identity("sma150_bounce_v3")
        assert v3["strategy_version"] == get_strategy("sma150_bounce_v3").version
        unknown = scan_runner._strategy_identity("no_such_pattern")
        assert unknown["strategy_version"] is None
        assert unknown["decision_policy_version"] is None

    def test_activation_fix_added_no_schema_migration(self):
        """The legacy activation fix itself required no migration. Migrations
        009–012 are known later phases; the next boundary is 013."""
        assert [p.name for p in MIGRATIONS.glob("009_*")] == [
            "009_watch_outcome_coverage.sql"
        ]
        assert [p.name for p in MIGRATIONS.glob("010_*")] == [
            "010_sma150_shadow_evaluations.sql"
        ]
        assert [p.name for p in sorted(MIGRATIONS.glob("011_*"))] == [
            "011_shadow_pair_outcomes.sql"
        ]
        assert [p.name for p in sorted(MIGRATIONS.glob("012_*"))] == [
            "012_wyckoff_mtf_v2.sql"
        ]
        assert not list(MIGRATIONS.glob("013_*"))

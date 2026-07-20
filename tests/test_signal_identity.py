"""Phase 7B — immutable signal identity (fingerprint) regression tests.

COLLISION DIAGNOSIS (the bug these tests guard against):
Migration 001 declared UNIQUE(symbol, pattern_code, snapshot_date) on signals,
and save_signal upserted with ON CONFLICT on that key using DO UPDATE, while
signal_provenance upserted ON CONFLICT (signal_id) DO UPDATE. Consequence: a
second persistence with the same symbol/pattern/date but a DIFFERENT
strategy_version, config_hash, market_data_as_of or external observations
OVERWROTE BOTH the signal row (verdict/score/details/created_at) AND its
provenance row — silently destroying the first variant's evidence. That made
sma150.v2 vs v3, wyckoff_mtf.v1 vs v2, with/without Lorentzian, different
configs and different data snapshots mutually destructive on the same day.

THE FIX: identity is now a SHA-256 signal_fingerprint over the canonical
decision inputs. New fingerprint -> new immutable signal. Repeated exact
fingerprint -> the existing signal is REUSED (linked, never overwritten).
"""

import asyncio
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

import app.workers.persistence as persistence
from app.workers.provenance import (
    SIGNAL_FINGERPRINT_VERSION,
    build_evidence_snapshot,
    compute_signal_fingerprint,
)

from test_signal_provenance_persistence import _FakeConn, _patch_conn, _provenance


MIGRATIONS = Path(__file__).resolve().parents[1] / "app" / "db" / "migrations"


# --------------------------------------------------------------------------- #
# Schema-level proof: old uniqueness existed and is replaced
# --------------------------------------------------------------------------- #

def test_migration_001_had_colliding_uniqueness():
    """Documents the pre-7B collision source: symbol/pattern/date uniqueness."""
    sql = (MIGRATIONS / "001_initial_schema.sql").read_text()
    assert "UNIQUE(symbol, pattern_code, snapshot_date)" in sql


def test_migration_007_replaces_old_uniqueness_with_fingerprint():
    sql = (MIGRATIONS / "007_scan_signal_provenance.sql").read_text()
    # Immutable identity for new rows: (fingerprint, algorithm version).
    assert "signal_fingerprint" in sql
    assert "signal_fingerprint_version" in sql
    assert "signals_fingerprint_uniq" in sql
    assert "(signal_fingerprint, signal_fingerprint_version)" in sql
    assert "WHERE signal_fingerprint IS NOT NULL" in sql
    # Old constraint dropped so multiple immutable variants can coexist.
    assert "DROP CONSTRAINT IF EXISTS signals_symbol_pattern_code_snapshot_date_key" in sql
    # Legacy rows (NULL fingerprint) keep their historical dedup semantics.
    assert "signals_legacy_dedup_uniq" in sql
    assert "WHERE signal_fingerprint IS NULL" in sql
    # Occurrence-link table exists with the required shape.
    assert "CREATE TABLE IF NOT EXISTS public.scan_run_signals" in sql
    assert "created_new_signal BOOLEAN NOT NULL" in sql
    assert "PRIMARY KEY (scan_run_id, signal_id)" in sql


# --------------------------------------------------------------------------- #
# Fingerprint unit semantics
# --------------------------------------------------------------------------- #

def _fingerprint(**overrides):
    kwargs = dict(
        symbol="AAPL",
        strategy_code="sma150_bounce",
        strategy_version="sma150.v2",
        decision_policy_version="strategy_decision.v1",
        config_hash_value="cfg-hash-1",
        snapshot_date="2026-07-03",
        market_data_as_of=datetime(2026, 7, 3, 20, 0, tzinfo=timezone.utc),
        verdict="ENTER",
        evidence_original_sha256="e" * 64,
        external_observation_ids=[],
    )
    kwargs.update(overrides)
    return compute_signal_fingerprint(**kwargs)


def test_identical_decision_inputs_produce_identical_fingerprint():
    assert _fingerprint() == _fingerprint()


def test_each_meaningful_input_changes_the_fingerprint():
    base = _fingerprint()
    assert _fingerprint(strategy_version="sma150.v3") != base
    assert _fingerprint(config_hash_value="cfg-hash-2") != base
    assert _fingerprint(
        market_data_as_of=datetime(2026, 7, 3, 21, 0, tzinfo=timezone.utc)
    ) != base
    assert _fingerprint(verdict="WATCH") != base
    assert _fingerprint(evidence_original_sha256="f" * 64) != base
    assert _fingerprint(external_observation_ids=["obs:lorentzian:1"]) != base
    assert _fingerprint(snapshot_date="2026-07-04") != base
    assert _fingerprint(symbol="MSFT") != base


def test_fingerprint_version_participates_in_identity():
    """The algorithm version is hashed into the payload: a future v2 can
    never produce the same fingerprint string for the same inputs."""
    v1 = _fingerprint()
    v2 = _fingerprint(fingerprint_version="signal_fingerprint.v2")
    assert v1 != v2
    assert _fingerprint(fingerprint_version=SIGNAL_FINGERPRINT_VERSION) == v1


def test_fingerprint_hashes_original_evidence_not_pruned_snapshot():
    """Two evidence payloads that differ ONLY inside an optional list that
    pruning later removes must still be distinct immutable identities."""
    def _details(marker):
        return {
            "score_components": {"trend": 0.8},
            # Optional, oversized -> guaranteed to be pruned from storage.
            "bounces_detail": [{"i": i, "pad": "x" * 100, "marker": marker}
                               for i in range(5000)],
        }

    snap_a, meta_a = build_evidence_snapshot(_details("A"))
    snap_b, meta_b = build_evidence_snapshot(_details("B"))

    # The stored (pruned) snapshots are identical...
    assert snap_a == snap_b
    assert "bounces_detail" in meta_a["evidence_pruned_keys"]
    # ...but the ORIGINAL evidence hashes differ, so identities differ.
    assert meta_a["evidence_original_sha256"] != meta_b["evidence_original_sha256"]
    fp_a = _fingerprint(evidence_original_sha256=meta_a["evidence_original_sha256"])
    fp_b = _fingerprint(evidence_original_sha256=meta_b["evidence_original_sha256"])
    assert fp_a != fp_b


def test_evidence_key_order_does_not_change_identity():
    a = {"score_components": {"trend": 0.8, "volume": 0.5},
         "thresholds_used": {"score_threshold": 0.5}}
    b = {"thresholds_used": {"score_threshold": 0.5},
         "score_components": {"volume": 0.5, "trend": 0.8}}
    _, meta_a = build_evidence_snapshot(a)
    _, meta_b = build_evidence_snapshot(b)
    assert meta_a["evidence_original_sha256"] == meta_b["evidence_original_sha256"]
    assert (_fingerprint(evidence_original_sha256=meta_a["evidence_original_sha256"])
            == _fingerprint(evidence_original_sha256=meta_b["evidence_original_sha256"]))


def test_external_observation_ids_are_order_insensitive():
    a = _fingerprint(external_observation_ids=["obs:b", "obs:a"])
    b = _fingerprint(external_observation_ids=["obs:a", "obs:b"])
    assert a == b


def test_naive_and_utc_as_of_normalize_identically():
    naive = _fingerprint(market_data_as_of=datetime(2026, 7, 3, 20, 0))
    aware = _fingerprint(
        market_data_as_of=datetime(2026, 7, 3, 20, 0, tzinfo=timezone.utc)
    )
    assert naive == aware


# --------------------------------------------------------------------------- #
# Persistence-level: variants coexist, exact repeats deduplicate
# --------------------------------------------------------------------------- #

def _save(conn_provenance, **kwargs):
    defaults = dict(
        symbol="AAA",
        pattern_code="sma150_bounce",
        verdict="ENTER",
        score=0.9,
        provenance=conn_provenance,
    )
    defaults.update(kwargs)
    return asyncio.run(persistence.save_signal(**defaults))


def test_different_strategy_versions_create_two_signals(monkeypatch):
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)

    r_v2 = _save(_provenance(strategy_version="sma150.v2"))
    r_v3 = _save(_provenance(strategy_version="sma150.v3"))

    assert r_v2["signal_id"] != r_v3["signal_id"]
    assert r_v2["created_new_signal"] and r_v3["created_new_signal"]
    assert len(conn.signals_by_fp) == 2
    assert len(conn.provenance_by_signal) == 2


def test_different_config_hashes_create_two_signals(monkeypatch):
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)

    r1 = _save(_provenance(config_hash="cfg-a"))
    r2 = _save(_provenance(config_hash="cfg-b"))
    assert r1["signal_id"] != r2["signal_id"]
    assert len(conn.signals_by_fp) == 2


def test_different_market_data_as_of_create_two_signals(monkeypatch):
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)

    r1 = _save(_provenance(
        market_data_as_of=datetime(2026, 7, 3, 20, 0, tzinfo=timezone.utc)))
    r2 = _save(_provenance(
        market_data_as_of=datetime(2026, 7, 3, 21, 30, tzinfo=timezone.utc)))
    assert r1["signal_id"] != r2["signal_id"]
    assert len(conn.signals_by_fp) == 2


def test_different_external_observations_create_two_signals(monkeypatch):
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)

    r_plain = _save(_provenance(external_observation_ids=[]))
    r_lorentzian = _save(
        _provenance(external_observation_ids=["obs:lorentzian:2026-07-03:AAA"])
    )
    assert r_plain["signal_id"] != r_lorentzian["signal_id"]
    assert len(conn.signals_by_fp) == 2


def test_fingerprint_version_is_persisted_for_all_new_signals(monkeypatch):
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)

    result = _save(_provenance())
    assert result["signal_fingerprint_version"] == SIGNAL_FINGERPRINT_VERSION
    # The version is written on the signal row itself ($12 in the insert).
    assert conn.signal_insert_args[0][11] == SIGNAL_FINGERPRINT_VERSION


def test_migration_prevents_fingerprint_version_partial_states():
    """The CHECK constraint forbids fingerprint-without-version and
    version-without-fingerprint, while legacy rows (both NULL) stay valid."""
    sql = (MIGRATIONS / "007_scan_signal_provenance.sql").read_text()
    assert "signals_fingerprint_version_pairing_check" in sql
    assert (
        "CHECK ((signal_fingerprint IS NULL) = (signal_fingerprint_version IS NULL))"
        in sql
    )
    # No fabrication for legacy rows: the migration must not UPDATE signals.
    assert "UPDATE public.signals" not in sql
    assert "UPDATE signals" not in sql


def test_exact_identical_fingerprints_reuse_one_signal(monkeypatch):
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)

    prov = _provenance()
    first = _save(dict(prov))
    second = _save(dict(prov))

    assert first["created_new_signal"] is True
    assert second["created_new_signal"] is False
    assert second["deduplicated"] is True
    assert second["signal_id"] == first["signal_id"]
    assert len(conn.signals_by_fp) == 1
    assert len(conn.provenance_by_signal) == 1


def test_two_scans_same_fingerprint_two_occurrence_links(monkeypatch):
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)

    scan_a, scan_b = str(uuid.uuid4()), str(uuid.uuid4())
    first = _save(_provenance(scan_run_id=scan_a))
    second = _save(_provenance(scan_run_id=scan_b))

    assert second["signal_id"] == first["signal_id"]
    assert len(conn.links) == 2
    link_runs = {str(link[0]) for link in conn.links}
    assert link_runs == {scan_a, scan_b}
    # First link is the origin creation, second is a re-detection.
    assert conn.links[0][3] is True
    assert conn.links[1][3] is False


def test_provenance_never_overwritten_by_later_scan(monkeypatch):
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)

    origin_scan = str(uuid.uuid4())
    first = _save(_provenance(scan_run_id=origin_scan))
    original_prov = dict(conn.provenance_by_signal)

    _save(_provenance(scan_run_id=str(uuid.uuid4())))

    # Same single provenance row, same origin scan_run_id, byte-for-byte.
    assert conn.provenance_by_signal == original_prov
    prov_args = next(iter(conn.provenance_by_signal.values()))
    assert str(prov_args[1]) == origin_scan
    assert str(list(conn.provenance_by_signal)[0]) == first["signal_id"]


def test_incompatible_provenance_on_fingerprint_reuse_is_rejected(monkeypatch):
    """Defense-in-depth: a fingerprint reuse whose stored identity disagrees
    (hash collision / corrupted row) must refuse rather than silently link."""
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)

    prov = _provenance()
    _save(dict(prov))

    # Corrupt the stored provenance identity to simulate a mismatch.
    sig_id = next(iter(conn.provenance_by_signal))
    args = list(conn.provenance_by_signal[sig_id])
    args[6] = "sma150.v999"  # strategy_version
    conn.provenance_by_signal[sig_id] = tuple(args)

    with pytest.raises(ValueError, match="incompatible provenance"):
        _save(dict(prov))


# --------------------------------------------------------------------------- #
# Explicit coexistence proofs
# --------------------------------------------------------------------------- #

def test_sma150_v2_and_v3_coexist_on_same_symbol_and_date(monkeypatch):
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)

    v2 = _save(_provenance(strategy_version="sma150.v2"),
               symbol="NVDA", verdict="ENTER")
    v3 = _save(_provenance(strategy_version="sma150.v3"),
               symbol="NVDA", verdict="WATCH")

    assert v2["signal_id"] != v3["signal_id"]
    assert len(conn.signals_by_fp) == 2  # both variants persisted, none lost


def test_wyckoff_with_and_without_lorentzian_coexist(monkeypatch):
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)

    base = dict(
        strategy_code="wyckoff_mtf",
        strategy_version="wyckoff_mtf.v2",
    )
    without = _save(
        _provenance(**base, external_observation_ids=[]),
        symbol="NVDA", pattern_code="wyckoff_mtf",
    )
    with_lorentzian = _save(
        _provenance(**base,
                    external_observation_ids=["obs:lorentzian:2026-07-03:NVDA"]),
        symbol="NVDA", pattern_code="wyckoff_mtf",
    )

    assert without["signal_id"] != with_lorentzian["signal_id"]
    assert len(conn.signals_by_fp) == 2


def test_wyckoff_v1_and_v2_coexist(monkeypatch):
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)

    v1 = _save(_provenance(strategy_code="wyckoff_mtf",
                           strategy_version="wyckoff_mtf.v1"),
               pattern_code="wyckoff_mtf")
    v2 = _save(_provenance(strategy_code="wyckoff_mtf",
                           strategy_version="wyckoff_mtf.v2"),
               pattern_code="wyckoff_mtf")
    assert v1["signal_id"] != v2["signal_id"]


# --------------------------------------------------------------------------- #
# Outcome separation by immutable variant
# --------------------------------------------------------------------------- #

def test_outcomes_remain_separated_by_immutable_variant(monkeypatch):
    """signal_outcomes is keyed 1:1 by signal_id: distinct immutable variants
    yield distinct signal ids (hence distinct outcome rows), while an exact
    repeated scan reuses one signal id (hence exactly one outcome row)."""
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)

    v2_first = _save(_provenance(strategy_version="sma150.v2"))
    v2_repeat = _save(_provenance(strategy_version="sma150.v2"))
    v3 = _save(_provenance(strategy_version="sma150.v3"))

    # Exact repeat -> same outcome target; new version -> separate target.
    assert v2_repeat["signal_id"] == v2_first["signal_id"]
    assert v3["signal_id"] != v2_first["signal_id"]
    outcome_targets = {v2_first["signal_id"], v2_repeat["signal_id"], v3["signal_id"]}
    assert len(outcome_targets) == 2

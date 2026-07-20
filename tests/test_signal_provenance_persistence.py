"""Phase 7B — canonical transactional signal+provenance persistence.

Uses a fake asyncpg connection with a tiny in-memory store (no DB). Verifies:
  * save_signal refuses to write a signal without provenance
  * signal + provenance + scan occurrence link are written in ONE transaction
  * a failed provenance insert rolls back the signal insert (no orphans)
  * oversized evidence rejects persistence before anything is written
  * no code path outside app/workers/persistence.py inserts into signals
"""

import asyncio
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

import app.workers.persistence as persistence
from app.workers.provenance import EvidenceTooLargeError


APP_DIR = Path(__file__).resolve().parents[1] / "app"


def _provenance(**overrides):
    rec = {
        "scan_run_id": str(uuid.uuid4()),
        "source_path": "funnel",
        "scanner_mode": "funnel",
        "provider": "massive",
        "strategy_code": "sma150_bounce",
        "strategy_version": "sma150.v2",
        "decision_policy_version": "strategy_decision.v1",
        "provenance_version": "provenance.v1",
        "config_hash": "abc123",
        "config_snapshot": {"strategy_config": {"min_price": 5.0}, "scanner": {}},
        "market_data_as_of": datetime(2026, 7, 3, tzinfo=timezone.utc),
        "evidence_snapshot": {"snapshot_date": "2026-07-03"},
        "evidence_original_sha256": "deadbeef",
        "evidence_original_size_bytes": 42,
        "evidence_pruned": False,
        "evidence_pruned_keys": [],
        "external_observation_ids": [],
    }
    rec.update(overrides)
    return rec


class _FakeTransaction:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        self.conn.in_transaction = True
        self.conn._snapshot = (
            dict(self.conn.signals_by_fp),
            dict(self.conn.provenance_by_signal),
            list(self.conn.links),
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.conn.in_transaction = False
        if exc_type is not None:
            self.conn.rolled_back = True
            # asyncpg discards writes made inside a failed transaction.
            (self.conn.signals_by_fp,
             self.conn.provenance_by_signal,
             self.conn.links) = self.conn._snapshot
        else:
            self.conn.committed = True
        return False


class _FakeConn:
    """Mini signals/provenance/links store keyed by signal_fingerprint."""

    def __init__(self, fail_provenance=False):
        self.fail_provenance = fail_provenance
        self.in_transaction = False
        self.rolled_back = False
        self.committed = False
        self.signals_by_fp = {}          # fingerprint -> signal id
        self.provenance_by_signal = {}   # signal id -> args tuple
        self.links = []                  # (scan_run_id, signal_id, source, created_new)
        self.signal_insert_args = []     # every INSERT INTO signals args tuple
        self.writes_outside_transaction = []

    def transaction(self):
        return _FakeTransaction(self)

    async def fetchrow(self, query, *args):
        if "INSERT INTO signals" in query:
            if not self.in_transaction:
                self.writes_outside_transaction.append("signals")
            self.signal_insert_args.append(args)
            fingerprint = args[10]
            if fingerprint in self.signals_by_fp:
                return None  # ON CONFLICT DO NOTHING
            self.signals_by_fp[fingerprint] = args[0]
            return {"id": args[0]}
        if "WHERE s.signal_fingerprint" in query:
            signal_id = self.signals_by_fp.get(args[0])
            if signal_id is None:
                return None
            prov = self.provenance_by_signal.get(signal_id)
            return {
                "id": signal_id,
                "strategy_code": prov[5] if prov else None,
                "strategy_version": prov[6] if prov else None,
                "decision_policy_version": prov[7] if prov else None,
                "config_hash": prov[9] if prov else None,
            }
        raise AssertionError(f"unexpected fetchrow: {query}")

    async def execute(self, query, *args):
        if not self.in_transaction:
            self.writes_outside_transaction.append("execute")
        if "INSERT INTO signal_provenance" in query:
            if self.fail_provenance:
                raise RuntimeError("provenance insert failed")
            self.provenance_by_signal[args[0]] = args
            return
        if "INSERT INTO scan_run_signals" in query:
            self.links.append(args)
            return
        raise AssertionError(f"unexpected execute: {query}")


def _patch_conn(monkeypatch, conn):
    async def _get():
        return conn

    async def _release(c):
        return None

    monkeypatch.setattr(persistence, "get_db_connection", _get)
    monkeypatch.setattr(persistence, "release_db_connection", _release)


def _save(**kwargs):
    defaults = dict(
        symbol="AAA",
        pattern_code="sma150_bounce",
        verdict="ENTER",
        score=0.9,
        provenance=_provenance(),
    )
    defaults.update(kwargs)
    return asyncio.run(persistence.save_signal(**defaults))


def test_save_signal_requires_provenance(monkeypatch):
    _patch_conn(monkeypatch, _FakeConn())
    with pytest.raises(ValueError, match="provenance"):
        _save(provenance=None)


def test_signal_provenance_and_link_written_in_one_transaction(monkeypatch):
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)

    result = _save()

    assert result["created_new_signal"] is True
    assert result["deduplicated"] is False
    assert conn.committed is True
    assert conn.rolled_back is False
    assert conn.writes_outside_transaction == []
    assert len(conn.provenance_by_signal) == 1
    assert len(conn.links) == 1
    # Provenance + link reference the SAME signal id that was returned.
    assert str(list(conn.provenance_by_signal)[0]) == result["signal_id"]
    assert str(conn.links[0][1]) == result["signal_id"]
    assert conn.links[0][3] is True  # created_new_signal on the link


def test_failed_provenance_insert_rolls_back_signal(monkeypatch):
    conn = _FakeConn(fail_provenance=True)
    _patch_conn(monkeypatch, conn)

    with pytest.raises(RuntimeError, match="provenance insert failed"):
        _save()

    assert conn.rolled_back is True
    # Neither row survives: no orphan signal without provenance, no link.
    assert conn.signals_by_fp == {}
    assert conn.provenance_by_signal == {}
    assert conn.links == []


def test_oversized_evidence_rejects_persistence_before_any_write(monkeypatch):
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)

    huge_evidence = {"score_components": {"pad": "x" * (70 * 1024)}}
    with pytest.raises(EvidenceTooLargeError):
        _save(provenance=_provenance(evidence_snapshot=huge_evidence))

    assert conn.signals_by_fp == {}
    assert conn.provenance_by_signal == {}
    assert conn.links == []


def test_provenance_timestamps_are_timezone_aware(monkeypatch):
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)

    _save()
    prov_args = next(iter(conn.provenance_by_signal.values()))
    as_of = prov_args[11]  # market_data_as_of param
    assert as_of.tzinfo is not None
    assert as_of.utcoffset().total_seconds() == 0


def test_external_observation_ids_persisted_as_empty_array(monkeypatch):
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)

    _save()
    prov_args = next(iter(conn.provenance_by_signal.values()))
    assert prov_args[17] == "[]"  # JSON empty array


def test_manual_source_without_scan_run_gets_no_link(monkeypatch):
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)

    result = _save(
        provenance=_provenance(scan_run_id=None, source_path="manual")
    )
    assert result["created_new_signal"] is True
    assert conn.links == []  # documented no-scan-context path


def test_no_direct_signal_inserts_bypass_canonical_helper():
    """Source scan: INSERT INTO signals may only exist in persistence.py."""
    offenders = []
    for path in APP_DIR.rglob("*.py"):
        text = path.read_text(encoding="utf-8", errors="ignore")
        if "INSERT INTO signals" in text and path.name != "persistence.py":
            offenders.append(str(path))
    assert offenders == [], f"direct signal inserts found: {offenders}"

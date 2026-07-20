"""Signal provenance building blocks (Phase 7B).

Pure helpers (no I/O) that turn a strategy evaluation into an immutable,
reproducible provenance record:

  * config sanitization (secret-shaped keys never persisted)
  * canonical JSON + SHA-256 config hashing (insertion-order independent)
  * bounded evidence snapshots (deterministic evidence only, never invented;
    deterministic pruning that can never drop the core decision inputs)
  * market-data as-of extraction from the actually-evaluated dataframe
  * the immutable signal fingerprint (identity of one exact decision)

Version identities kept deliberately separate (do NOT conflate):
  * strategy_version          — the strategy's own rules (e.g. sma150.v2)
  * DECISION_POLICY_VERSION   — how decisions map to verdicts/persistence
  * PROVENANCE_VERSION        — the shape of the provenance record itself
  * decision-card card_version and outcome calculation_version live elsewhere
"""

import hashlib
import json
import re
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

PROVENANCE_VERSION = "provenance.v1"
# The strategies currently embed their decision rules (ENTER/WATCH/AVOID/
# REJECT mapping + persistence gates). This names that implicit policy so
# future explicit decision policies (Phase 11) are separately versioned.
DECISION_POLICY_VERSION = "strategy_decision.v1"
# Version of the fingerprint ALGORITHM itself (payload shape + hashing rules).
# Persisted next to every fingerprint and included in the hashed payload, so
# a future algorithm change (v2) can never be confused with — or collide
# into — v1 identities. Legacy rows keep both fingerprint and version NULL.
SIGNAL_FINGERPRINT_VERSION = "signal_fingerprint.v1"

# Evidence snapshots are bounded: one malformed strategy result must not be
# able to create an unbounded row. Pruning is deterministic (largest optional
# key first, key name as tiebreak) and NEVER removes mandatory decision
# inputs; if the mandatory-only snapshot still exceeds the bound, persistence
# is rejected (EvidenceTooLargeError) and the whole signal write is aborted.
MAX_EVIDENCE_BYTES = 64 * 1024

# Core decision inputs that may never be pruned when present.
MANDATORY_EVIDENCE_KEYS = frozenset({
    "decision",
    "verdict",
    "score_components",
    "thresholds_used",
    "trigger_needed",
    "confirmation_needed",
    "missing_data",
    "rejection_reason",
    "waiting_reason",
    "timeframe_summary",
    "timeframes",
    "snapshot_date",
    "market_data_as_of_missing_reason",
    "decision_card_evidence",   # holds trigger/confirmation/missing_data
    "strategy_evidence_identity",
})

# Keys copied from StrategyResult.details / decision card when present.
# Nothing is invented: absent keys stay absent.
_DETAIL_EVIDENCE_KEYS = (
    "score_components",
    "thresholds_used",
    "bounces_detail",
    "trend_context",
    "rejection_reason",
    "waiting_reason",
    "timeframes",
    "timeframe_summary",
    "snapshot_date",
    "vol_ratio",
    "proximity_pct",
    "sma_value",
    "current_price",
    "bounce_count",
    "avg_rebound_pct",
)
_CARD_EVIDENCE_KEYS = (
    "raw_evidence",
    "missing_data",
    "trigger_needed",
    "confirmation_needed",
    "invalidation",
    "why_now",
)

_SECRET_KEY_RE = re.compile(
    r"(api[_-]?key|apikey|token|secret|password|authorization|credential|"
    r"private[_-]?key|dsn|service[_-]?key)",
    re.IGNORECASE,
)
_SECRET_VALUE_RE = re.compile(r"(apiKey=|Bearer\s+\S+|postgres(ql)?://)", re.IGNORECASE)


class EvidenceTooLargeError(ValueError):
    """Mandatory evidence alone exceeds the bound — persistence must abort."""


# --------------------------------------------------------------------------- #
# Config sanitization + canonical hashing
# --------------------------------------------------------------------------- #

def sanitize_config(value: Any) -> Any:
    """Recursively drop secret-shaped keys and credential-looking string values.

    Applied before hashing AND before persistence, so a secret can neither be
    stored nor leak through the hash.
    """
    if isinstance(value, dict):
        return {
            k: sanitize_config(v)
            for k, v in value.items()
            if not _SECRET_KEY_RE.search(str(k))
        }
    if isinstance(value, list):
        return [sanitize_config(v) for v in value]
    if isinstance(value, str) and _SECRET_VALUE_RE.search(value):
        return "***excluded***"
    return value


def canonical_json(obj: Any) -> str:
    """Deterministic JSON: recursively sorted object keys, compact separators.

    List order is preserved (it is semantic — e.g. ordered thresholds); dict
    insertion order is irrelevant by construction.
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def config_hash(config: Dict[str, Any]) -> str:
    """SHA-256 of the canonical JSON of the SANITIZED config."""
    return _sha256(canonical_json(sanitize_config(config)))


def build_config_snapshot(
    strategy_config: Dict[str, Any],
    scanner_settings: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Safe, decision-relevant configuration snapshot.

    Includes the resolved strategy config plus the scanner settings that affect
    candidate behavior (limit, expensive-stage flags, persistence flags).
    Never includes credentials, URLs with secrets, or unrelated app settings.
    """
    snapshot = {
        "strategy_config": sanitize_config(strategy_config or {}),
        "scanner": sanitize_config(scanner_settings or {}),
    }
    return snapshot


# --------------------------------------------------------------------------- #
# Evidence snapshot (bounded, deterministic-only)
# --------------------------------------------------------------------------- #

def build_evidence_snapshot(
    details: Optional[Dict[str, Any]],
    score_components: Optional[Dict[str, Any]] = None,
    decision_card: Optional[Dict[str, Any]] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Immutable snapshot of the deterministic evidence that already exists.

    Copies known evidence keys (a whitelist — this IS the sanitization: only
    known deterministic fields can enter) from the strategy details and
    decision card. Missing keys are NOT invented; no LLM prose is ever
    included. `extra` adds caller-supplied deterministic facts (e.g. an
    explicit missing-as-of reason) BEFORE hashing, so the original hash
    covers the complete evidence.

    Returns (snapshot, meta) where meta records reproducible pruning facts
    about the COMPLETE ORIGINAL evidence (computed before any pruning):
      evidence_original_sha256, evidence_original_size_bytes,
      evidence_pruned, evidence_pruned_keys.

    Raises EvidenceTooLargeError when even the mandatory decision inputs
    exceed MAX_EVIDENCE_BYTES — callers must abort the signal write.
    """
    details = details or {}
    snapshot: Dict[str, Any] = {}

    for key in _DETAIL_EVIDENCE_KEYS:
        if key in details:
            snapshot[key] = details[key]

    if score_components and "score_components" not in snapshot:
        snapshot["score_components"] = score_components

    card = decision_card or details.get("decision_card")
    if isinstance(card, dict):
        card_evidence = {k: card[k] for k in _CARD_EVIDENCE_KEYS if k in card}
        if card_evidence:
            snapshot["decision_card_evidence"] = card_evidence

    if extra:
        snapshot.update(extra)

    return _bound_evidence(snapshot)


def _json_size(obj: Any) -> int:
    return len(canonical_json(obj).encode("utf-8"))


def _bound_evidence(
    snapshot: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Deterministically bound the snapshot to MAX_EVIDENCE_BYTES.

    Only OPTIONAL keys (not in MANDATORY_EVIDENCE_KEYS) may be pruned, largest
    serialized size first (key name ascending as a deterministic tiebreak).
    The original hash/size are always recorded so pruning is reproducible and
    auditable. If the mandatory-only snapshot still exceeds the bound, raise —
    a snapshot that lost its core decision inputs must never be stored.
    """
    original_json = canonical_json(snapshot)
    meta: Dict[str, Any] = {
        "evidence_original_sha256": _sha256(original_json),
        "evidence_original_size_bytes": len(original_json.encode("utf-8")),
        "evidence_pruned": False,
        "evidence_pruned_keys": [],
    }

    if meta["evidence_original_size_bytes"] <= MAX_EVIDENCE_BYTES:
        return snapshot, meta

    bounded = dict(snapshot)
    optional = [k for k in bounded if k not in MANDATORY_EVIDENCE_KEYS]
    # Deterministic prune order: largest first, then key name.
    optional.sort(key=lambda k: (-_json_size(bounded[k]), k))

    pruned: List[str] = []
    for key in optional:
        if _json_size(bounded) <= MAX_EVIDENCE_BYTES:
            break
        bounded.pop(key)
        pruned.append(key)

    if _json_size(bounded) > MAX_EVIDENCE_BYTES:
        raise EvidenceTooLargeError(
            "mandatory evidence exceeds the "
            f"{MAX_EVIDENCE_BYTES}-byte bound; refusing to persist a snapshot "
            "without its core decision inputs"
        )

    meta["evidence_pruned"] = True
    meta["evidence_pruned_keys"] = pruned
    return bounded, meta


# --------------------------------------------------------------------------- #
# Market-data as-of
# --------------------------------------------------------------------------- #

def market_data_as_of_from_df(df: Optional[pd.DataFrame]) -> Optional[datetime]:
    """Latest bar timestamp actually present in the evaluated dataframe (UTC).

    Never derives from insertion time, server time, or provider response time.
    Returns None when no trustworthy timestamp exists (callers persist NULL and
    an explicit missing reason instead of inventing one).
    """
    if df is None or getattr(df, "empty", True) or "date" not in getattr(df, "columns", []):
        return None
    try:
        latest = pd.to_datetime(df["date"].iloc[-1])
    except Exception:
        return None
    if pd.isna(latest):
        return None
    py_dt = latest.to_pydatetime()
    if py_dt.tzinfo is None:
        return py_dt.replace(tzinfo=timezone.utc)
    return py_dt.astimezone(timezone.utc)


# --------------------------------------------------------------------------- #
# Immutable signal fingerprint
# --------------------------------------------------------------------------- #

def _utc_iso(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def compute_signal_fingerprint(
    *,
    symbol: str,
    strategy_code: str,
    strategy_version: str,
    decision_policy_version: str,
    config_hash_value: str,
    snapshot_date: Any,
    market_data_as_of: Optional[datetime],
    verdict: str,
    evidence_original_sha256: str,
    external_observation_ids: Optional[List[Any]] = None,
    fingerprint_version: str = SIGNAL_FINGERPRINT_VERSION,
) -> str:
    """SHA-256 identity of ONE immutable decision (signal_fingerprint.v1).

    The exact same decision inputs always produce the same fingerprint; a
    meaningful change in evidence, version, config, data as-of or observations
    produces a different one. scan_run_id is deliberately EXCLUDED: repeated
    scans re-detecting the same decision reuse the same immutable signal.

    Evidence enters as `evidence_original_sha256` — the hash of the COMPLETE
    canonical evidence BEFORE size pruning — so two decisions that differ only
    in optional evidence later removed by pruning still get distinct
    identities, while dictionary insertion order can never split identities
    (the hash is over recursively key-sorted canonical JSON).
    """
    payload = {
        "fingerprint_version": fingerprint_version,
        "symbol": symbol,
        "strategy_code": strategy_code,
        "strategy_version": strategy_version,
        "decision_policy_version": decision_policy_version,
        "config_hash": config_hash_value,
        "snapshot_date": str(snapshot_date) if snapshot_date is not None else None,
        "market_data_as_of": _utc_iso(market_data_as_of),
        "verdict": verdict,
        "evidence_sha256": evidence_original_sha256,
        # Set-like identifiers: sorted so ordering can never split identities.
        "external_observation_ids": sorted(
            str(x) for x in (external_observation_ids or [])
        ),
    }
    return _sha256(canonical_json(payload))


# --------------------------------------------------------------------------- #
# Full provenance record
# --------------------------------------------------------------------------- #

def build_provenance(
    *,
    scan_run_id: Optional[str],
    source_path: str,
    scanner_mode: Optional[str],
    provider: Optional[str],
    strategy_code: str,
    strategy_version: str,
    strategy_config: Dict[str, Any],
    scanner_settings: Optional[Dict[str, Any]] = None,
    details: Optional[Dict[str, Any]] = None,
    score_components: Optional[Dict[str, Any]] = None,
    decision_card: Optional[Dict[str, Any]] = None,
    market_data_as_of: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Assemble the provenance dict persisted 1:1 with a signal.

    external_observation_ids stays [] for internal-only signals (Phase 10
    readiness — never filled with placeholder IDs).

    Raises EvidenceTooLargeError if even the mandatory evidence exceeds the
    bound (the caller must NOT persist the signal in that case).
    """
    snapshot = build_config_snapshot(strategy_config, scanner_settings)
    extra = None
    if market_data_as_of is None:
        # Explicit missing-data reason instead of an invented timestamp —
        # added BEFORE hashing so the original evidence hash covers it.
        extra = {
            "market_data_as_of_missing_reason":
                "no trustworthy bar timestamp in evaluated data"
        }
    evidence, evidence_meta = build_evidence_snapshot(
        details, score_components, decision_card, extra=extra
    )

    return {
        "scan_run_id": scan_run_id,
        "source_path": source_path,
        "scanner_mode": scanner_mode,
        "provider": provider,
        "strategy_code": strategy_code,
        "strategy_version": strategy_version,
        "decision_policy_version": DECISION_POLICY_VERSION,
        "provenance_version": PROVENANCE_VERSION,
        "config_hash": config_hash(snapshot),
        "config_snapshot": snapshot,
        "market_data_as_of": market_data_as_of,
        "evidence_snapshot": evidence,
        "evidence_original_sha256": evidence_meta["evidence_original_sha256"],
        "evidence_original_size_bytes": evidence_meta["evidence_original_size_bytes"],
        "evidence_pruned": evidence_meta["evidence_pruned"],
        "evidence_pruned_keys": evidence_meta["evidence_pruned_keys"],
        "external_observation_ids": [],
    }

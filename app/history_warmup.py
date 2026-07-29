"""Read-only capability verification for the History-Warmup foundation.

Proves the connected identity is EXACTLY the least-privilege
`smart_scanner_history_warmer` role: SELECT on the readiness read relations,
INSERT/UPDATE on ONLY daily_bars / market_bars_4h / history_warmup_runs, and NO
campaign/evaluation/outcome writes and NO DELETE anywhere. It performs only
capability probes (`current_user`, `to_regclass`, `has_table_privilege`,
`pg_roles`) — it NEVER constructs a provider, never calls a market-data API, and
never mutates. Returns only safe capability metadata (no DSN / password / key /
token). This task adds NO provider-backed execute route.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

HISTORY_WARMUP_ACCESS_CHECK_CONTRACT_VERSION = "history_warmup_access_check.v1"

READ_RELATIONS = ("public.daily_bars", "public.market_bars_4h",
                  "public.history_warmup_runs", "public.patterns",
                  "public.pattern_configs")
WRITE_RELATIONS = ("public.daily_bars", "public.market_bars_4h",
                   "public.history_warmup_runs")
# writes on these MUST be forbidden
FORBIDDEN_WRITE_RELATIONS = ("public.strategy_shadow_runs",
                             "public.strategy_shadow_evaluations",
                             "public.strategy_shadow_pairs",
                             "public.strategy_shadow_pair_outcomes")
DELETE_CHECK_RELATIONS = ("public.market_bars_4h", "public.history_warmup_runs")


def evaluate_history_warmup_access(
    *, database_identity: Optional[str], expected_role: Optional[str],
    history_warmup_only_mode: bool, scheduler_enabled: bool,
    provider_name: Optional[str], provider_credential_configured: bool,
    relation_privileges: Dict[str, Dict[str, bool]],
    relation_exists: Dict[str, bool],
) -> Dict[str, Any]:
    """PURE verdict from gathered privilege probes + process configuration."""
    reasons: List[str] = []

    if not (expected_role or "").strip():
        reasons.append("expected_role_not_configured")
    elif database_identity != expected_role:
        reasons.append("database_identity_mismatch")
    if not history_warmup_only_mode:
        reasons.append("history_warmup_only_mode_disabled")
    if scheduler_enabled:
        reasons.append("scheduler_enabled")

    def _priv(rel, p):
        return bool(relation_privileges.get(rel, {}).get(p))

    missing_read = [r for r in READ_RELATIONS
                    if relation_exists.get(r) and not _priv(r, "SELECT")]
    missing_write = [r for r in WRITE_RELATIONS
                     if relation_exists.get(r) and not (_priv(r, "INSERT") and _priv(r, "UPDATE"))]
    missing_relations = [r for r in ("public.market_bars_4h", "public.history_warmup_runs",
                                     "public.daily_bars")
                         if not relation_exists.get(r)]
    forbidden_writes_held = [r for r in FORBIDDEN_WRITE_RELATIONS
                             if relation_exists.get(r)
                             and (_priv(r, "INSERT") or _priv(r, "UPDATE"))]
    delete_held = [r for r in DELETE_CHECK_RELATIONS
                   if relation_exists.get(r) and _priv(r, "DELETE")]

    if missing_relations:
        reasons.append(f"missing_relations:{sorted(missing_relations)}")
    if missing_read:
        reasons.append(f"missing_select:{sorted(missing_read)}")
    if missing_write:
        reasons.append(f"missing_write:{sorted(missing_write)}")
    if forbidden_writes_held:
        reasons.append(f"forbidden_write_privileges:{sorted(forbidden_writes_held)}")
    if delete_held:
        reasons.append(f"delete_privilege_held:{sorted(delete_held)}")

    ready = not reasons
    return {
        "access_check_contract_version": HISTORY_WARMUP_ACCESS_CHECK_CONTRACT_VERSION,
        "ready": ready,
        # Explicitly SEPARATE foundation readiness from provider execution. The
        # foundation (isolated DB + least-privilege role + mode) is usable with
        # NO provider credential: `foundation_ready` NEVER depends on
        # provider_credential_configured. The bounded execute route EXISTS in this
        # build, so `provider_execution_supported` is true; but execution is only
        # `provider_execution_ready` once a provider credential is ALSO configured
        # (and the foundation is ready). A missing provider key therefore leaves
        # execution not-ready WITHOUT ever making the database foundation unusable.
        "foundation_ready": ready,
        "provider_execution_supported": True,
        "provider_execution_ready": bool(ready and provider_credential_configured),
        "reasons": reasons,
        "database_identity": database_identity,
        "expected_database_role": expected_role or None,
        "history_warmup_only_mode": history_warmup_only_mode,
        "scheduler_enabled": scheduler_enabled,
        "provider_name": provider_name,
        "provider_credential_configured": provider_credential_configured,
        "provider_constructed": False,
        "required_relations": list(READ_RELATIONS),
        "required_write_relations": list(WRITE_RELATIONS),
        "required_functions": ["has_table_privilege", "to_regclass"],
        "required_privileges": {
            "select": list(READ_RELATIONS), "insert_update": list(WRITE_RELATIONS)},
        "market_bars_4h_readable": _priv("public.market_bars_4h", "SELECT"),
        "market_bars_4h_writable": (_priv("public.market_bars_4h", "INSERT")
                                    and _priv("public.market_bars_4h", "UPDATE")),
        "daily_bars_readable": _priv("public.daily_bars", "SELECT"),
        "daily_bars_writable": (_priv("public.daily_bars", "INSERT")
                                and _priv("public.daily_bars", "UPDATE")),
        "history_warmup_runs_writable": (_priv("public.history_warmup_runs", "INSERT")
                                         and _priv("public.history_warmup_runs", "UPDATE")),
        "campaign_writes_forbidden": not forbidden_writes_held,
        "outcome_writes_forbidden": not any(
            r in forbidden_writes_held for r in
            ("public.strategy_shadow_pair_outcomes",)),
        "delete_forbidden": not delete_held,
    }


async def run_history_warmup_access_check(
    conn, *, expected_role: Optional[str], history_warmup_only_mode: bool,
    scheduler_enabled: bool, provider_name: Optional[str],
    provider_credential_configured: bool,
) -> Dict[str, Any]:
    """Gather read-only privilege probes and return the verdict. Constructs no
    provider, issues no mutation."""
    identity = await conn.fetchval("SELECT current_user")
    all_rels = list(dict.fromkeys(
        READ_RELATIONS + WRITE_RELATIONS + FORBIDDEN_WRITE_RELATIONS + DELETE_CHECK_RELATIONS))
    relation_exists: Dict[str, bool] = {}
    relation_privileges: Dict[str, Dict[str, bool]] = {}
    for rel in all_rels:
        exists = await conn.fetchval("SELECT to_regclass($1)", rel) is not None
        relation_exists[rel] = exists
        if not exists:
            relation_privileges[rel] = {}
            continue
        row = await conn.fetchrow(
            "SELECT has_table_privilege($1,'SELECT') AS s, "
            "has_table_privilege($1,'INSERT') AS i, "
            "has_table_privilege($1,'UPDATE') AS u, "
            "has_table_privilege($1,'DELETE') AS d", rel)
        relation_privileges[rel] = {"SELECT": bool(row["s"]), "INSERT": bool(row["i"]),
                                    "UPDATE": bool(row["u"]), "DELETE": bool(row["d"])}
    return evaluate_history_warmup_access(
        database_identity=identity, expected_role=expected_role,
        history_warmup_only_mode=history_warmup_only_mode,
        scheduler_enabled=scheduler_enabled, provider_name=provider_name,
        provider_credential_configured=provider_credential_configured,
        relation_privileges=relation_privileges, relation_exists=relation_exists)


__all__ = [
    "HISTORY_WARMUP_ACCESS_CHECK_CONTRACT_VERSION",
    "evaluate_history_warmup_access", "run_history_warmup_access_check",
    "READ_RELATIONS", "WRITE_RELATIONS", "FORBIDDEN_WRITE_RELATIONS",
]

"""Read-only capability verification for the outcome-maintenance environment.

Proves that the connected PostgreSQL identity is EXACTLY the least-privilege
`smart_scanner_outcome_maintainer` role the bounded outcome-maturation write
path needs — SELECT on the plan/calc read relations, INSERT+UPDATE on ONLY the
two outcome write relations, and NO DELETE/TRUNCATE/TRIGGER/DDL anywhere — plus
the required RLS policies, and that the process is configured as a
maintenance-only, scheduler-disabled, Massive-backed, single-mutation-route app.

It performs only capability probes (`current_user`, `SHOW`, `to_regclass`,
`has_table_privilege`, `pg_policies`, `pg_roles`) and never issues a mutation,
never constructs a provider and never makes a live Massive request. It returns
only safe capability metadata — never a DSN, host, password, API key or token.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from app.audit_access import (
    ELEVATED_ROLE_ATTRIBUTES,
    classify_relation_rls,
)


logger = logging.getLogger(__name__)

MAINTENANCE_ACCESS_CHECK_CONTRACT_VERSION = "shadow_maintenance_access_check.v1"

# Read relations the preflight plan + calc selection require (SELECT only).
MAINT_READ_RELATIONS: tuple = (
    "public.strategy_shadow_evaluations",
    "public.strategy_shadow_pairs",
    "public.strategy_shadow_pair_outcomes",
    "public.strategy_shadow_run_pairs",
    "public.strategy_shadow_runs",
    "public.daily_bars",
    "public.patterns",
    "public.pattern_configs",
)
# The ONLY relations the maintainer may write (INSERT + UPDATE, never more).
MAINT_WRITE_RELATIONS: tuple = (
    "public.strategy_shadow_pair_outcomes",
    "public.strategy_shadow_outcome_runs",
)
# Privileges that must be ABSENT everywhere.
FORBIDDEN_PRIVILEGES: tuple = ("DELETE", "TRUNCATE", "TRIGGER")

DENYLISTED_ROLES = frozenset({
    "postgres", "supabase_admin", "service_role", "supabase_auth_admin",
    "supabase_storage_admin", "supabase_read_only_user", "authenticator",
    "rds_superuser", "pg_read_all_data", "pg_write_all_data",
    "smart_scanner_audit_reader",  # the audit reader must never be the writer
})


async def _relation_probe(conn, rel: str, *, need_write: bool) -> Dict[str, Any]:
    """Existence + privileges + applicable RLS policies for one relation."""
    regclass = await conn.fetchval("SELECT to_regclass($1)", rel)
    if regclass is None:
        return {"relation": rel, "exists": False, "need_write": need_write,
                "can_select": False, "can_insert": False, "can_update": False,
                "can_delete": False, "can_truncate": False, "can_trigger": False,
                "rls_enabled": None, "applicable_select_policies": [],
                "has_insert_policy": False, "has_update_policy": False}
    priv = await conn.fetchrow(
        """
        SELECT has_table_privilege($1,'SELECT')   AS can_select,
               has_table_privilege($1,'INSERT')   AS can_insert,
               has_table_privilege($1,'UPDATE')   AS can_update,
               has_table_privilege($1,'DELETE')   AS can_delete,
               has_table_privilege($1,'TRUNCATE') AS can_truncate,
               has_table_privilege($1,'TRIGGER')  AS can_trigger
        """,
        rel,
    )
    flags = await conn.fetchrow(
        """
        SELECT c.relrowsecurity AS rls_enabled
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = split_part($1,'.',1) AND c.relname = split_part($1,'.',2)
        """,
        rel,
    )
    policies = await conn.fetch(
        """
        SELECT p.policyname, p.cmd, p.permissive,
               (p.qual IS NOT NULL
                AND regexp_replace(p.qual,'[[:space:]()]','','g') = 'true')
               AS unconditional_true
        FROM pg_policies p
        WHERE p.schemaname = split_part($1,'.',1)
          AND p.tablename  = split_part($1,'.',2)
          AND EXISTS (
            SELECT 1 FROM unnest(p.roles) rn
            WHERE rn = 'public'
               OR (rn <> 'public' AND pg_has_role(current_user, rn, 'MEMBER')))
        """,
        rel,
    )
    select_policies = [
        {"policyname": r["policyname"], "command": r["cmd"],
         "permissive": r["permissive"],
         "unconditional_true": bool(r["unconditional_true"])}
        for r in policies if r["cmd"] in ("SELECT", "ALL")
    ]
    return {
        "relation": rel, "exists": True, "need_write": need_write,
        "can_select": bool(priv["can_select"]),
        "can_insert": bool(priv["can_insert"]),
        "can_update": bool(priv["can_update"]),
        "can_delete": bool(priv["can_delete"]),
        "can_truncate": bool(priv["can_truncate"]),
        "can_trigger": bool(priv["can_trigger"]),
        "rls_enabled": bool(flags["rls_enabled"]) if flags else False,
        "applicable_select_policies": select_policies,
        "has_insert_policy": any(r["cmd"] in ("INSERT", "ALL") for r in policies),
        "has_update_policy": any(r["cmd"] in ("UPDATE", "ALL") for r in policies),
    }


def evaluate_maintenance_access(
    *,
    database_identity: Optional[str],
    role_attributes: Optional[Dict[str, Any]],
    relation_probes: List[Dict[str, Any]],
    expected_role: Optional[str],
    connection_mode: Optional[str],
    provider: Optional[str],
    provider_credential_configured: bool,
    scheduler_enabled: bool,
    maintenance_only_mode: bool,
    max_batch_size: int,
    mutation_route_count: int,
    locked_cohort_hash: Optional[str] = None,
    current_cohort_lock_hash: Optional[str] = None,
    cohort_pair_count: Optional[int] = None,
    min_batch_interval_seconds: Optional[int] = None,
) -> Dict[str, Any]:
    """PURE verdict from the gathered probes + process configuration."""
    reasons: List[str] = []
    write_rels = set(MAINT_WRITE_RELATIONS)

    # identity
    if not (expected_role or "").strip():
        reasons.append("expected_role_not_configured")
    elif database_identity != expected_role:
        reasons.append("database_identity_mismatch")
    if database_identity in DENYLISTED_ROLES:
        reasons.append("denylisted_role")
    elevated = [a for a in ELEVATED_ROLE_ATTRIBUTES
                if (role_attributes or {}).get(a) is True]
    if elevated:
        reasons.append(f"privileged_role_attributes:{elevated}")

    missing_relations, missing_select, missing_write = [], [], []
    unexpected_write, missing_rls, missing_write_policy = [], [], []
    for row in relation_probes:
        rel = row["relation"]
        if not row["exists"]:
            missing_relations.append(rel)
            continue
        if not row["can_select"]:
            missing_select.append(rel)
        # forbidden privileges must be absent everywhere
        held_forbidden = [p for p in FORBIDDEN_PRIVILEGES
                          if row.get(f"can_{p.lower()}") is True]
        if held_forbidden:
            unexpected_write.append({"relation": rel, "privileges": held_forbidden})
        if rel in write_rels:
            if not row["can_insert"]:
                missing_write.append(f"{rel}:INSERT")
            if not row["can_update"]:
                missing_write.append(f"{rel}:UPDATE")
        else:
            # a read-only relation must NOT be writable
            extra = [p for p in ("INSERT", "UPDATE")
                     if row.get(f"can_{p.lower()}") is True]
            if extra:
                unexpected_write.append({"relation": rel, "privileges": extra})
        # RLS: read relations need a full-row SELECT policy when RLS is on;
        # write relations additionally need applicable INSERT + UPDATE policies.
        if row["rls_enabled"]:
            rls = classify_relation_rls(row)
            row["full_row_select_policy_present"] = rls["full_row_select_policy_present"]
            row["rls_ready"] = rls["rls_ready"]
            if not rls["rls_ready"]:
                missing_rls.append(rel)
            if rel in write_rels:
                if not row.get("has_insert_policy"):
                    missing_write_policy.append(f"{rel}:INSERT")
                if not row.get("has_update_policy"):
                    missing_write_policy.append(f"{rel}:UPDATE")
        else:
            row["rls_ready"] = True

    if missing_relations:
        reasons.append(f"missing_relations:{sorted(missing_relations)}")
    if missing_select:
        reasons.append(f"missing_select_privilege:{sorted(missing_select)}")
    if missing_write:
        reasons.append(f"missing_write_privilege:{sorted(missing_write)}")
    if unexpected_write:
        reasons.append("unexpected_write_privileges:"
                       f"{sorted(w['relation'] for w in unexpected_write)}")
    if missing_rls:
        reasons.append(f"rls_not_ready:{sorted(set(missing_rls))}")
    if missing_write_policy:
        reasons.append(f"missing_write_rls_policy:{sorted(set(missing_write_policy))}")

    # process configuration
    if connection_mode != "maintenance_explicit":
        reasons.append(f"connection_mode_not_maintenance:{connection_mode}")
    if (provider or "").lower() != "massive":
        reasons.append(f"provider_not_massive:{provider}")
    if not provider_credential_configured:
        reasons.append("provider_credential_missing")
    if scheduler_enabled:
        reasons.append("scheduler_enabled")
    if not maintenance_only_mode:
        reasons.append("maintenance_only_mode_disabled")
    # Bounded batch size: 1..10 (the hard cap is 10; a reduced pilot size like 1
    # is valid and must not block readiness).
    if not (1 <= max_batch_size <= 10):
        reasons.append(f"max_batch_size_out_of_range:{max_batch_size}")
    if mutation_route_count != 1:
        reasons.append(f"unexpected_mutation_route_count:{mutation_route_count}")

    # stable cohort lock: must be configured AND match the recomputed cohort
    # lock hash (never the dynamic remaining hash).
    locked_configured = bool((locked_cohort_hash or "").strip())
    locked_matches = (
        locked_configured and current_cohort_lock_hash is not None
        and locked_cohort_hash == current_cohort_lock_hash)
    if not locked_configured:
        reasons.append("locked_cohort_hash_not_configured")
    elif current_cohort_lock_hash is None:
        reasons.append("cohort_lock_unverifiable")
    elif not locked_matches:
        reasons.append("cohort_lock_drift")

    ready = not reasons
    return {
        "access_check_contract_version": MAINTENANCE_ACCESS_CHECK_CONTRACT_VERSION,
        "database_connected": True,
        "database_connection_mode": connection_mode,
        "database_identity": database_identity,
        "expected_database_role": expected_role or None,
        "elevated_role_attributes": elevated,
        "read_relations": [r for r in relation_probes
                           if r["relation"] not in write_rels],
        "write_relations": [r for r in relation_probes
                            if r["relation"] in write_rels],
        "unexpected_write_privileges": unexpected_write,
        "provider": provider,
        "provider_credential_configured": provider_credential_configured,
        "scheduler_enabled": scheduler_enabled,
        "maintenance_only_mode": maintenance_only_mode,
        "max_batch_size": max_batch_size,
        "mutation_route_count": mutation_route_count,
        "locked_cohort_hash_configured": locked_configured,
        "locked_cohort_hash_matches": locked_matches,
        "current_cohort_lock_hash": current_cohort_lock_hash,
        "cohort_pair_count": cohort_pair_count,
        # Provider pacing: readiness is an ENVIRONMENT capability verdict and is
        # deliberately NOT a function of the current clock. A temporary cooldown
        # (temporary unavailability) is reported only by the preflight endpoint;
        # here we surface the configured interval and where the pacing state is
        # persisted so operators can reason about it. Environment readiness may
        # remain true during a cooldown.
        "min_batch_interval_seconds": min_batch_interval_seconds,
        "cooldown_persistence_source": (
            "strategy_shadow_outcome_runs"
            if min_batch_interval_seconds is not None else None),
        "ready_for_maintenance_execution": ready,
        "reasons": reasons,
    }


async def run_maintenance_access_check(
    conn,
    *,
    expected_role: Optional[str],
    connection_mode: Optional[str],
    provider: Optional[str],
    provider_credential_configured: bool,
    scheduler_enabled: bool,
    maintenance_only_mode: bool,
    max_batch_size: int,
    mutation_route_count: int,
    locked_cohort_hash: Optional[str] = None,
    current_cohort_lock_hash: Optional[str] = None,
    cohort_pair_count: Optional[int] = None,
    min_batch_interval_seconds: Optional[int] = None,
) -> Dict[str, Any]:
    """Gather read-only capability probes and return the maintenance verdict.

    Runs inside an explicit transaction; issues no mutation and constructs no
    provider. The writable relations mean the transaction is NOT read-only, but
    only capability probes are executed here.
    """
    identity = await conn.fetchval("SELECT current_user")
    row = await conn.fetchrow(
        """
        SELECT rolsuper, rolcreaterole, rolcreatedb, rolreplication, rolbypassrls
        FROM pg_roles WHERE rolname = current_user
        """
    )
    role_attributes = {
        a: bool(row[a]) for a in ELEVATED_ROLE_ATTRIBUTES
    } if row is not None else {}

    all_rels = list(dict.fromkeys(MAINT_READ_RELATIONS + MAINT_WRITE_RELATIONS))
    write_set = set(MAINT_WRITE_RELATIONS)
    probes = [
        await _relation_probe(conn, rel, need_write=(rel in write_set))
        for rel in all_rels
    ]
    return evaluate_maintenance_access(
        database_identity=identity,
        role_attributes=role_attributes,
        relation_probes=probes,
        expected_role=expected_role,
        connection_mode=connection_mode,
        provider=provider,
        provider_credential_configured=provider_credential_configured,
        scheduler_enabled=scheduler_enabled,
        maintenance_only_mode=maintenance_only_mode,
        max_batch_size=max_batch_size,
        mutation_route_count=mutation_route_count,
        locked_cohort_hash=locked_cohort_hash,
        current_cohort_lock_hash=current_cohort_lock_hash,
        cohort_pair_count=cohort_pair_count,
        min_batch_interval_seconds=min_batch_interval_seconds,
    )


__all__ = [
    "MAINTENANCE_ACCESS_CHECK_CONTRACT_VERSION",
    "MAINT_READ_RELATIONS",
    "MAINT_WRITE_RELATIONS",
    "FORBIDDEN_PRIVILEGES",
    "evaluate_maintenance_access",
    "run_maintenance_access_check",
]

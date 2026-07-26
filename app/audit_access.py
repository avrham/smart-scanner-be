"""Read-only database access verification for the cohort closeout audit.

Proves that the connected PostgreSQL identity has ONLY the privileges the
`GET /api/admin/shadow-cohort/closeout` read path needs — and no write
privileges — without ever mutating the database. It performs only capability
probes (`current_user`, `SHOW ...`, `to_regclass`, `has_table_privilege`) and
never issues INSERT/UPDATE/DELETE/TRUNCATE/DDL/temp-table statements.

The exact relation set below is the closeout DB call graph, traced from the
endpoint through the shadow persistence, quality-audit, evaluation/outcome
reads and the trading-calendar read:

  fetch_strategy_shadow_evaluations  -> strategy_shadow_evaluations,
                                         strategy_shadow_pairs,
                                         strategy_shadow_pair_outcomes,
                                         strategy_shadow_run_pairs,
                                         strategy_shadow_runs
  fetch_pair_outcomes                -> (same five relations)
  fetch_shadow_campaign_runs         -> strategy_shadow_runs
  discover_strategy                  -> patterns, pattern_configs
  _cohort_trading_calendar           -> daily_bars

No custom SQL functions, sequences, views or materialized views are read (the
queries use only built-in functions, which are executable by PUBLIC).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional


logger = logging.getLogger(__name__)

ACCESS_CHECK_CONTRACT_VERSION = "shadow_audit_access_check.v1"

# Exact relations the closeout read path requires (schema-qualified). SELECT
# only; every write privilege here must be absent on the audit role.
REQUIRED_RELATIONS: tuple = (
    "public.strategy_shadow_evaluations",
    "public.strategy_shadow_pairs",
    "public.strategy_shadow_pair_outcomes",
    "public.strategy_shadow_run_pairs",
    "public.strategy_shadow_runs",
    "public.daily_bars",
    "public.patterns",
    "public.pattern_configs",
)

# The closeout path calls no custom SQL functions.
REQUIRED_FUNCTIONS: tuple = ()

# Table privileges that must NOT be held by the audit identity.
WRITE_PRIVILEGES: tuple = (
    "INSERT", "UPDATE", "DELETE", "TRUNCATE", "TRIGGER",
)

# PostgreSQL role attributes that make an identity broader than an audit
# reader. Any of these being true fails readiness.
ELEVATED_ROLE_ATTRIBUTES: tuple = (
    "rolsuper", "rolcreaterole", "rolcreatedb", "rolreplication",
    "rolbypassrls",
)

# Role names that are never acceptable as the audit identity (belt-and-suspenders
# alongside the attribute check).
DENYLISTED_ROLES: frozenset = frozenset({
    "postgres", "supabase_admin", "service_role", "supabase_auth_admin",
    "supabase_storage_admin", "supabase_read_only_user", "authenticator",
    "rds_superuser", "pg_read_all_data", "pg_write_all_data",
})


def _is_on(value: Any) -> Optional[bool]:
    """Interpret a PostgreSQL on/off setting; None when unknown."""
    if value is None:
        return None
    return str(value).strip().lower() == "on"


def _elevated_attributes(role_attributes: Optional[Dict[str, Any]]) -> List[str]:
    attrs = role_attributes or {}
    return [a for a in ELEVATED_ROLE_ATTRIBUTES if attrs.get(a) is True]


def evaluate_access(
    *,
    database_identity: Optional[str],
    transaction_read_only: Any,
    default_transaction_read_only: Any,
    relation_privileges: List[Dict[str, Any]],
    role_attributes: Optional[Dict[str, Any]] = None,
    expected_role: Optional[str] = None,
    require_expected_role: bool = False,
    connection_mode: Optional[str] = None,
) -> Dict[str, Any]:
    """PURE: turn raw capability probe results into the access-check verdict.

    `relation_privileges` is one dict per required relation with keys:
    relation, exists, can_select, can_insert, can_update, can_delete,
    can_truncate, can_trigger. `role_attributes` are the pg_roles attributes of
    current_user; `expected_role` is the identity the caller demands.
    """
    reasons: List[str] = []
    unexpected_write: List[Dict[str, Any]] = []
    missing_relations: List[str] = []
    missing_select: List[str] = []

    for row in relation_privileges:
        rel = row.get("relation")
        if not row.get("exists"):
            missing_relations.append(rel)
            continue
        if not row.get("can_select"):
            missing_select.append(rel)
        held = [
            priv for priv in WRITE_PRIVILEGES
            if row.get(f"can_{priv.lower()}") is True
        ]
        if held:
            unexpected_write.append({"relation": rel, "privileges": held})

    default_ro = _is_on(default_transaction_read_only)
    elevated = _elevated_attributes(role_attributes)

    # ---- identity ---------------------------------------------------------- #
    if require_expected_role and not (expected_role or "").strip():
        reasons.append("expected_role_not_configured")
    elif expected_role and database_identity != expected_role:
        reasons.append("database_identity_mismatch")
    if database_identity in DENYLISTED_ROLES:
        reasons.append("denylisted_role")
    if elevated:
        reasons.append(f"privileged_role_attributes:{elevated}")

    # ---- privileges / transaction defaults --------------------------------- #
    if missing_relations:
        reasons.append(f"missing_relations:{sorted(missing_relations)}")
    if missing_select:
        reasons.append(f"missing_select_privilege:{sorted(missing_select)}")
    if unexpected_write:
        reasons.append(
            "unexpected_write_privileges:"
            f"{sorted(w['relation'] for w in unexpected_write)}"
        )
    if default_ro is not True:
        reasons.append("default_transaction_read_only_not_on")

    ready = not reasons
    return {
        "access_check_contract_version": ACCESS_CHECK_CONTRACT_VERSION,
        "database_connected": True,
        "database_connection_mode": connection_mode,
        "database_identity": database_identity,
        "expected_database_role": expected_role or None,
        "role_attributes": {
            a: bool((role_attributes or {}).get(a))
            for a in ELEVATED_ROLE_ATTRIBUTES
        } if role_attributes is not None else None,
        "elevated_role_attributes": elevated,
        "transaction_read_only": _is_on(transaction_read_only),
        "default_transaction_read_only": default_ro,
        "required_relations": relation_privileges,
        "required_functions": list(REQUIRED_FUNCTIONS),
        "unexpected_write_privileges": unexpected_write,
        "ready_for_closeout_audit": ready,
        "reasons": reasons,
    }


async def _relation_privileges(conn) -> List[Dict[str, Any]]:
    """Probe existence + table privileges for each required relation.

    Existence is checked with to_regclass FIRST so has_table_privilege is only
    called on relations that exist (it errors on a missing relation). All
    read-only probes; nothing is mutated.
    """
    out: List[Dict[str, Any]] = []
    for rel in REQUIRED_RELATIONS:
        regclass = await conn.fetchval("SELECT to_regclass($1)", rel)
        if regclass is None:
            out.append({
                "relation": rel, "exists": False, "can_select": False,
                "can_insert": False, "can_update": False, "can_delete": False,
                "can_truncate": False, "can_trigger": False,
            })
            continue
        row = await conn.fetchrow(
            """
            SELECT has_table_privilege($1, 'SELECT')   AS can_select,
                   has_table_privilege($1, 'INSERT')   AS can_insert,
                   has_table_privilege($1, 'UPDATE')   AS can_update,
                   has_table_privilege($1, 'DELETE')   AS can_delete,
                   has_table_privilege($1, 'TRUNCATE') AS can_truncate,
                   has_table_privilege($1, 'TRIGGER')  AS can_trigger
            """,
            rel,
        )
        out.append({
            "relation": rel, "exists": True,
            "can_select": bool(row["can_select"]),
            "can_insert": bool(row["can_insert"]),
            "can_update": bool(row["can_update"]),
            "can_delete": bool(row["can_delete"]),
            "can_truncate": bool(row["can_truncate"]),
            "can_trigger": bool(row["can_trigger"]),
        })
    return out


async def _role_attributes(conn, identity: Optional[str]) -> Dict[str, Any]:
    """pg_roles attributes of current_user (read-only). {} when unavailable."""
    row = await conn.fetchrow(
        """
        SELECT rolsuper, rolcreaterole, rolcreatedb, rolreplication, rolbypassrls
        FROM pg_roles WHERE rolname = current_user
        """
    )
    if row is None:
        return {}
    return {
        "rolsuper": bool(row["rolsuper"]),
        "rolcreaterole": bool(row["rolcreaterole"]),
        "rolcreatedb": bool(row["rolcreatedb"]),
        "rolreplication": bool(row["rolreplication"]),
        "rolbypassrls": bool(row["rolbypassrls"]),
    }


async def run_access_check(
    conn,
    *,
    expected_role: Optional[str] = None,
    require_expected_role: bool = False,
    connection_mode: Optional[str] = None,
) -> Dict[str, Any]:
    """Run every capability probe inside an explicit READ ONLY transaction.

    Defense-in-depth: the audit probes cannot issue a mutation because the
    surrounding transaction is read-only (the dedicated PostgreSQL role remains
    the primary enforcement layer). Read-only, bounded, no raw SQL error text
    is surfaced to the caller.
    """
    async with conn.transaction(readonly=True):
        identity = await conn.fetchval("SELECT current_user")
        txn_ro = await conn.fetchval("SHOW transaction_read_only")
        default_ro = await conn.fetchval("SHOW default_transaction_read_only")
        role_attributes = await _role_attributes(conn, identity)
        relation_privileges = await _relation_privileges(conn)

    return evaluate_access(
        database_identity=identity,
        transaction_read_only=txn_ro,
        default_transaction_read_only=default_ro,
        relation_privileges=relation_privileges,
        role_attributes=role_attributes,
        expected_role=expected_role,
        require_expected_role=require_expected_role,
        connection_mode=connection_mode,
    )


__all__ = [
    "ACCESS_CHECK_CONTRACT_VERSION",
    "REQUIRED_RELATIONS",
    "REQUIRED_FUNCTIONS",
    "WRITE_PRIVILEGES",
    "evaluate_access",
    "run_access_check",
]

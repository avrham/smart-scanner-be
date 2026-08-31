"""WHICH COHORT a source-freshness row describes.

`catalyst_source_state` answers "can we see this source right now?". Until now
it held exactly one row per source, and every reader silently assumed that row
described the frozen 25 — the product cohort. That assumption was invisible
because nothing else ever wrote it.

The research lifecycle broke the assumption the first time it tried to enrich a
discovered symbol. `refresh_sec_filings` for ONE research symbol would have
written

    sec_edgar | ok | symbols_covered = 1

into the same row the Product API reads to decide whether the SEC dimension is
trustworthy for the 25. The freshness would have been true — for a symbol the
product cannot see. That is not a stale row; it is a row that is confidently
about the wrong population.

SCOPE IS PART OF THE IDENTITY, NOT PART OF THE NAME
---------------------------------------------------
The obvious shortcut is a composite source string (`sec_edgar:research`). It is
rejected deliberately: `source` is a foreign-key-ish vocabulary shared with
`external_signal_sources`, is matched with `LIKE 'external\\_%'` in live RLS
predicates, and is prefixed by `source_state_key()`. Encoding a second
dimension inside it would make every one of those string rules quietly wrong,
and "is this row for research?" would become a parsing question instead of a
column comparison.

So the primary key becomes (source, scope). One source can now be fresh for one
cohort and never-run for another, which is simply the truth.

WHY THE DEFAULT IS `product`
----------------------------
Every row that exists today describes the product cohort, so `product` as the
column default makes the migration a no-op for existing data AND makes every
existing writer correct without changing it. A scope-unaware caller keeps
writing the product row, which is exactly what it was already doing.

WHAT ENFORCES IT
----------------
Three independent layers, because a convention is not a boundary:

  1. the Product API reads `WHERE scope = 'product'` (application);
  2. the product reader role's RLS policy is `USING (scope = 'product')`, so
     even a router bug cannot surface a research row (database, read side);
  3. the research role's RLS policy is `WITH CHECK (scope = 'research')`, so
     the research lifecycle is structurally unable to write the product row —
     the exact mistake this module exists to prevent (database, write side).

Layer 3 is the important one. The first live attempt was stopped by RLS rather
than by review, and that should remain true after the feature is switched on.
"""

from __future__ import annotations

from typing import Optional, Tuple

SOURCE_SCOPE_CONTRACT_VERSION = "smart_scanner_source_scope.v1"

#: The canonical scanner cohort — the frozen 25 the Product API serves. Every
#: pre-existing `catalyst_source_state` row is this scope by definition.
SCOPE_PRODUCT = "product"

#: Dynamically discovered symbols under research. Internal only; no product
#: surface reads it, and nothing in it may inform a product freshness verdict.
SCOPE_RESEARCH = "research"

SOURCE_SCOPES: Tuple[str, ...] = (SCOPE_PRODUCT, SCOPE_RESEARCH)

#: What a caller that does not know about scopes gets. Chosen so that silence
#: means "product", which is what every existing writer already meant.
DEFAULT_SCOPE = SCOPE_PRODUCT


class UnknownSourceScope(ValueError):
    """A scope that is not in the vocabulary. Raised rather than defaulted.

    Coercing an unrecognised scope to `product` would turn a typo into a
    product-freshness write, which is the one failure this module must not
    have.
    """


def is_valid_scope(scope: Optional[str]) -> bool:
    return scope in SOURCE_SCOPES


def normalise_scope(scope: Optional[str]) -> str:
    """`None` -> the product default; anything unrecognised -> raise."""
    if scope is None:
        return DEFAULT_SCOPE
    text = str(scope).strip()
    if text not in SOURCE_SCOPES:
        raise UnknownSourceScope(
            f"unknown source scope {scope!r}; expected one of {SOURCE_SCOPES}")
    return text


def is_product_scope(scope: Optional[str]) -> bool:
    """True only for the cohort the Product API is allowed to report on."""
    return normalise_scope(scope) == SCOPE_PRODUCT


__all__ = [
    "SOURCE_SCOPE_CONTRACT_VERSION", "SCOPE_PRODUCT", "SCOPE_RESEARCH",
    "SOURCE_SCOPES", "DEFAULT_SCOPE", "UnknownSourceScope",
    "is_valid_scope", "normalise_scope", "is_product_scope",
]

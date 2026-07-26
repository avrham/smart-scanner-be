"""Deterministic experiment-universe identity (shadow_universe_identity.v1).

The repository intentionally uses EXPLICIT, sorted, deduplicated symbol lists
with no implicit universe. A prospective Wyckoff v2 shadow campaign is run
against an operator-supplied 50-symbol experiment file; there is no hidden
default list anywhere in the code.

This module computes a stable IDENTITY for such an explicit list so an
operator can:
  * freeze the file once and prove every campaign used the same universe
    (the hash is independent of input order, casing and duplicates);
  * detect membership drift immediately (a different symbol set ⇒ a
    different hash — never a false match on count alone).

The normalization is the SAME canonicalization campaign creation uses
(`normalize_campaign_symbols`) so the identity is computed over exactly the
symbols a campaign would run. PURE — no I/O, no persistence, no migration.
"""

from __future__ import annotations

import hashlib
from typing import Any, Dict, List

from app.workers.shadow.campaigns import _SYMBOL_RE, CampaignRequestError


UNIVERSE_IDENTITY_VERSION = "shadow_universe_identity.v1"


def normalize_campaign_symbols(symbols: Any) -> List[str]:
    """Canonicalize an explicit symbol list exactly as campaign creation does.

    Reuses the campaign symbol pattern (`_SYMBOL_RE`) and error type verbatim
    so an experiment universe is validated by the SAME rule a campaign would
    apply — trims whitespace, upper-cases, rejects malformed tickers,
    deduplicates and sorts deterministically. There is no implicit universe:
    the input must be an explicit non-empty list. (Kept here, in an additive
    module, so the frozen campaign execution layer stays byte-identical.)
    """
    if not isinstance(symbols, list) or not symbols:
        raise CampaignRequestError(
            "symbols must be an explicit non-empty list — campaigns never "
            "run an implicit universe"
        )
    normalized: List[str] = []
    seen = set()
    for raw in symbols:
        symbol = str(raw or "").strip().upper()
        if not symbol:
            continue
        if not _SYMBOL_RE.match(symbol):
            raise CampaignRequestError(f"malformed symbol {symbol!r}")
        if symbol not in seen:
            seen.add(symbol)
            normalized.append(symbol)
    if not normalized:
        raise CampaignRequestError("symbols must contain at least one ticker")
    return sorted(normalized)

# Newline-joined canonical form is hashed under a versioned prefix so the
# payload shape can never silently change identity without a version bump.
_HASH_PREFIX = f"{UNIVERSE_IDENTITY_VERSION}\n"


def compute_universe_hash(normalized_symbols: List[str]) -> str:
    """SHA-256 over the versioned newline-joined canonical symbol list.

    Input MUST already be normalized (sorted/deduped/upper). Order-independent
    identity is guaranteed by the caller normalizing first; this function is
    deterministic over its input.
    """
    payload = _HASH_PREFIX + "\n".join(normalized_symbols)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def universe_identity(symbols: Any) -> Dict[str, Any]:
    """Normalize an explicit symbol list and return its stable identity.

    Raises CampaignRequestError (from normalize_campaign_symbols) for a
    non-list, empty or malformed input — an experiment universe is never
    implicit and never silently truncated.
    """
    normalized = normalize_campaign_symbols(symbols)
    return {
        "universe_identity_version": UNIVERSE_IDENTITY_VERSION,
        "symbol_count": len(normalized),
        "universe_hash": compute_universe_hash(normalized),
        "symbols": normalized,
    }


def inspect_universe_symbols(
    raw_symbols: Any, *, expected_count: Optional[int] = None
) -> Dict[str, Any]:
    """Operator-facing, NON-RAISING validation of a supplied symbol list.

    Unlike `universe_identity` (which raises on the first problem and silently
    deduplicates for identity), this reports EVERY problem so an operator can
    fix the frozen file before launching: duplicates are surfaced explicitly
    rather than silently implying the file was clean, invalid tokens are
    collected (not just the first), and an empty input is flagged. The stable
    identity is still computed over the unique valid symbols.

    Returns `ok=True` only when: the input is non-empty, has no invalid tokens,
    has no duplicates, and (when `expected_count` is given) the unique valid
    count matches it.
    """
    if not isinstance(raw_symbols, list):
        return {
            "ok": False,
            "problems": ["not_a_list"],
            "raw_count": 0,
            "unique_count": 0,
            "symbols": [],
            "duplicates_supplied": [],
            "invalid_tokens": [],
            "universe_hash": None,
            "expected_count": expected_count,
        }

    raw_count = len(raw_symbols)
    seen: Dict[str, int] = {}
    invalid: List[str] = []
    blank = 0
    for token in raw_symbols:
        norm = str(token or "").strip().upper()
        if not norm:
            blank += 1
            continue
        if not _SYMBOL_RE.match(norm):
            invalid.append(norm)
            continue
        seen[norm] = seen.get(norm, 0) + 1

    valid_unique = sorted(seen)
    duplicates = sorted(s for s, n in seen.items() if n > 1)
    problems: List[str] = []
    if raw_count == 0 or (blank == raw_count):
        problems.append("empty")
    if invalid:
        problems.append("invalid_symbols")
    if duplicates:
        problems.append("duplicate_symbols")
    if expected_count is not None and len(valid_unique) != expected_count:
        problems.append("unexpected_count")

    return {
        "universe_identity_version": UNIVERSE_IDENTITY_VERSION,
        "ok": not problems,
        "problems": problems,
        "raw_count": raw_count,
        "unique_count": len(valid_unique),
        "symbols": valid_unique,
        "duplicates_supplied": duplicates,
        "invalid_tokens": sorted(set(invalid)),
        "universe_hash": (
            compute_universe_hash(valid_unique) if valid_unique else None
        ),
        "expected_count": expected_count,
    }


def parse_symbol_file_text(text: str) -> List[str]:
    """Parse operator-supplied universe file text into a raw symbol list.

    Accepts newline-, comma- or whitespace-delimited tickers and ignores
    blank lines and `#` comment lines. The result is passed through the same
    `universe_identity` normalization; this helper only splits, it never
    validates or sorts (so malformed tokens still surface as errors).
    """
    tokens: List[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        for piece in stripped.replace(",", " ").split():
            tokens.append(piece)
    return tokens


__all__ = [
    "UNIVERSE_IDENTITY_VERSION",
    "normalize_campaign_symbols",
    "compute_universe_hash",
    "universe_identity",
    "inspect_universe_symbols",
    "parse_symbol_file_text",
]

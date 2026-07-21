"""Normalized evidence contract — evidence.v1 (Phase 8).

A typed, versioned, JSON-safe representation of everything a strategy
measured and decided. The contract is deterministic:

  * item lists serialize sorted by the FULL identity key
    (category, code, source_type, timeframe, as_of) — insertion order can
    never change identity, and two observations that differ only by
    timeframe or as-of are distinct items, never collapsed;
  * duplicate item identities (same full key) are REJECTED — an ambiguous
    duplicate would make the serialized evidence ordering meaningless;
  * only SET-LIKE lists are sorted (missing_data, contradictions); semantic
    sequences (chronological event lists, declared timeframe sequences,
    documented trigger-condition order) are carried inside item raw values /
    metadata and are NEVER reordered by serialization;
  * ranking components are stable NAMED keys (serialized key-sorted), never
    positional;
  * raw values are ALWAYS preserved next to normalized values (a normalized
    value never replaces the measurement it came from);
  * unknown stays unknown — no value is ever invented to fill a field;
  * hard filters / required confirmations are visibly separate from soft
    (ranking) evidence via `required` + `hard_filter_summary`;
  * missing data and contradictions are explicit top-level lists.

No new database table: an EvidenceBundle is persisted as a plain dict inside
the existing immutable `signal_provenance.evidence_snapshot` (Phase 7B),
under the `evidence` key, and therefore participates in the original
pre-pruning evidence hash and the signal fingerprint.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

EVIDENCE_VERSION = "evidence.v1"

# Item states. pass/fail are for gate-like checks (hard filters, required
# confirmations); positive/negative/neutral are for directional soft
# evidence; unknown means the value could not be computed honestly.
EVIDENCE_STATES = frozenset(
    {"pass", "fail", "positive", "negative", "neutral", "unknown"}
)

# Where the evidence came from. external/fundamental/event/risk are reserved
# for Phase 10 external observations — defined now so the vocabulary is
# stable, but nothing fabricates them in Phase 8.
SOURCE_TYPES = frozenset(
    {"market_data", "strategy", "external", "fundamental", "event", "risk"}
)

SETUP_STATES = frozenset({"valid", "invalid", "unknown"})
TRIGGER_STATES = frozenset({"confirmed", "missing", "contradicted", "unknown"})
VERDICTS = frozenset({"ENTER", "WATCH", "AVOID"})

_JSON_SCALARS = (str, int, float, bool, type(None))


def _require_json_safe(value: Any, where: str) -> Any:
    """Reject non-JSON-safe values instead of silently coercing them.

    Evidence must be reproducible: a datetime/np.float64 slipping through
    would serialize differently across environments. Callers convert
    explicitly (ISO strings, float(), int()) before building evidence.
    """
    if isinstance(value, _JSON_SCALARS):
        return value
    if isinstance(value, dict):
        for k, v in value.items():
            if not isinstance(k, str):
                raise ValueError(f"non-string dict key in {where}: {k!r}")
            _require_json_safe(v, f"{where}.{k}")
        return value
    if isinstance(value, (list, tuple)):
        for i, v in enumerate(value):
            _require_json_safe(v, f"{where}[{i}]")
        return value
    raise ValueError(
        f"non-JSON-safe value in {where}: {type(value).__name__} ({value!r})"
    )


@dataclass
class EvidenceItem:
    """One measured fact: what was checked, against what, and the outcome."""

    code: str                                  # e.g. "sma_proximity"
    category: str                              # e.g. "setup" | "confirmation" | ...
    source_type: str                           # see SOURCE_TYPES
    state: str                                 # see EVIDENCE_STATES
    raw_value: Any = None                      # the measurement itself (never replaced)
    normalized_value: Optional[float] = None   # bounded [0,1] quality, when defined
    unit: Optional[str] = None                 # e.g. "pct", "ratio", "bars", "count"
    threshold: Any = None                      # the configured gate value, when any
    operator: Optional[str] = None             # e.g. ">=", "<=", "between", "=="
    required: bool = False                     # True = hard filter / required confirmation
    timeframe: Optional[str] = None            # e.g. "1d"
    as_of: Optional[str] = None                # ISO timestamp/date the value refers to
    reason_code: Optional[str] = None          # deterministic machine-readable reason
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.code:
            raise ValueError("EvidenceItem.code must be non-empty")
        if self.state not in EVIDENCE_STATES:
            raise ValueError(
                f"invalid evidence state {self.state!r} for {self.code!r}; "
                f"allowed: {sorted(EVIDENCE_STATES)}"
            )
        if self.source_type not in SOURCE_TYPES:
            raise ValueError(
                f"invalid source_type {self.source_type!r} for {self.code!r}; "
                f"allowed: {sorted(SOURCE_TYPES)}"
            )
        if self.normalized_value is not None:
            nv = float(self.normalized_value)
            if not (0.0 <= nv <= 1.0):
                raise ValueError(
                    f"normalized_value {nv} out of [0,1] for {self.code!r}"
                )
            self.normalized_value = nv
        _require_json_safe(self.raw_value, f"{self.code}.raw_value")
        _require_json_safe(self.threshold, f"{self.code}.threshold")
        _require_json_safe(self.metadata, f"{self.code}.metadata")

    def identity_key(self) -> tuple:
        """Full deterministic identity: two items may share category+code only
        when they differ by source, timeframe or as-of (e.g. the same check on
        1d vs 4h). Identical keys are ambiguous duplicates and are rejected
        at the bundle level."""
        return (
            self.category,
            self.code,
            self.source_type,
            self.timeframe or "",
            self.as_of or "",
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "category": self.category,
            "source_type": self.source_type,
            "state": self.state,
            "raw_value": self.raw_value,
            "normalized_value": self.normalized_value,
            "unit": self.unit,
            "threshold": self.threshold,
            "operator": self.operator,
            "required": self.required,
            "timeframe": self.timeframe,
            "as_of": self.as_of,
            "reason_code": self.reason_code,
            "metadata": dict(self.metadata),
        }


@dataclass
class EvidenceBundle:
    """The complete evidence for one strategy decision on one symbol."""

    strategy_code: str
    strategy_version: str
    decision_policy_version: str
    symbol: str
    verdict: str                                # ENTER | WATCH | AVOID
    setup_state: str = "unknown"                # valid | invalid | unknown
    trigger_state: str = "unknown"              # confirmed | missing | contradicted | unknown
    market_data_as_of: Optional[str] = None     # ISO string; None = explicitly missing
    items: List[EvidenceItem] = field(default_factory=list)
    hard_filter_summary: Dict[str, Any] = field(default_factory=dict)
    missing_data: List[str] = field(default_factory=list)
    contradictions: List[str] = field(default_factory=list)
    timeframe_summary: Dict[str, Any] = field(default_factory=dict)
    ranking_components: Dict[str, Optional[float]] = field(default_factory=dict)
    ranking_score: Optional[float] = None
    evidence_version: str = EVIDENCE_VERSION

    def __post_init__(self) -> None:
        if self.verdict not in VERDICTS:
            raise ValueError(f"invalid verdict {self.verdict!r}")
        if self.setup_state not in SETUP_STATES:
            raise ValueError(f"invalid setup_state {self.setup_state!r}")
        if self.trigger_state not in TRIGGER_STATES:
            raise ValueError(f"invalid trigger_state {self.trigger_state!r}")
        for name, value in self.ranking_components.items():
            if value is not None and not (0.0 <= float(value) <= 1.0):
                raise ValueError(
                    f"ranking component {name!r}={value} out of [0,1]"
                )
        if self.ranking_score is not None and not (
            0.0 <= float(self.ranking_score) <= 1.0
        ):
            raise ValueError(f"ranking_score {self.ranking_score} out of [0,1]")
        _require_json_safe(self.hard_filter_summary, "hard_filter_summary")
        _require_json_safe(self.timeframe_summary, "timeframe_summary")
        seen: Dict[tuple, str] = {}
        for item in self.items:
            key = item.identity_key()
            if key in seen:
                raise ValueError(
                    "duplicate ambiguous evidence item identity "
                    f"{key!r}; disambiguate by source_type, timeframe or as_of"
                )
            seen[key] = item.code

    def to_dict(self) -> Dict[str, Any]:
        """Deterministic dict form: items sorted by their FULL identity key
        (category, code, source_type, timeframe, as_of); set-like lists
        (missing_data/contradictions) sorted; component keys sorted. Semantic
        sequences inside raw values/metadata are preserved as given."""
        return {
            "evidence_version": self.evidence_version,
            "strategy_code": self.strategy_code,
            "strategy_version": self.strategy_version,
            "decision_policy_version": self.decision_policy_version,
            "symbol": self.symbol,
            "market_data_as_of": self.market_data_as_of,
            "verdict": self.verdict,
            "setup_state": self.setup_state,
            "trigger_state": self.trigger_state,
            "items": [
                item.to_dict()
                for item in sorted(self.items, key=lambda i: i.identity_key())
            ],
            "hard_filter_summary": dict(self.hard_filter_summary),
            "missing_data": sorted(self.missing_data),
            "contradictions": sorted(self.contradictions),
            "timeframe_summary": dict(self.timeframe_summary),
            "ranking_components": {
                k: self.ranking_components[k]
                for k in sorted(self.ranking_components)
            },
            "ranking_score": self.ranking_score,
        }

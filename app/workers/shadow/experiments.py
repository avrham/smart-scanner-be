"""Explicit shadow-experiment registry (Phase 9D2).

The Phase 8.1B1 shadow runner compared exactly one hard-coded pair
(sma150.v2 control vs sma150.v3 candidate). Phase 9D keeps that experiment
byte-identical — same experiment code/version, same arm codes, same
fingerprint payloads, same category labels — and generalizes the SAME runner
behind an explicit, closed registry of experiment definitions. There is no
dynamic experiment creation and no automatic execution: every experiment is
declared here and only an authorized operator may invoke one.

The wyckoff_v2_vs_baseline experiment measures the registered-but-disabled
wyckoff_mtf_v2 candidate against the production baseline strategy
(sma150_bounce) on the exact same canonical completed frame. Running it never
enables wyckoff_mtf_v2, never changes rollout flags and never creates
signals, watches, alerts, notifications, decision cards or ranking inputs —
shadow rows are experiment evidence only.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

from app.workers.shadow.constants import (
    CANDIDATE_ARM_CODE,
    CANDIDATE_PATTERN_CODE,
    CONTROL_ARM_CODE,
    CONTROL_PATTERN_CODE,
    EXPERIMENT_CODE,
    EXPERIMENT_VERSION,
)
from app.workers.shadow.frames import (
    CanonicalFrame,
    required_history_bars_v2,
    required_history_bars_v3,
    required_history_bars_wyckoff_v2,
)


class UnknownShadowExperimentError(KeyError):
    """No shadow experiment is declared for the requested code/candidate."""


def _wyckoff_v2_data_meta_extras(frame: CanonicalFrame) -> Dict[str, Any]:
    """Completion vocabulary wyckoff_mtf.v2 reads from data_meta.

    The canonical frame builder PROVED the latest bar completed
    (ny_session_close.v1) before either arm evaluates, so the proof is passed
    explicitly instead of being re-derived from wall-clock time.
    """
    return {"explicit_completed": True, "as_of_date": frame.last_date}


@dataclass(frozen=True)
class ShadowExperiment:
    """One declared two-arm shadow comparison protocol.

    * arm codes are persisted verbatim (migration 010/013 CHECK constraint);
    * category labels drive the deterministic verdict-combination vocabulary
      (the sma150 experiment keeps its historical 'v2_*_v3_*' labels; new
      experiments use neutral 'control_*_candidate_*' labels);
    * history-bar derivations are each arm's OWN canonical requirement;
    * data_meta extras carry per-strategy completion vocabulary — the default
      runner keys are never removed, only extended;
    * `requires_four_hour_frame` declares that the CANDIDATE arm evaluates a
      canonical completed 4H frame alongside the daily frame (Phase 9E3);
    * `candidate_config_overrides` is the experiment-only IMMUTABLE
      evaluation override applied on top of the canonically resolved config
      for the CANDIDATE arm inside the shadow run ONLY. It never mutates
      stored pattern configuration rows or any production configuration; it
      is visible verbatim in the frozen config snapshot, enters the config
      hash (and therefore every fingerprint), and is echoed in run
      telemetry. `allow_enter` may never be overridden here.
    """

    experiment_code: str
    experiment_version: str
    control_pattern_code: str
    candidate_pattern_code: str
    control_arm_code: str
    candidate_arm_code: str
    control_category_label: str
    candidate_category_label: str
    control_history_bars: Callable[[Dict[str, Any]], int]
    candidate_history_bars: Callable[[Dict[str, Any]], int]
    control_data_meta_extras: Optional[
        Callable[[CanonicalFrame], Dict[str, Any]]
    ] = field(default=None)
    candidate_data_meta_extras: Optional[
        Callable[[CanonicalFrame], Dict[str, Any]]
    ] = field(default=None)
    requires_four_hour_frame: bool = False
    candidate_config_overrides: Optional[Dict[str, Any]] = field(default=None)

    def __post_init__(self) -> None:
        overrides = self.candidate_config_overrides
        if overrides is not None and "allow_enter" in overrides:
            raise ValueError(
                "experiment config overrides may never touch allow_enter"
            )


# Phase 8.1B1 experiment, preserved verbatim (codes, arms, labels, depth
# derivations). Its fingerprints and persisted rows are unchanged by 9D.
SMA150_V2_VS_V3 = ShadowExperiment(
    experiment_code=EXPERIMENT_CODE,
    experiment_version=EXPERIMENT_VERSION,
    control_pattern_code=CONTROL_PATTERN_CODE,
    candidate_pattern_code=CANDIDATE_PATTERN_CODE,
    control_arm_code=CONTROL_ARM_CODE,
    candidate_arm_code=CANDIDATE_ARM_CODE,
    control_category_label="v2",
    candidate_category_label="v3",
    control_history_bars=required_history_bars_v2,
    candidate_history_bars=required_history_bars_v3,
)

# Phase 9D2: wyckoff_mtf_v2 candidate vs the production baseline strategy.
# Phase 9E3 bumps the protocol version: the candidate now also receives a
# canonical completed 4H frame and an experiment-only enable_4h_trigger
# evaluation override, both of which are MATERIAL to pair fingerprints. No
# wyckoff_v2_shadow.v1 rows can exist anywhere (migration 013 has not been
# applied), so no recorded evidence changes identity.
WYCKOFF_V2_EXPERIMENT_CODE = "wyckoff_v2_vs_baseline"
WYCKOFF_V2_EXPERIMENT_VERSION = "wyckoff_v2_shadow.v2"
WYCKOFF_V2_CONTROL_ARM_CODE = "control_baseline"
WYCKOFF_V2_CANDIDATE_ARM_CODE = "candidate_wyckoff_v2"

# The experiment-only evaluation override: real completed-4H trigger analysis
# is measured inside the shadow run, while the STORED production rollout
# default (enable_4h_trigger=false) is never touched. allow_enter stays
# false everywhere — a confirmed trigger remains shadow-only evidence.
WYCKOFF_V2_CANDIDATE_CONFIG_OVERRIDES: Dict[str, Any] = {
    "enable_4h_trigger": True,
}

WYCKOFF_V2_VS_BASELINE = ShadowExperiment(
    experiment_code=WYCKOFF_V2_EXPERIMENT_CODE,
    experiment_version=WYCKOFF_V2_EXPERIMENT_VERSION,
    control_pattern_code="sma150_bounce",
    candidate_pattern_code="wyckoff_mtf_v2",
    control_arm_code=WYCKOFF_V2_CONTROL_ARM_CODE,
    candidate_arm_code=WYCKOFF_V2_CANDIDATE_ARM_CODE,
    control_category_label="control",
    candidate_category_label="candidate",
    control_history_bars=required_history_bars_v2,
    candidate_history_bars=required_history_bars_wyckoff_v2,
    candidate_data_meta_extras=_wyckoff_v2_data_meta_extras,
    requires_four_hour_frame=True,
    candidate_config_overrides=WYCKOFF_V2_CANDIDATE_CONFIG_OVERRIDES,
)

DEFAULT_EXPERIMENT = SMA150_V2_VS_V3

# Closed registry: declared experiments only, keyed by experiment_code.
EXPERIMENTS: Dict[str, ShadowExperiment] = {
    SMA150_V2_VS_V3.experiment_code: SMA150_V2_VS_V3,
    WYCKOFF_V2_VS_BASELINE.experiment_code: WYCKOFF_V2_VS_BASELINE,
}

# Every arm code any declared experiment may persist (kept in sync with the
# migration 010 + 013 CHECK constraint; tested).
KNOWN_ARM_CODES = (
    CONTROL_ARM_CODE,
    CANDIDATE_ARM_CODE,
    WYCKOFF_V2_CONTROL_ARM_CODE,
    WYCKOFF_V2_CANDIDATE_ARM_CODE,
)


def get_experiment(experiment_code: str) -> ShadowExperiment:
    """Resolve one declared experiment or raise UnknownShadowExperimentError."""
    try:
        return EXPERIMENTS[experiment_code]
    except KeyError:
        raise UnknownShadowExperimentError(
            f"No shadow experiment declared for code '{experiment_code}'. "
            f"Declared: {sorted(EXPERIMENTS.keys())}"
        )


def experiment_for_candidate(pattern_code: str) -> ShadowExperiment:
    """The declared experiment whose CANDIDATE arm is `pattern_code`.

    Deterministic: candidate pattern codes are unique across the registry
    (asserted by tests). Raises UnknownShadowExperimentError when no declared
    experiment shadows the strategy — there is no implicit fallback pairing.
    """
    for experiment in EXPERIMENTS.values():
        if experiment.candidate_pattern_code == pattern_code:
            return experiment
    raise UnknownShadowExperimentError(
        f"No shadow experiment declares candidate '{pattern_code}'. "
        "Declared candidates: "
        f"{sorted(e.candidate_pattern_code for e in EXPERIMENTS.values())}"
    )

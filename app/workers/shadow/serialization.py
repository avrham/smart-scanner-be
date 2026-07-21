"""Deterministic JSON-safety boundary for shadow persistence (8.1B1).

Strategy evaluators legitimately return non-JSON primitives inside their
details (e.g. sma150.v2 stores pandas.Timestamp objects in
details["bounces_detail"][*]["date"] straight from the DataFrame). Shadow
persistence is STRICT (json.dumps with allow_nan=False and no fallback), so
every value that will be hashed or persisted must first cross exactly one
explicit normalization boundary.

The contract is intentionally narrow and closed:

  converted deterministically
    * None, str, bool                -> unchanged
    * int                            -> unchanged
    * finite float                   -> unchanged
    * datetime / date / pd.Timestamp -> ISO-8601 string
    * numpy integer/floating scalar  -> .item(), then normalized
    * dict (string keys)             -> values normalized recursively
    * list / tuple                   -> normalized recursively, order kept

  rejected explicitly (ShadowSerializationError with a bounded reason code)
    * non-finite floats (NaN / Infinity)
    * set / frozenset (silent reordering would fake determinism)
    * bytes / bytearray
    * non-string dictionary keys
    * any other object — there is NO blanket str(value) fallback, unknown
      objects are never silently stringified.
"""

import json
import math
from datetime import date, datetime
from typing import Any

import numpy as np


class ShadowSerializationError(ValueError):
    """A value cannot be represented in the shadow JSON contract.

    Carries a machine-readable `reason_code` and a bounded `path` of dict
    keys / list indices — never the offending object's repr, so payload
    contents and secrets cannot leak into logs or telemetry.
    """

    def __init__(self, reason_code: str, path: str):
        self.reason_code = reason_code
        self.path = path
        super().__init__(f"{reason_code} at {path}")


def normalize_json_safe(value: Any, _path: str = "$") -> Any:
    """Recursively produce the deterministic JSON-safe form of `value`.

    Raises ShadowSerializationError for anything outside the closed contract
    documented in the module docstring. Semantic list order is preserved
    verbatim (chronological bounce order etc. is meaning, never re-sorted).
    """
    if value is None or isinstance(value, str):
        return value
    # bool before int: bool is an int subclass.
    if isinstance(value, bool):
        return value
    # numpy scalars before the Python numeric checks: np.float64 subclasses
    # float, and the contract requires conversion through .item().
    if isinstance(value, (np.integer, np.floating, np.bool_)):
        return normalize_json_safe(value.item(), _path)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ShadowSerializationError("non_finite_float", _path)
        return value
    # datetime before date is unnecessary (datetime subclasses date and both
    # normalize via isoformat), and pd.Timestamp subclasses datetime.
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        normalized = {}
        for key, item in value.items():
            if not isinstance(key, str):
                # Only the key's TYPE is reported — an arbitrary key value
                # must never leak into an error message.
                raise ShadowSerializationError(
                    f"non_string_dict_key:{type(key).__name__}", _path
                )
            normalized[key] = normalize_json_safe(item, f"{_path}.{key}")
        return normalized
    if isinstance(value, (list, tuple)):
        return [
            normalize_json_safe(item, f"{_path}[{i}]")
            for i, item in enumerate(value)
        ]
    if isinstance(value, (set, frozenset)):
        raise ShadowSerializationError("set_not_json_safe", _path)
    if isinstance(value, (bytes, bytearray)):
        raise ShadowSerializationError("bytes_not_json_safe", _path)
    raise ShadowSerializationError(
        f"unsupported_type:{type(value).__name__}", _path
    )


def strict_json(value: Any) -> str:
    """Strict JSON for shadow JSONB persistence.

    allow_nan=False and NO default fallback: a value that skipped the
    normalization boundary raises here instead of being silently stringified
    into the frozen snapshot. Compact separators keep snapshots bounded;
    semantic data is never altered.
    """
    return json.dumps(value, allow_nan=False, separators=(",", ":"))

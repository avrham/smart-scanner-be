"""Strict typed parameter boundary for shadow DB persistence (8.1B1).

asyncpg encodes parameters with PostgreSQL-type-specific codecs: a DATE
column requires datetime.date (a string raises "'str' object has no
attribute 'toordinal'"), TIMESTAMPTZ requires a datetime, UUID requires
uuid.UUID or a canonical string. The live pair_error was exactly this:
CanonicalFrame.frame_first_date/frame_last_date are ISO strings while
migration 010 declares those columns DATE.

Every typed parameter written to the strategy_shadow_* tables crosses one
of these pure converters. Each converter either returns the exact Python
type the codec expects or raises ShadowPersistenceTypeError with a
machine-readable reason code and the FIELD NAME ONLY — never the raw value,
so payload contents can never leak into logs or telemetry.
"""

import math
import uuid as uuid_lib
from datetime import date, datetime, timezone
from typing import Any, Optional


class ShadowPersistenceTypeError(TypeError):
    """A shadow DB parameter does not match its PostgreSQL column type.

    Deterministic application-level type failure — the runner classifies it
    separately (persistence_type_error) from unexpected database failures
    (pair_error). Carries only `reason_code` and `field`; the offending
    value itself is never included.
    """

    def __init__(self, reason_code: str, field: str):
        self.reason_code = reason_code
        self.field = field
        super().__init__(f"{reason_code} for field {field}")


def as_date_param(value: Any, field: str) -> date:
    """DATE column parameter.

    datetime.date passes through; datetime.datetime is EXPLICITLY converted
    via .date() (never silently — this converter IS the explicit boundary);
    an ISO YYYY-MM-DD string is parsed with date.fromisoformat. Anything
    else rejects.
    """
    # datetime (incl. pandas.Timestamp) BEFORE date: datetime subclasses date.
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            raise ShadowPersistenceTypeError("invalid_iso_date", field)
    raise ShadowPersistenceTypeError(
        f"expected_date_got_{type(value).__name__}", field
    )


def as_utc_datetime_param(value: Any, field: str) -> datetime:
    """TIMESTAMPTZ column parameter.

    Requires a timezone-AWARE datetime and normalizes it to UTC. Naive
    datetimes reject (guessing a zone would corrupt snapshot identity);
    strings reject (no implicit ISO parsing at this boundary).
    """
    if not isinstance(value, datetime):
        raise ShadowPersistenceTypeError(
            f"expected_datetime_got_{type(value).__name__}", field
        )
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ShadowPersistenceTypeError("naive_datetime", field)
    return value.astimezone(timezone.utc)


def as_uuid_param(value: Any, field: str) -> uuid_lib.UUID:
    """UUID column parameter: uuid.UUID passes, canonical string parses."""
    if isinstance(value, uuid_lib.UUID):
        return value
    if isinstance(value, str):
        try:
            return uuid_lib.UUID(value)
        except ValueError:
            raise ShadowPersistenceTypeError("invalid_uuid", field)
    raise ShadowPersistenceTypeError(
        f"expected_uuid_got_{type(value).__name__}", field
    )


def as_int_param(value: Any, field: str) -> int:
    """INT column parameter. bool is rejected (it IS an int subclass but a
    True bar count would be a silent data bug); numpy integers convert
    explicitly through .item()."""
    if isinstance(value, bool):
        raise ShadowPersistenceTypeError("bool_not_int", field)
    if isinstance(value, int):
        return value
    item = getattr(value, "item", None)
    if item is not None and callable(item):
        candidate = value.item()
        if isinstance(candidate, int) and not isinstance(candidate, bool):
            return candidate
    raise ShadowPersistenceTypeError(
        f"expected_int_got_{type(value).__name__}", field
    )


def as_score_param(value: Any, field: str) -> Optional[float]:
    """DOUBLE PRECISION score: finite Python float or None.

    bool rejects; int converts to float; numpy scalars convert explicitly
    through .item(); NaN and Infinity reject (a non-finite frozen score
    could never be reproduced through strict JSON telemetry)."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise ShadowPersistenceTypeError("bool_not_score", field)
    if not isinstance(value, (int, float)):
        item = getattr(value, "item", None)
        if item is not None and callable(item):
            return as_score_param(value.item(), field)
        raise ShadowPersistenceTypeError(
            f"expected_number_got_{type(value).__name__}", field
        )
    result = float(value)
    if not math.isfinite(result):
        raise ShadowPersistenceTypeError("non_finite_score", field)
    return result


def as_bool_param(value: Any, field: str) -> bool:
    """BOOLEAN column parameter: exact bool only."""
    if isinstance(value, bool):
        return value
    raise ShadowPersistenceTypeError(
        f"expected_bool_got_{type(value).__name__}", field
    )

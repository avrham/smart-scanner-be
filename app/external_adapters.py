"""Third-party payload adapters for the external-intelligence gateway.

PURE: no DB, no network, no clock of its own (the caller supplies `received_at`
so validation is fully reproducible in a test). Given one decoded payload, each
adapter either returns a canonical signal dict or raises `PayloadRejected` with
a short, stable, secret-free reason code.

WHY THE CONTRACT IS VERSIONED AND REQUIRED
------------------------------------------
A TradingView alert message is free text that the user types once and forgets
for months. If this gateway accepted whatever arrived, then the day the shape
changed we would not find out — we would simply start storing something else
under the same column names. So the payload must DECLARE which contract it was
written against, and the gateway validates against that declaration. An alert
configured today keeps working until the contract genuinely changes, and when
it does, the failure is loud and diagnosable instead of silent.

This is also why the gateway refuses a bare text alert. "Do not accept
arbitrary JSON and dump it into the database" is not only about injection: an
unvalidated payload produces rows whose meaning nobody can reconstruct later.

WHY `source` COMES FROM THE PAYLOAD BUT IS NOT TRUSTED
-----------------------------------------------------
The endpoint is deliberately generic — one static path serves every
TradingView-family indicator, so a new script never needs a new deployment.
The payload therefore names its source. That name is checked against the
registry's webhook-capable sources and nothing else: it selects a NORMALISER,
it does not grant anything. Authentication is the shared ingress token, which
lives in a header or the query string and never in the payload.

WHAT WE DO NOT DO
-----------------
No adapter here infers a confidence, converts an indicator's word into
BUY/SELL, or decides that one source outranks another. An adapter's whole job
is to move a claim across the boundary without changing what was claimed.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from app.external_signals import (
    DIRECTION_UNKNOWN, SOURCE_AI_EDGE, SOURCE_TRADINGVIEW,
    TRADINGVIEW_CONTRACT_VERSION, TYPE_CLASSIFICATION, TYPE_ENTRY, TYPE_EXIT,
    TYPE_REGIME_FILTER, TYPE_TREND, TYPE_UNKNOWN, WEBHOOK_SOURCES,
    normalize_direction, normalize_signal_type, normalize_timeframe,
)

#: Symbols are validated for SHAPE here and for MEMBERSHIP by the gateway (a
#: membership check needs the database). The pattern matches the DB CHECK
#: constraint exactly, so a payload that passes here cannot fail on INSERT.
SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,15}$")

#: Hard ceiling on one delivery body. A TradingView alert message is a few
#: hundred bytes; anything approaching this is not an alert.
MAX_PAYLOAD_BYTES = 8 * 1024

#: Bounds on the free-form metadata object, so an unbounded blob can never be
#: smuggled into a JSONB column through a field we deliberately left open.
MAX_METADATA_KEYS = 24
MAX_METADATA_BYTES = 2 * 1024
MAX_STRING_FIELD_CHARS = 256

#: How far the source's own timestamp may sit from arrival before the delivery
#: is refused. This IS the replay window: an attacker who captures a body
#: cannot usefully re-post it later, because the body is also fingerprinted for
#: exact-duplicate rejection and the clock check bounds everything else.
#:
#: Generous in both directions on purpose — chart timezones are a classic
#: misconfiguration and a 30-minute window absorbs an honest mistake while
#: still refusing yesterday's captured request.
MAX_CLOCK_SKEW_SECONDS = 30 * 60

#: Keys whose VALUE is dropped before the raw payload is persisted. The
#: contract says secrets never travel in the body; this is the belt to that
#: braces, so a user who pastes a token into their alert message once does not
#: leave it in our database forever.
_SECRET_KEY_HINTS = ("token", "secret", "password", "passwd", "api_key",
                     "apikey", "authorization", "auth", "signature", "key")

REDACTED = "[redacted]"


class PayloadRejected(Exception):
    """A delivery that must not become a signal.

    Carries a short, stable reason CODE — never an exception string, never a
    database message, never anything derived from the caller's credentials.
    The gateway returns the code and logs it; both are safe to expose.
    """

    def __init__(self, reason: str, detail: str = ""):
        super().__init__(reason)
        self.reason = reason
        self.detail = detail


# --------------------------------------------------------------------------- #
# field helpers
# --------------------------------------------------------------------------- #

def _text(value: Any, *, limit: int = MAX_STRING_FIELD_CHARS) -> Optional[str]:
    """A bounded, stripped string, or None. Never raises on odd input."""
    if value is None or isinstance(value, (dict, list)):
        return None
    text = str(value).strip()
    return text[:limit] if text else None


def parse_timestamp(value: Any) -> Optional[datetime]:
    """ISO 8601 -> aware UTC datetime, or None.

    TradingView emits `{{timenow}}` and `{{time}}` in ISO 8601. A value that
    arrives with no offset is read as UTC rather than as server-local time,
    which would otherwise shift the recorded skew by whole hours depending on
    where this process happens to run.
    """
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = _text(value, limit=64)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def redact(value: Any, *, depth: int = 0) -> Any:
    """Deep-copy a decoded payload with secret-shaped values removed.

    Applied before the raw body is persisted. Matching is on the KEY NAME, not
    on the value, because a value that merely looks random is usually a
    legitimate identifier and blanking it would destroy provenance.
    """
    if depth > 4:
        return REDACTED
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for key, item in list(value.items())[:MAX_METADATA_KEYS]:
            name = str(key)
            if any(hint in name.lower() for hint in _SECRET_KEY_HINTS):
                out[name] = REDACTED
            else:
                out[name] = redact(item, depth=depth + 1)
        return out
    if isinstance(value, list):
        return [redact(item, depth=depth + 1) for item in value[:MAX_METADATA_KEYS]]
    if isinstance(value, str):
        return value[:MAX_STRING_FIELD_CHARS]
    return value


def bound_metadata(value: Any) -> Dict[str, Any]:
    """A JSONB-safe, size-bounded, redacted metadata object.

    Anything that is not an object, or that will not fit, is replaced by an
    explicit marker rather than being silently truncated mid-structure — a
    half-stored object reads as complete and is worse than an honest note.
    """
    if not isinstance(value, dict):
        return {}
    cleaned = redact(value)
    try:
        encoded = json.dumps(cleaned, default=str)
    except (TypeError, ValueError):
        return {"_metadata_error": "not_serializable"}
    if len(encoded.encode("utf-8")) > MAX_METADATA_BYTES:
        return {"_metadata_error": "too_large",
                "_metadata_keys": sorted(cleaned.keys())[:MAX_METADATA_KEYS]}
    return cleaned


# --------------------------------------------------------------------------- #
# the generic TradingView-family adapter
# --------------------------------------------------------------------------- #

def normalize_tradingview(payload: Dict[str, Any], *,
                          received_at: datetime) -> Dict[str, Any]:
    """One validated TradingView-family alert -> one canonical signal.

    Serves EVERY TradingView indicator, ours or anyone else's. Source-specific
    behaviour is a thin layer on top (see `normalize_ai_edge`), never a second
    endpoint: a new script must not require a new deployment.
    """
    if not isinstance(payload, dict):
        raise PayloadRejected("payload_not_object")

    declared = _text(payload.get("contract_version"))
    if declared != TRADINGVIEW_CONTRACT_VERSION:
        # Named explicitly in the detail so a user who pasted an old template
        # can fix it without guessing. No internals leak: both strings are
        # public contract identifiers.
        raise PayloadRejected(
            "unsupported_contract_version",
            f"expected {TRADINGVIEW_CONTRACT_VERSION}")

    source = _text(payload.get("source")) or SOURCE_TRADINGVIEW
    source = source.lower()
    if source not in WEBHOOK_SOURCES:
        raise PayloadRejected("unknown_source")

    symbol = (_text(payload.get("symbol")) or "").upper()
    # TradingView's {{ticker}} is bare, but a user may paste {{syminfo.tickerid}}
    # which is EXCHANGE:SYMBOL. Taking the last segment is a shape fix, not an
    # interpretation — the exchange is preserved in metadata below.
    if ":" in symbol:
        symbol = symbol.rsplit(":", 1)[-1]
    if not SYMBOL_RE.match(symbol):
        raise PayloadRejected("invalid_symbol")

    # An unsubstituted placeholder means the user pasted the template into a
    # context where TradingView does not expand it. Storing "{{ticker}}" as a
    # symbol would be silently useless, so it is refused with a clear code.
    if "{{" in (payload.get("symbol") or ""):
        raise PayloadRejected("unsubstituted_placeholder")

    raw_type = _text(payload.get("signal_type")) or _text(payload.get("event"))
    if not raw_type:
        raise PayloadRejected("missing_signal_type")

    raw_direction = _text(payload.get("direction")) or _text(payload.get("signal"))
    raw_timeframe = _text(payload.get("timeframe")) or _text(payload.get("interval"))

    observed_at = parse_timestamp(payload.get("source_timestamp"))
    skew_seconds: Optional[int] = None
    if observed_at is not None:
        skew = (observed_at - received_at).total_seconds()
        if abs(skew) > MAX_CLOCK_SKEW_SECONDS:
            raise PayloadRejected("timestamp_out_of_window")
        skew_seconds = int(skew)

    confidence, confidence_scale, unscaled = _read_confidence(payload)

    metadata = bound_metadata(payload.get("metadata"))
    # Fields TradingView commonly supplies that are context rather than
    # semantics. Kept beside the signal, never promoted to a column, because
    # none of them is what the source is CLAIMING.
    #
    # `bar_time` deserves a note: it is `{{time}}`, the bar's OPEN time, and it
    # is NOT `{{timenow}}`. Only the latter is the moment the alert fired, so
    # only the latter belongs in `source_timestamp`. Recording the bar open as
    # a firing time would place a 4H signal up to four hours in the past.
    for key in ("exchange", "price", "close", "bar_time", "comment"):
        value = _text(payload.get(key))
        if value is not None:
            metadata.setdefault(key, value)
    # TradingView appends `_DL` / `_DLY` to {{exchange}} when the chart is on
    # delayed data. Kept verbatim above; the clean venue is recorded beside it
    # so a reader is not left parsing a suffix, and `data_delayed` states the
    # fact that suffix was actually carrying.
    exchange = metadata.get("exchange")
    if isinstance(exchange, str):
        for suffix in ("_DLY", "_DL"):
            if exchange.upper().endswith(suffix):
                metadata["exchange_venue"] = exchange[: -len(suffix)]
                metadata["data_delayed"] = True
                break
        else:
            metadata.setdefault("data_delayed", False)
    if unscaled is not None:
        # A number whose scale the source did not state. Preserved as the
        # claim it is, and deliberately NOT stored in `confidence`, because a
        # confidence whose meaning is unknown is not a measurement.
        metadata["unscaled_confidence"] = unscaled

    return {
        "source": source,
        "source_signal_id": _text(payload.get("source_signal_id")),
        "symbol": symbol,
        "observed_at": observed_at,
        "received_at": received_at,
        # THE GATE. Arrival, always — never the source's own clock. See the
        # module docstring of app/external_signals.py.
        "effective_at": received_at,
        "clock_skew_seconds": skew_seconds,
        "timeframe": raw_timeframe,
        "timeframe_normalized": normalize_timeframe(raw_timeframe),
        "signal_type": raw_type,
        "signal_type_normalized": normalize_signal_type(raw_type),
        "direction": raw_direction,
        "direction_normalized": normalize_direction(raw_direction),
        "confidence": confidence,
        "confidence_scale": confidence_scale,
        "indicator": _text(payload.get("indicator")),
        "indicator_version": _text(payload.get("indicator_version")),
        "alert_id": _text(payload.get("alert_id")),
        "contract_version": TRADINGVIEW_CONTRACT_VERSION,
        "source_payload_version": _text(payload.get("source_payload_version")),
        "source_metadata": metadata,
        "supersedes_signal_id": _text(payload.get("supersedes_signal_id")),
    }


def _read_confidence(payload: Dict[str, Any]):
    """(confidence, scale, unscaled). Never invents a number.

    A confidence is only stored when the source ALSO said what the number
    means. Otherwise the value is preserved in metadata as `unscaled_confidence`
    and the product reports confidence as unavailable — which is the truth.
    """
    raw = payload.get("confidence")
    if raw is None or raw == "":
        return None, None, None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None, None, _text(raw, limit=64)
    scale = _text(payload.get("confidence_scale"))
    if not scale:
        return None, None, value
    return value, scale, None


# --------------------------------------------------------------------------- #
# AI Edge
# --------------------------------------------------------------------------- #
# AI Edge reaches us through the SAME gateway as any other TradingView script,
# because that is genuinely all it is from our side: an indicator whose alert
# message the account owner configures. This normaliser adds exactly two things
# the generic path cannot know, and nothing else.
#
# WHAT IS DELIBERATELY NOT DONE HERE
# ----------------------------------
# No attempt is made to reconstruct the indicator's internal state, infer a
# score it did not publish, or model its algorithm. We consume its ALERT
# OUTPUT — the part it is designed to emit — and nothing that is not offered.
#
# CONFIDENCE IS UNAVAILABLE, AND THAT IS A MEASURED FACT
# ------------------------------------------------------
# The indicator does compute an internal vote score, but it renders it with
# `label.new()`. TradingView's `{{plot_N}}` placeholders can only read values
# published through `plot()`, and the script's two plots are a price level
# (the kernel regression estimate) and a categorical backtest stream. There is
# therefore NO path by which a real confidence reaches an alert message.
#
# So `confidence` stays NULL and the product reports it as unavailable. It is
# not approximated from the vote count, not derived from the signal type, and
# not defaulted. This is the single most important honesty rule in this file:
# a number that was never measured would be read as a measurement.
#
# ALERT SEMANTICS ARE THE INDICATOR'S OWN
# ---------------------------------------
# The open-source script exposes its alerts through `alertcondition()` — eight
# discrete conditions, listed below. `alertcondition()` means the alert message
# is fully user-editable (the built-in text is only a prefill), which is what
# makes a structured JSON contract possible at all. It also means there is no
# "Any alert() function call" option, so each condition needs its own alert.
#
# The supplied templates carry explicit `signal_type` and `direction`, so
# normalisation needs no inference. This map exists for the other case: a user
# who sends the condition NAME instead. Anything unlisted falls through to
# `unknown` with the raw word preserved — an indicator update adds a row here
# rather than silently mis-labelling.

_AI_EDGE_TYPES: Dict[str, str] = {
    # Generic words a hand-written alert may use.
    "classification": TYPE_CLASSIFICATION,
    "prediction": TYPE_CLASSIFICATION,
    "signal": TYPE_CLASSIFICATION,
    "kernel": TYPE_REGIME_FILTER,
    "filter": TYPE_REGIME_FILTER,
    "regime": TYPE_REGIME_FILTER,
    # The eight open-source alert conditions, verbatim.
    "open_long": TYPE_ENTRY,
    "open_short": TYPE_ENTRY,
    "open_position": TYPE_ENTRY,
    "close_long": TYPE_EXIT,
    "close_short": TYPE_EXIT,
    "close_position": TYPE_EXIT,
    # A kernel colour change is a regime READING that flipped, not an entry.
    # Calling it an entry would promote a filter into a trade signal.
    "kernel_bullish_color_change": TYPE_TREND,
    "kernel_bearish_color_change": TYPE_TREND,
}

#: Direction implied by a condition NAME, used only when the payload supplied
#: no direction of its own. The condition names literally contain the word, so
#: this is reading the source's label — not inferring a view it did not state.
#: A `close_position` / `open_position` alert is direction-agnostic by design
#: and is deliberately absent, so it normalises to `unknown` rather than being
#: assigned a side the indicator never claimed.
_AI_EDGE_DIRECTIONS: Dict[str, str] = {
    "open_long": "bullish",
    "close_long": "bearish",
    "open_short": "bearish",
    "close_short": "bullish",
    "kernel_bullish_color_change": "bullish",
    "kernel_bearish_color_change": "bearish",
}

DEFAULT_AI_EDGE_INDICATOR = "ai_edge"


def normalize_ai_edge(payload: Dict[str, Any], *,
                      received_at: datetime) -> Dict[str, Any]:
    """AI Edge alert -> canonical signal. Built ON TOP of the generic adapter."""
    signal = normalize_tradingview(payload, received_at=received_at)

    # 1. Identity. An AI Edge alert that omits the indicator name is still an
    #    AI Edge alert; recording it as anonymous would lose that.
    if not signal.get("indicator"):
        signal["indicator"] = DEFAULT_AI_EDGE_INDICATOR

    # 2. Vocabulary. Its own words for what an alert IS, where the generic map
    #    would have shrugged. Only applied when the generic path could not
    #    read the word — a source-specific map must never override a mapping
    #    the shared vocabulary already agrees on.
    condition = _condition_token(signal.get("signal_type"))
    if signal["signal_type_normalized"] == TYPE_UNKNOWN and condition:
        signal["signal_type_normalized"] = _AI_EDGE_TYPES.get(
            condition, TYPE_UNKNOWN)

    # 3. Direction, ONLY when the payload stated none and the condition name
    #    itself says it. `close_position` and `open_position` are excluded from
    #    the map on purpose: they are direction-agnostic conditions, and
    #    assigning them a side would invent a claim.
    if signal["direction_normalized"] == DIRECTION_UNKNOWN and condition:
        implied = _AI_EDGE_DIRECTIONS.get(condition)
        if implied:
            signal["direction_normalized"] = implied
            # The raw column keeps saying what arrived. Only the normalised
            # reading was derived, and `direction` staying None is the record
            # that the source never sent one.

    return signal


def _condition_token(raw: Any) -> str:
    """'Open Long' / 'openLong' / 'open-long' -> 'open_long'.

    AI Edge's condition names appear in the wild with several spellings
    depending on whether a user typed them or copied the indicator's label.
    Normalising the SHAPE is not the same as normalising the MEANING: this
    only decides which map entry to look up.
    """
    token = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", str(raw or ""))
    token = re.sub(r"[^a-z0-9]+", "_", token.lower()).strip("_")
    return token


#: source -> normaliser. Adding a TradingView-family source is one entry here
#: plus one registry row; it is never a new endpoint, a new mode or a new deploy
#: target.
NORMALIZERS = {
    SOURCE_TRADINGVIEW: normalize_tradingview,
    SOURCE_AI_EDGE: normalize_ai_edge,
}


def normalize(payload: Dict[str, Any], *,
              received_at: datetime) -> Dict[str, Any]:
    """Dispatch to the right adapter by the payload's declared source.

    The generic adapter runs first for every source, so contract validation,
    symbol shape, clock skew and metadata bounds are enforced identically
    regardless of who is sending — a source-specific path can never be the way
    a malformed payload gets in.
    """
    source = (_text((payload or {}).get("source")) or SOURCE_TRADINGVIEW).lower()
    normalizer = NORMALIZERS.get(source)
    if normalizer is None:
        # Still run the generic path so the caller gets the precise reason
        # (unknown_source) rather than a generic failure.
        return normalize_tradingview(payload, received_at=received_at)
    return normalizer(payload, received_at=received_at)


__all__ = [
    "SYMBOL_RE", "MAX_PAYLOAD_BYTES", "MAX_METADATA_KEYS",
    "MAX_METADATA_BYTES", "MAX_STRING_FIELD_CHARS", "MAX_CLOCK_SKEW_SECONDS",
    "REDACTED", "PayloadRejected", "parse_timestamp", "redact",
    "bound_metadata", "normalize_tradingview", "normalize_ai_edge",
    "DEFAULT_AI_EDGE_INDICATOR", "NORMALIZERS", "normalize",
]

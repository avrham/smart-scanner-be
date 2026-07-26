"""Explicit experiment-universe identity (shadow_universe_identity.v1).

Proves the repository's explicit-universe architecture is preserved: a
prospective universe is supplied explicitly, normalized deterministically and
hashed into a stable identity that is independent of input order/casing/
duplicates and changes whenever membership changes.
"""

from __future__ import annotations

import pytest

from app.workers.shadow.campaigns import CampaignRequestError
from app.workers.shadow.universe_identity import (
    UNIVERSE_IDENTITY_VERSION,
    compute_universe_hash,
    inspect_universe_symbols,
    normalize_campaign_symbols,
    parse_symbol_file_text,
    universe_identity,
)


class TestNormalization:
    def test_trims_uppercases_dedupes_sorts(self):
        assert normalize_campaign_symbols(
            [" msft ", "AAPL", "aapl", "nvda"]
        ) == ["AAPL", "MSFT", "NVDA"]

    def test_explicit_non_empty_list_required(self):
        with pytest.raises(CampaignRequestError):
            normalize_campaign_symbols([])
        with pytest.raises(CampaignRequestError):
            normalize_campaign_symbols("AAPL")  # not a list

    def test_all_blank_rejected(self):
        with pytest.raises(CampaignRequestError):
            normalize_campaign_symbols(["", "   "])

    def test_malformed_symbol_rejected(self):
        with pytest.raises(CampaignRequestError):
            normalize_campaign_symbols(["AAPL", "bad symbol!"])


class TestUniverseIdentity:
    def test_contract_and_shape(self):
        identity = universe_identity(["AAPL", "MSFT"])
        assert identity["universe_identity_version"] == UNIVERSE_IDENTITY_VERSION
        assert identity["symbol_count"] == 2
        assert identity["symbols"] == ["AAPL", "MSFT"]
        assert isinstance(identity["universe_hash"], str)
        assert len(identity["universe_hash"]) == 64  # sha256 hex

    def test_hash_is_order_and_case_and_dup_independent(self):
        a = universe_identity(["AAPL", "MSFT", "NVDA"])
        b = universe_identity(["nvda", "aapl", "MSFT", "aapl"])
        assert a["universe_hash"] == b["universe_hash"]
        assert a["symbol_count"] == b["symbol_count"] == 3

    def test_different_membership_changes_hash(self):
        a = universe_identity(["AAPL", "MSFT"])
        b = universe_identity(["AAPL", "NVDA"])
        assert a["universe_hash"] != b["universe_hash"]

    def test_count_alone_does_not_prove_membership(self):
        # Two DIFFERENT 3-symbol universes must not share an identity.
        a = universe_identity(["AAPL", "MSFT", "NVDA"])
        b = universe_identity(["TSLA", "AMD", "INTC"])
        assert a["symbol_count"] == b["symbol_count"]
        assert a["universe_hash"] != b["universe_hash"]

    def test_hash_is_versioned_prefix(self):
        # Bare join without the version prefix must differ from the contract.
        import hashlib

        symbols = ["AAPL", "MSFT"]
        bare = hashlib.sha256("\n".join(symbols).encode()).hexdigest()
        assert compute_universe_hash(symbols) != bare


class TestSymbolFileParsing:
    def test_parses_newline_comma_and_whitespace(self):
        text = "AAPL, MSFT\nNVDA TSLA\n# comment\n\n  AMD  "
        assert parse_symbol_file_text(text) == [
            "AAPL", "MSFT", "NVDA", "TSLA", "AMD",
        ]

    def test_file_text_round_trips_into_identity(self):
        text = "msft\naapl\n# frozen universe\nnvda"
        tokens = parse_symbol_file_text(text)
        identity = universe_identity(tokens)
        assert identity["symbols"] == ["AAPL", "MSFT", "NVDA"]


class TestOperatorValidation:
    def test_clean_50_passes(self):
        symbols = [f"SYM{i:02d}" for i in range(50)]
        report = inspect_universe_symbols(symbols, expected_count=50)
        assert report["ok"] is True
        assert report["problems"] == []
        assert report["unique_count"] == 50
        assert report["universe_hash"]

    def test_empty_input_flagged(self):
        assert inspect_universe_symbols([])["problems"] == ["empty"]
        assert inspect_universe_symbols(["", "  "])["problems"] == ["empty"]

    def test_invalid_symbols_collected_not_first_only(self):
        report = inspect_universe_symbols(["AAPL", "bad!", "@x", "MSFT"])
        assert "invalid_symbols" in report["problems"]
        assert report["invalid_tokens"] == ["@X", "BAD!"]

    def test_duplicates_reported_not_silently_cleaned(self):
        # Normalization dedupes for identity, but the report must SURFACE the
        # duplicate so the operator does not think the file was clean.
        report = inspect_universe_symbols(["aapl", "AAPL", "MSFT"])
        assert report["duplicates_supplied"] == ["AAPL"]
        assert "duplicate_symbols" in report["problems"]
        assert report["ok"] is False
        # identity still computed over the unique set
        assert report["unique_count"] == 2

    def test_wrong_count_flagged(self):
        report = inspect_universe_symbols(["AAPL", "MSFT"], expected_count=50)
        assert "unexpected_count" in report["problems"]
        assert report["ok"] is False

    def test_report_hash_matches_identity(self):
        symbols = ["MSFT", "AAPL", "NVDA"]
        report = inspect_universe_symbols(symbols)
        assert report["universe_hash"] == universe_identity(symbols)[
            "universe_hash"
        ]

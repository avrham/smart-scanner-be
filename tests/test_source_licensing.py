"""The licence boundary, enforced in three independent places.

This file exists because "we just never wired it up" stops being a guarantee
the moment a second restricted source arrives. Wave 2 adds one — analyst grade
changes from the same provider as the movers feed, and far more tempting to put
on a symbol screen than a movers list is.

The three layers, each tested here:
  1. the vocabulary and the single predicate that decides display;
  2. the Product API's registry filter, so a restricted source is not even
     named to a reader;
  3. a read of the router source, so a future edit that reaches for a forbidden
     relation fails a test rather than a licence.
"""

import app.external_signals as ex
import app.source_licensing as lic


def _executable_source(path: str) -> str:
    """A module's code with its DOCSTRINGS and comments removed — SQL kept.

    The prose in these files names the forbidden relations on purpose: saying
    which tables the Product API must never read is the documentation doing its
    job, and matching raw text would fail on a correct file. But the SQL
    literals must SURVIVE the strip, because a query string is exactly where an
    accidental read would appear. So this blanks string EXPRESSION STATEMENTS
    (which is precisely what a docstring is) and nothing else; `ast.unparse`
    drops `#` comments on its own.
    """
    import ast

    tree = ast.parse(open(path, encoding="utf-8").read())
    for node in ast.walk(tree):
        for field in ("body", "orelse", "finalbody"):
            statements = getattr(node, field, None)
            if not isinstance(statements, list):
                continue
            for statement in statements:
                if (isinstance(statement, ast.Expr)
                        and isinstance(statement.value, ast.Constant)
                        and isinstance(statement.value.value, str)):
                    statement.value.value = ""
    return ast.unparse(tree)


class TestVocabulary:
    def test_only_an_explicit_allowance_permits_display(self):
        assert lic.is_product_displayable(lic.LICENSING_PRODUCT_DISPLAY)
        assert not lic.is_product_displayable(lic.LICENSING_INTERNAL_ONLY)
        # "Nobody has established the position" is not permission.
        assert not lic.is_product_displayable(lic.LICENSING_UNKNOWN)
        assert not lic.is_product_displayable(None)
        assert not lic.is_product_displayable("")

    def test_an_unknown_source_defaults_closed(self):
        assert lic.resolve_visibility("some_new_vendor") \
            == lic.LICENSING_UNKNOWN
        assert not lic.source_is_product_displayable("some_new_vendor")

    def test_the_registry_may_tighten_but_not_invent_a_class(self):
        # An operator can restrict a source without a deploy...
        assert lic.resolve_visibility("ai_edge", lic.LICENSING_INTERNAL_ONLY) \
            == lic.LICENSING_INTERNAL_ONLY
        # ...but a value we do not recognise falls back to the code position,
        # never to whatever the database happened to contain.
        assert lic.resolve_visibility("fmp", "totally_fine_honest") \
            == lic.LICENSING_INTERNAL_ONLY

    def test_fmp_is_internal_only(self):
        # The measured position: individual plans are personal and
        # non-commercial and forbid third-party access to the data.
        assert lic.SOURCE_LICENSING["fmp"] == lic.LICENSING_INTERNAL_ONLY

    def test_the_government_calendars_are_displayable(self):
        for source in ("federal_reserve", "bea"):
            assert lic.source_is_product_displayable(source), source

    def test_every_declared_class_is_in_the_vocabulary(self):
        for source, cls in lic.SOURCE_LICENSING.items():
            assert cls in lic.LICENSING_CLASSES, source


class TestRegistryFilter:
    def test_an_internal_only_source_is_not_even_named(self):
        rows = [{"source": "ai_edge", "licensing_visibility": None},
                {"source": "fmp", "licensing_visibility": None},
                {"source": "finviz", "licensing_visibility": None}]
        visible = [r["source"] for r in lic.product_visible_rows(rows)]
        assert visible == ["ai_edge"]

    def test_the_class_is_stamped_onto_what_survives(self):
        rows = [{"source": "federal_reserve", "licensing_visibility": None}]
        assert lic.product_visible_rows(rows)[0]["licensing_visibility"] \
            == lic.LICENSING_PRODUCT_DISPLAY

    def test_a_database_still_on_the_old_migration_still_filters(self):
        # No `licensing_visibility` column at all: the code position applies.
        rows = [{"source": "fmp"}, {"source": "tradingview"}]
        assert [r["source"] for r in lic.product_visible_rows(rows)] \
            == ["tradingview"]

    def test_the_filter_is_applied_in_the_product_path(self):
        router = open("app/routers/scanner.py", encoding="utf-8").read()
        assert "lic.product_visible_rows" in router


class TestLeakDetection:
    def test_a_forbidden_name_anywhere_in_a_payload_is_found(self):
        payload = {"external_intelligence": {
            "sources": [{"source": "ai_edge"}, {"source": "fmp"}]}}
        assert lic.find_licensing_leaks(payload) == ["fmp"]

    def test_a_clean_payload_reports_nothing(self):
        payload = {"external_intelligence": {
            "sources": [{"source": "ai_edge"}, {"source": "tradingview"}]},
            "market_calendar_context": {"sources": [{"source": "bea"}]}}
        assert lic.find_licensing_leaks(payload) == []

    def test_it_looks_at_keys_as_well_as_values(self):
        assert lic.find_licensing_leaks({"per_source": {"fmp": {}}}) == ["fmp"]

    def test_a_substring_is_not_a_match(self):
        # "fmp_notes" is not the source `fmp`; a substring rule would produce
        # false positives that trained everyone to ignore this test.
        assert lic.find_licensing_leaks({"note": "fmp_notes are internal"}) == []


class TestProductApiBoundary:
    def test_the_router_never_names_a_forbidden_relation(self):
        for relation in lic.PRODUCT_FORBIDDEN_RELATIONS:
            assert relation not in _executable_source("app/routers/scanner.py"), \
                relation

    def test_no_router_in_the_tree_names_a_forbidden_relation(self):
        import pathlib
        for path in pathlib.Path("app/routers").glob("*.py"):
            source = _executable_source(str(path))
            for relation in ("external_discovery_candidates",
                             "analyst_grade_events"):
                assert relation not in source, f"{path.name}: {relation}"

    def test_the_product_reader_role_is_not_granted_the_restricted_tables(self):
        grants = open("ops/sql/create_smart_scanner_product_reader.sql",
                      encoding="utf-8").read()
        for relation in ("external_discovery_candidates",
                         "analyst_grade_events"):
            assert f"GRANT SELECT ON public.{relation}" not in grants, relation
        # The displayable one IS granted, so the boundary is a decision rather
        # than a blanket refusal.
        assert "GRANT SELECT ON public.macro_events" in grants

    def test_the_source_strip_keeps_sql_literals(self):
        # Guards the guard: if _executable_source ever dropped query strings,
        # every assertion above would pass vacuously.
        source = _executable_source("app/routers/scanner.py")
        assert "FROM public.external_signals" in source
        assert "FROM public.macro_events" in source

    def test_the_source_entry_carries_the_class_to_the_reader(self):
        entry = ex.build_source_entry(
            {"source": "ai_edge", "display_name": "AI Edge",
             "status": "live",
             "licensing_visibility": lic.LICENSING_PRODUCT_DISPLAY})
        assert entry["licensing_visibility"] == lic.LICENSING_PRODUCT_DISPLAY

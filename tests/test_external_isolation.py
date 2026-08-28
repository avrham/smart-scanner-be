"""External signals are EVIDENCE. This file is the structural proof.

The promise made in every docstring is that a third party cannot reach the
Wyckoff verdict, the attention tier, the ordering or ENTER eligibility. A
promise in prose survives exactly until someone adds a convenient import. These
tests assert the boundary mechanically:

  * no module that decides what the scanner SAYS can reach the external layer;
  * the external layer produces nothing rankable;
  * the internet-facing write is unreachable from the read-only product app,
    and the scanner surface is unreachable from the ingress app;
  * adding it left Earnings, News and SEC exactly where they were.
"""

import ast
import pathlib

import app.audit_mode as am
import app.external_ingest_mode as eim
import app.external_signals as ex
import app.scanner_view as sv

APP = pathlib.Path(__file__).resolve().parents[1] / "app"
OPS = pathlib.Path(__file__).resolve().parents[1] / "ops" / "sql"

# Everything that decides WHAT the scanner says about a setup. None of these
# may know that external signals exist.
DECISION_MODULES = (
    "scanner_view.py",
    "market_context.py",
    "prospective_campaign.py",
    "prospective_readiness.py",
    "prospective_session.py",
    "reference_market.py",
    "catalyst.py",      # earnings must not learn about external signals
    "news.py",          # nor may news
    "sec_events.py",    # nor may SEC
)


def imported_names(path: pathlib.Path):
    tree = ast.parse(path.read_text())
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


class TestNoDecisionModuleKnowsAboutExternalSignals:
    def test_decision_modules_do_not_import_the_external_layer(self):
        offenders = {}
        for name in DECISION_MODULES:
            path = APP / name
            assert path.exists(), f"{name} moved; update this guard"
            bad = {m for m in imported_names(path)
                   if m.split(".")[-1].startswith("external")}
            if bad:
                offenders[name] = sorted(bad)
        assert offenders == {}, (
            "a decision module imported the external layer — a third party "
            f"must never be reachable from a verdict: {offenders}")

    def test_decision_modules_never_mention_external_signals_at_all(self):
        # Stronger than the import check: catches a dict lookup like
        # row["external_intelligence"] sneaking into a ranking function.
        offenders = []
        for name in DECISION_MODULES:
            text = (APP / name).read_text()
            for needle in ("external_intelligence", "external_signal",
                           "ai_edge", "tradingview"):
                if needle in text:
                    offenders.append(f"{name}:{needle}")
        assert offenders == [], (
            f"a decision module referenced the external layer: {offenders}")

    def test_the_dependency_runs_one_way_only(self):
        # The external layer reads the attention vocabulary; the attention
        # layer must not read anything of ours. A cycle here would be the
        # mechanism by which confluence started feeding the tier it describes.
        assert "scanner_view" not in {
            m.split(".")[-1] for m in imported_names(APP / "external_signals.py")}


class TestTheExternalLayerProducesNothingRankable:
    def test_the_context_block_carries_no_score(self):
        block = ex.build_external_context(
            [], as_of_session=None, sources=[],
            freshness={"status": ex.STATUS_AVAILABLE, "reason": None,
                       "age_hours": 1.0})
        forbidden = ("score", "rank", "weight", "probability", "confidence_avg",
                     "strength", "conviction", "priority")
        found = [k for k in block if any(f in k for f in forbidden)]
        assert found == [], f"external context exposed a rankable field: {found}"

    def test_confluence_is_a_closed_set_of_words(self):
        assert all(isinstance(state, str) for state in ex.CONFLUENCE_STATES)
        # If this ever became ordered or numeric, a UI would sort on it.
        assert not any(state.isdigit() for state in ex.CONFLUENCE_STATES)

    def test_the_attention_vocabulary_has_not_drifted(self):
        # `INTERNAL_INTERESTED_TIERS` is duplicated by value in
        # external_signals so that module stays pure. That duplication is only
        # safe while this assertion holds.
        for tier in ex.INTERNAL_INTERESTED_TIERS:
            assert tier in sv.ATTENTION_TIERS, (
                f"attention tier '{tier}' no longer exists; the confluence "
                "reading is silently wrong")

    def test_the_row_summary_cannot_reorder_a_list(self):
        row = ex.build_row_external(ex.empty_external_context())
        numeric = [k for k, v in row.items() if isinstance(v, (int, float))
                   and not isinstance(v, bool)]
        # Counts are allowed; anything else numeric would invite a sort.
        assert set(numeric) <= {"notable_count", "in_window_count"}, numeric


class TestRouteGateIsolation:
    def test_the_ingress_is_unreachable_from_the_read_only_product_app(self):
        # The Product API app runs AUDIT_ONLY_MODE. If the webhook path ever
        # appeared in that allowlist, an anonymous POST would arrive at an app
        # whose database role cannot write — and the failure mode would be a
        # confusing 500 rather than a clean 404.
        assert eim.EXTERNAL_SIGNAL_INGRESS_PATH not in am.AUDIT_ONLY_ALLOWLIST

    def test_audit_mode_permits_no_write_method_at_all(self):
        assert "POST" not in am.AUDIT_ONLY_METHODS
        assert not am.is_audit_route_allowed(
            "POST", eim.EXTERNAL_SIGNAL_INGRESS_PATH)

    def test_the_scanner_surface_is_unreachable_from_the_ingress_app(self):
        # The reverse direction, and the one that matters more: the ingress is
        # internet-facing, so the scanner must not be reachable from it.
        for path in ("/api/scanner/overview", "/api/scanner/symbol",
                     "/api/scanner/scans", "/api/admin/prospective/execute",
                     "/docs", "/openapi.json"):
            assert not eim.is_external_ingest_route_allowed("GET", path), path
            assert not eim.is_external_ingest_route_allowed("POST", path), path

    def test_the_ingress_mode_permits_exactly_one_write_path(self):
        writable = [path for path, methods in eim.EXTERNAL_INGEST_ALLOWLIST.items()
                    if "POST" in methods]
        assert writable == [eim.EXTERNAL_SIGNAL_INGRESS_PATH]

    def test_liveness_stays_read_only_even_in_ingress_mode(self):
        assert eim.is_external_ingest_route_allowed("GET", "/version")
        assert not eim.is_external_ingest_route_allowed("POST", "/version")

    def test_an_unknown_path_is_refused(self):
        assert not eim.is_external_ingest_route_allowed(
            "POST", "/api/external/signals/../../admin")


class TestModeExclusivity:
    def test_external_ingest_mode_is_declared_mutually_exclusive(self):
        # Running the ingress alongside audit mode would put an anonymous POST
        # in front of the read-only product role; alongside prospective mode it
        # would sit in front of a role that can write scanner evaluations.
        main = (APP.parent / "main.py").read_text()
        assert "EXTERNAL_INGEST_ONLY_MODE" in main
        guard = main.split("EXTERNAL_INGEST_ONLY_MODE and (")[1].split(")")[0]
        for other in ("AUDIT_ONLY_MODE", "MAINTENANCE_ONLY_MODE",
                      "HISTORY_WARMUP_ONLY_MODE",
                      "PROSPECTIVE_CAMPAIGN_ONLY_MODE"):
            assert other in guard, f"{other} missing from the exclusivity guard"

    def test_the_ingress_refuses_to_boot_without_a_credential(self):
        main = (APP.parent / "main.py").read_text()
        assert "EXTERNAL_INGEST_TOKEN (the ingress fails closed without it)" in main


class TestPrivilegeBoundaryIsDeclared:
    def test_the_product_reader_is_granted_what_the_product_api_reads(self):
        grants = (OPS / "create_smart_scanner_product_reader.sql").read_text()
        for relation in ("external_signals", "external_signal_sources"):
            assert f"GRANT SELECT ON public.{relation}" in grants, relation

    def test_the_product_reader_never_sees_raw_third_party_payloads(self):
        grants = (OPS / "create_smart_scanner_product_reader.sql").read_text()
        assert "GRANT SELECT ON public.external_signal_deliveries" not in grants

    def test_the_product_reader_gains_no_write_from_this_milestone(self):
        # Inspect the GRANT STATEMENTS only. The file also lists the withheld
        # verbs in a trailing comment, which is documentation rather than a
        # privilege and must not be read as one.
        statements = [line for line
                      in (OPS / "create_smart_scanner_product_reader.sql")
                      .read_text().splitlines()
                      if line.strip().upper().startswith("GRANT ")]
        table_grants = [s for s in statements if " ON public." in s]
        assert table_grants, "no table grants found; the guard would pass vacuously"
        for statement in table_grants:
            assert statement.strip().upper().startswith("GRANT SELECT ON"), (
                f"product reader gained a non-SELECT privilege: {statement}")

    def test_the_ingest_role_holds_no_scanner_privilege(self):
        role = (OPS / "create_smart_scanner_external_ingest.sql").read_text()
        for relation in ("strategy_shadow_runs", "strategy_shadow_evaluations",
                         "strategy_shadow_pair_outcomes", "daily_bars",
                         "market_bars_4h", "patterns", "job_tasks"):
            assert f"GRANT SELECT ON public.{relation}" not in role, relation
            assert f"ON public.{relation} TO smart_scanner_external_ingest" \
                not in role, relation

    def test_the_ingest_role_can_never_delete(self):
        role = (OPS / "create_smart_scanner_external_ingest.sql").read_text()
        assert "GRANT DELETE" not in role
        assert "DELETE ON" not in role

    def test_the_shared_freshness_table_is_namespace_confined(self):
        # Without this predicate a leaked ingress credential could mark the SEC
        # or news dimension as failed and quietly degrade a dimension it has
        # nothing to do with.
        role = (OPS / "create_smart_scanner_external_ingest.sql").read_text()
        update_policy = role.split(
            "CREATE POLICY smart_scanner_external_ingest_state_update")[1]
        assert "external\\_%" in update_policy


class TestExistingDimensionsAreUntouched:
    def test_external_intelligence_is_not_nested_inside_catalyst_context(self):
        scanner = (APP / "routers" / "scanner.py").read_text()
        for wrong in ('catalyst_context["external',
                      'catalyst_context"]["external',
                      "catalyst_context['external"):
            assert wrong not in scanner, (
                "external intelligence was nested inside catalyst_context — an "
                "opinion must not sit among formal disclosures")

    def test_the_three_catalyst_dimensions_still_load_independently(self):
        scanner = (APP / "routers" / "scanner.py").read_text()
        for loader in ("_load_catalysts", "_load_news", "_load_sec",
                       "_load_external"):
            assert scanner.count(loader) >= 2, (
                f"{loader} lost its independent call sites")

    def test_each_dimension_has_its_own_failure_boundary(self):
        # Four dimensions, four try/except blocks in the overview. One shared
        # handler would mean a third-party outage silences the SEC filings.
        scanner = (APP / "routers" / "scanner.py").read_text()
        overview = scanner.split("async def scanner_overview")[1].split(
            "async def scanner_symbol_detail")[0]
        assert overview.count("except Exception:") >= 4

    def test_the_sec_and_news_contract_versions_are_unchanged(self):
        import app.news as nw
        import app.sec_events as se
        assert se.SEC_EVENTS_CONTRACT_VERSION == "smart_scanner_sec_events.v1"
        assert nw.NEWS_CONTEXT_CONTRACT_VERSION.endswith(".v1")


class TestNoAutomatedExecution:
    def test_the_external_layer_contains_no_execution_vocabulary(self):
        # The hard safety boundary: this system records opinions and measures
        # them. Nothing here may read as an instruction to trade.
        for name in ("external_signals.py", "external_adapters.py",
                     "external_ingest.py", "routers/external.py"):
            text = (APP / name).read_text().lower()
            for word in ("broker", "place_order", "submit_order", "execute_trade",
                         "position_size", "order_qty"):
                assert word not in text, f"{name} mentions {word}"

    def test_buy_and_sell_are_not_part_of_our_direction_vocabulary(self):
        assert "buy" not in ex.DIRECTIONS
        assert "sell" not in ex.DIRECTIONS

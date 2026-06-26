"""Unit tests for the declared-inputs gate (AIDPF-2084 / AIDPF-2085).

Two layers:
* `extract_upstream_reads` (pure, no pack) — the conservative block-scoped
  extractor: qualified reads, wildcard detection, CTE alias-reuse, token symbols,
  COA roles, bare identifiers.
* `validate_declared_inputs` / `collect_declared_input_warnings` — the gate over
  a fixture pack, plus the shipped-pack completeness proof.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import yaml

from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack import load_pack
from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack_validators import (
    AIDPF_2084_UNDECLARED_INPUT,
    AIDPF_2085_UNQUALIFIED_UPSTREAM_COLUMN,
    collect_declared_input_warnings,
    validate_declared_inputs,
)
from oracle_ai_data_platform_fusion_bundle.orchestrator.sql_references import (
    extract_upstream_reads,
)

# ---------------------------------------------------------------------------
# extract_upstream_reads — pure extractor
# ---------------------------------------------------------------------------


def test_qualified_read_attributed():
    sql = "SELECT a.Col1, a.Col2 FROM {{ catalog }}.{{ bronze_schema }}.src a"
    r = extract_upstream_reads(sql, depends_on_ids={"src"})
    assert r.demands == {"src": {"Col1", "Col2"}}
    assert not r.wildcard_sources


def test_select_star_from_upstream_is_wildcard():
    sql = "SELECT * FROM {{ catalog }}.{{ bronze_schema }}.src a WHERE a.x IS NOT NULL"
    r = extract_upstream_reads(sql, depends_on_ids={"src"})
    assert r.wildcard_sources == {"src"}


def test_mid_projection_star_is_wildcard():
    # A bare `*` anywhere in the SELECT list (not just first) is a wildcard read,
    # even when other columns are explicitly projected.
    sql = "SELECT s.A, * FROM {{ catalog }}.{{ bronze_schema }}.src s"
    r = extract_upstream_reads(sql, depends_on_ids={"src"})
    assert r.wildcard_sources == {"src"}


def test_count_star_and_multiplication_not_wildcard():
    # COUNT(*) / function args and `a * b` multiplication must NOT trip wildcard.
    for sql in (
        "SELECT s.A, COUNT(*) FROM {{ catalog }}.{{ bronze_schema }}.src s",
        "SELECT s.A * s.B AS p FROM {{ catalog }}.{{ bronze_schema }}.src s",
    ):
        assert extract_upstream_reads(sql, depends_on_ids={"src"}).wildcard_sources == set()


def test_select_star_over_cte_is_not_wildcard():
    # `*` over a derived/CTE block (no direct upstream) must NOT flag.
    sql = (
        "WITH c AS (SELECT a.Col1 FROM {{ catalog }}.{{ bronze_schema }}.src a) "
        "SELECT * FROM c"
    )
    r = extract_upstream_reads(sql, depends_on_ids={"src"})
    assert r.wildcard_sources == set()
    assert r.demands == {"src": {"Col1"}}


def test_cte_alias_reuse_not_misattributed():
    # `inv` = upstream inside the CTE body, = the CTE in the outer query.
    sql = (
        "WITH invoices AS ("
        "  SELECT inv.RealCol FROM {{ catalog }}.{{ bronze_schema }}.ap_invoices inv"
        ") "
        "SELECT inv.DerivedCol FROM invoices inv"
    )
    r = extract_upstream_reads(sql, depends_on_ids={"ap_invoices"})
    # Only the CTE-body read is attributed; the outer inv.DerivedCol (CTE) is not.
    assert r.demands == {"ap_invoices": {"RealCol"}}


def test_column_token_emits_symbol():
    sql = "SELECT a.{{ column.invoice_currency_code }} FROM {{ catalog }}.{{ bronze_schema }}.src a"
    r = extract_upstream_reads(sql, depends_on_ids={"src"})
    assert r.demands == {"src": {"$column.invoice_currency_code"}}


def test_coa_token_collected_as_role():
    sql = "SELECT {{ coa.balancing }} AS company FROM {{ catalog }}.{{ bronze_schema }}.gl_coa coa"
    r = extract_upstream_reads(sql, depends_on_ids={"gl_coa"})
    assert "$coa.balancing" in r.coa_roles


def test_function_wrapped_column_still_seen():
    sql = "SELECT CAST(a.Amt AS DECIMAL(20,2)) FROM {{ catalog }}.{{ bronze_schema }}.src a"
    r = extract_upstream_reads(sql, depends_on_ids={"src"})
    assert r.demands == {"src": {"Amt"}}


def test_bare_identifier_collected_not_demanded():
    # Unaliased single upstream: bare reads are warn candidates, not demands.
    sql = "SELECT BareCol FROM {{ catalog }}.{{ bronze_schema }}.src"
    r = extract_upstream_reads(sql, depends_on_ids={"src"})
    assert "BareCol" in r.bare_identifiers
    assert r.demands == {}  # bare → not a confident demand


# ---------------------------------------------------------------------------
# validate_declared_inputs — gate over a fixture pack
# ---------------------------------------------------------------------------


def _profile(resolved_column=None):
    m = MagicMock()
    m.resolved.column = dict(resolved_column or {})
    m.profile = {}
    return m


def _write(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, sort_keys=False))


def _make_pack(root: Path, *, silver_sql: str, required: dict, column_aliases=None) -> Path:
    """Minimal pack: bronze `src` (outputSchema A,B,C) + a silver SQL node."""
    pr = root / "p"
    _write(pr / "pack.yaml", {
        "id": "p", "version": "0.1.0",
        "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
        "columnAliases": column_aliases or {},
    })
    _write(pr / "bronze.yaml", {"datasets": []})
    _write(pr / "bronze" / "src.yaml", {
        "id": "src", "layer": "bronze",
        "implementation": {"type": "bronze_extract", "datastore": "D.PVO",
                            "pvo_id": "D.PVO", "biccSchema": "Financial",
                            "incrementalCapable": True, "auditColumnsMode": "bronze_v1"},
        "target": "src", "dependsOn": {"bronze": [], "silver": []},
        "refresh": {"seed": {"strategy": "replace"}},
        "requiredColumns": {"src": ["A"]},
        "outputSchema": {"columns": [
            {"name": "A", "type": "string", "nullable": True, "pii": "none"},
            {"name": "B", "type": "string", "nullable": True, "pii": "none"},
            {"name": "C", "type": "string", "nullable": True, "pii": "none"},
        ]},
    })
    _write(pr / "silver" / "dim.yaml", {
        "id": "dim", "layer": "silver",
        "implementation": {"type": "sql", "sql": "silver/dim.sql"},
        "target": "dim", "dependsOn": {"bronze": [{"id": "src"}]},
        "refresh": {"seed": {"strategy": "replace"}},
        "requiredColumns": required,
        "outputSchema": {"columns": [
            {"name": "x", "type": "string", "nullable": True, "pii": "none"}]},
    })
    (pr / "silver" / "dim.sql").write_text(silver_sql)
    return pr


def test_undeclared_read_fails(tmp_path):
    pack = load_pack(_make_pack(
        tmp_path,
        silver_sql="SELECT s.B AS x FROM {{ catalog }}.{{ bronze_schema }}.src s",
        required={"src": ["A"]},  # B not declared
    ))
    errs = validate_declared_inputs(pack)
    assert any(e.code == AIDPF_2084_UNDECLARED_INPUT and "B" in e.message for e in errs)


def test_declared_read_passes(tmp_path):
    pack = load_pack(_make_pack(
        tmp_path,
        silver_sql="SELECT s.B AS x FROM {{ catalog }}.{{ bronze_schema }}.src s",
        required={"src": ["A", "B"]},
    ))
    assert validate_declared_inputs(pack) == []


def test_token_read_profile_none_hole_closed(tmp_path):
    # The reviewer's required test: `<alias>.{{ column.k }}` undeclared → AIDPF-2084
    # even with profile=None (symbol-level match).
    aliases = {"k": {"appliesTo": "bronze.src", "required": True, "candidates": ["B"]}}
    sql = "SELECT s.{{ column.k }} AS x FROM {{ catalog }}.{{ bronze_schema }}.src s"
    pack = load_pack(_make_pack(tmp_path, silver_sql=sql, required={"src": ["A"]}, column_aliases=aliases))
    errs = validate_declared_inputs(pack, profile=None)
    assert any(e.code == AIDPF_2084_UNDECLARED_INPUT and "$column.k" in e.message for e in errs)
    # Declaring the $column symbol satisfies it — still profile-None.
    pack_ok = load_pack(_make_pack(tmp_path / "ok", silver_sql=sql,
                                   required={"src": ["A", "$column.k"]}, column_aliases=aliases))
    assert validate_declared_inputs(pack_ok, profile=None) == []


def test_wildcard_from_upstream_hard_error(tmp_path):
    pack = load_pack(_make_pack(
        tmp_path,
        silver_sql="SELECT * FROM {{ catalog }}.{{ bronze_schema }}.src s",
        required={"src": ["A", "B", "C"]},  # even fully declared, * is unverifiable
    ))
    errs = validate_declared_inputs(pack)
    assert any(e.code == AIDPF_2084_UNDECLARED_INPUT and "*" in e.message for e in errs)


def test_mid_projection_wildcard_fails_even_if_explicit_declared(tmp_path):
    # `SELECT s.A, *` — A is declared, but the trailing `*` reads everything →
    # still a hard AIDPF-2084 (the mid-projection wildcard regression).
    pack = load_pack(_make_pack(
        tmp_path,
        silver_sql="SELECT s.A AS x, * FROM {{ catalog }}.{{ bronze_schema }}.src s",
        required={"src": ["A", "B", "C"]},
    ))
    errs = validate_declared_inputs(pack)
    assert any(e.code == AIDPF_2084_UNDECLARED_INPUT and "*" in e.message for e in errs)


def test_coa_role_must_be_declared_on_the_coa_source(tmp_path):
    # `{{ coa.balancing }}` is read from `src` (its segment cols come from there).
    # Declaring `$coa.balancing` under a NON-dependency / wrong key must NOT
    # satisfy it — it has to be on the referenced COA source.
    sql = "SELECT s.A AS x, {{ coa.balancing }} AS company FROM {{ catalog }}.{{ bronze_schema }}.src s"
    pack_bad = load_pack(_make_pack(
        tmp_path / "bad", silver_sql=sql,
        required={"src": ["A"], "not_src": ["$coa.balancing"]},
    ))
    errs = validate_declared_inputs(pack_bad)
    assert any(e.code == AIDPF_2084_UNDECLARED_INPUT and "coa.balancing" in e.message for e in errs)
    # Declared on the real referenced source → passes.
    pack_ok = load_pack(_make_pack(
        tmp_path / "ok", silver_sql=sql, required={"src": ["A", "$coa.balancing"]},
    ))
    assert validate_declared_inputs(pack_ok) == []


def test_bare_upstream_column_warns_not_errors(tmp_path):
    pack = load_pack(_make_pack(
        tmp_path,
        silver_sql="SELECT B AS x FROM {{ catalog }}.{{ bronze_schema }}.src",
        required={"src": ["A"]},
    ))
    # Bare B matches src.outputSchema → warn, not hard error.
    assert validate_declared_inputs(pack) == []
    warns = collect_declared_input_warnings(pack)
    assert any(w.code == AIDPF_2085_UNQUALIFIED_UPSTREAM_COLUMN and "B" in w.message for w in warns)


# ---------------------------------------------------------------------------
# Completeness proof — the shipped starter pack is fully aligned
# ---------------------------------------------------------------------------


def test_starter_pack_declared_inputs_clean():
    from oracle_ai_data_platform_fusion_bundle.commands.content_pack import (
        _load_full_chain, resolve_pack_path,
    )
    pack = _load_full_chain(resolve_pack_path("fusion-finance-starter"))
    assert validate_declared_inputs(pack) == []
    assert collect_declared_input_warnings(pack) == []

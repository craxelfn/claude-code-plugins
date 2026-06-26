"""Conservative, block-scoped extraction of a silver/gold node's upstream column
references from its (pre-render) SQL — the engine behind the declared-inputs gate
(AIDPF-2084 / AIDPF-2085).

Design (see docs/features/declared-inputs-contract-gate/plan.md):

* **Symbol-level, profile-independent.** Each demand is emitted in the SAME
  vocabulary the author declares ``requiredColumns`` in — a literal physical
  name, ``$column.<key>`` (from a ``{{ column.<key> }}`` token), or ``$coa.<role>``
  (from a ``{{ coa.<role> }}`` token). The gate then matches symbol-to-symbol with
  no profile, so it works on the profile-less run-start validation path.
* **Block-scoped.** SQL is split into nested query blocks (the top-level query,
  each ``WITH <name> AS ( … )`` CTE body, and each parenthesised subquery). Each
  block builds its OWN ``{alias: upstream_id}`` map from its FROM/JOIN clauses, so
  an alias that means an upstream table in one block and a CTE/derived table in
  another is never confused (e.g. ``inv`` in supplier_spend).
* **Conservative.** Only references the extractor can confidently attribute to an
  upstream table — ``<alias>.<Col>`` where ``<alias>`` maps to an upstream in the
  *same* block — become hard demands. A wildcard (``*`` / ``<alias>.*``) over an
  upstream is a provably-unverifiable read → hard violation. Bare unqualified
  identifiers are surfaced separately as warn-only candidates (they may be
  CTE-derived). Anything else is ignored — no false positives.

Out of scope (documented v1 gaps): columns hidden inside ``{{ semantic.<key> }}``
predicates (profile-resolved; backstopped by the live gates), and full SQL
semantics. Upgrade path is sqlglot (plan Risks) — the result dataclass stays the
same.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

# Schema tokens collapse to short sentinel identifiers so a FROM/JOIN target like
# ``{{ catalog }}.{{ bronze_schema }}.gl_coa`` becomes ``cat.br.gl_coa`` and the
# trailing segment (the node id) is recoverable by the FROM scanner.
_SCHEMA_TOKEN_SENTINEL = {
    "catalog": "cat",
    "bronze_schema": "br",
    "silver_schema": "sv",
    "gold_schema": "gd",
}
_SCHEMA_SENTINELS = set(_SCHEMA_TOKEN_SENTINEL.values())

_COLUMN_TOKEN_RE = re.compile(r"\{\{\s*column\.([A-Za-z0-9_]+)\s*\}\}")
_COA_TOKEN_RE = re.compile(r"\{\{\s*coa\.([A-Za-z0-9_]+)\s*\}\}")
_SCHEMA_TOKEN_RE = re.compile(r"\{\{\s*(catalog|bronze_schema|silver_schema|gold_schema)\s*\}\}")
_ANY_TOKEN_RE = re.compile(r"\{\{[^}]*\}\}")

# A ``{{ column.<key> }}`` token is rewritten to this sentinel identifier so it
# survives as a normal ``<alias>.<ident>`` / bare ``<ident>`` reference; the
# extractor maps the sentinel back to the ``$column.<key>`` symbol.
_COL_SENTINEL_PREFIX = "coltok_x_"

_SQL_KEYWORDS = {
    "on", "where", "group", "order", "by", "left", "right", "inner", "outer",
    "full", "cross", "join", "using", "having", "qualify", "window", "union",
    "all", "select", "from", "as", "and", "or", "where", "lateral", "limit",
    "distinct", "partition", "over", "when", "then", "else", "end", "case",
}

# Identifier (table or column). Allows the schema-sentinel dotted form.
_IDENT = r"[A-Za-z_][A-Za-z0-9_]*"


@dataclass
class UpstreamReads:
    """Result of extracting one node's SQL.

    * ``demands`` — confidently-attributed reads, keyed by upstream source id,
      each a set of *symbols* (literal column name, ``$column.<key>``, or
      ``$coa.<role>``). These are gated as hard AIDPF-2084 demands.
    * ``coa_roles`` — ``$coa.<role>`` symbols read via a standalone
      ``{{ coa.<role> }}`` token (not alias-qualified); the gate attributes them
      to whichever upstream declares that role.
    * ``wildcard_sources`` — upstream ids read via ``*`` / ``<alias>.*`` (hard
      AIDPF-2084: unverifiable).
    * ``bare_identifiers`` — unqualified identifiers seen in a block that has an
      upstream source (warn-only AIDPF-2085 candidates; the validator filters
      them against the upstream ``outputSchema``).
    """

    demands: dict[str, set[str]] = field(default_factory=dict)
    coa_roles: set[str] = field(default_factory=set)
    wildcard_sources: set[str] = field(default_factory=set)
    bare_identifiers: set[str] = field(default_factory=set)

    def _add(self, source_id: str, symbol: str) -> None:
        self.demands.setdefault(source_id, set()).add(symbol)


# ---------------------------------------------------------------------------
# Token neutralization (Step 1)
# ---------------------------------------------------------------------------


def _neutralize_tokens(sql: str) -> tuple[str, dict[str, str]]:
    """Replace ``{{ … }}`` tokens with parseable sentinels, preserving demand.

    Returns ``(neutralized_sql, col_symbol_by_sentinel)`` where the map turns a
    column-token sentinel back into its ``$column.<key>`` symbol. ``{{ coa.* }}``
    tokens are handled separately (they carry no alias), and inert tokens
    (watermark/run_id/snapshot/semantic) become harmless literals.
    """
    col_symbol: dict[str, str] = {}

    def _col(m: "re.Match[str]") -> str:
        key = m.group(1)
        sentinel = f"{_COL_SENTINEL_PREFIX}{key}"
        col_symbol[sentinel.lower()] = f"$column.{key}"
        return sentinel

    sql = _COLUMN_TOKEN_RE.sub(_col, sql)
    # COA tokens → a neutral literal here; roles are collected separately.
    sql = _COA_TOKEN_RE.sub("COA_ROLE_PLACEHOLDER", sql)
    # Schema tokens → sentinel idents so FROM targets are dotted, parseable.
    sql = _SCHEMA_TOKEN_RE.sub(lambda m: _SCHEMA_TOKEN_SENTINEL[m.group(1)], sql)
    # Everything else ({{ watermark_predicate }}, {{ run_id_literal }},
    # {{ snapshot_date }}, {{ semantic.* }}) → an inert literal.
    sql = _ANY_TOKEN_RE.sub("NULL", sql)
    return sql, col_symbol


# ---------------------------------------------------------------------------
# Block splitting (Step 2)
# ---------------------------------------------------------------------------


_SELECT_RE = re.compile(r"\bSELECT\b", re.IGNORECASE)


def _paren_spans(sql: str) -> list[tuple[int, int]]:
    """All balanced ``(start, end)`` paren spans (inclusive indices)."""
    spans: list[tuple[int, int]] = []
    stack: list[int] = []
    for i, ch in enumerate(sql):
        if ch == "(":
            stack.append(i)
        elif ch == ")" and stack:
            spans.append((stack.pop(), i))
    return spans


def _split_blocks(sql: str) -> list[str]:
    """Return each query block's own-level text.

    A **subquery** paren group (one whose inner text contains ``SELECT`` — a CTE
    body or a derived table) starts a new block; a **function-call** paren group
    (``CAST(…)``, ``OVER(…)``, ``COALESCE(…)`` — no ``SELECT``) does NOT and is
    kept inline so the column refs inside it stay visible. For each block we blank
    only its **direct child subqueries** (not function parens), so:

    * a reused alias (``b``/``inv`` defined inside a CTE) can't leak into the
      parent block, AND
    * ``CAST(b.BalanceDr AS …)`` column refs at the block's own level survive.

    Blocks = the whole SQL (top-level) + each subquery group's inner text.
    """
    spans = _paren_spans(sql)
    subqueries = [sp for sp in spans if _SELECT_RE.search(sql[sp[0] + 1 : sp[1]])]
    ranges = [(0, len(sql))] + [(st + 1, en) for (st, en) in subqueries]

    def _scannable(lo: int, hi: int) -> str:
        # Direct child subqueries of this block = subquery spans inside (lo,hi)
        # not nested within another subquery that is itself inside (lo,hi).
        inside = [sp for sp in subqueries if lo <= sp[0] and sp[1] < hi]
        direct = [
            sp
            for sp in inside
            if not any(o != sp and o[0] <= sp[0] and sp[1] <= o[1] for o in inside)
        ]
        chars = list(sql[lo:hi])
        for st, en in direct:
            for k in range(st, en + 1):
                chars[k - lo] = " "
        return "".join(chars)

    return [_scannable(lo, hi) for lo, hi in ranges]


# ---------------------------------------------------------------------------
# Per-block source + reference scanning (Steps 2–3, 3b)
# ---------------------------------------------------------------------------

# A schema-qualified upstream source in FROM/JOIN: ``<schemaparts>.<id> [AS] <alias>``
# where the part before the id is one/two schema sentinels. The alias is optional.
_SOURCE_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+"
    r"(?:(?P<s1>" + _IDENT + r")\.)?"
    r"(?:(?P<s2>" + _IDENT + r")\.)?"
    r"(?P<id>" + _IDENT + r")"
    r"(?:\s+(?:AS\s+)?(?P<alias>" + _IDENT + r"))?",
    re.IGNORECASE,
)


def _block_upstream_aliases(block: str, depends_on_ids: set[str]) -> dict[str, str]:
    """``{alias_or_id: source_id}`` for upstream tables in THIS block's FROM/JOIN.

    A source counts as upstream only if its trailing id is in ``depends_on_ids``
    AND it is schema-qualified (preceded by a schema sentinel) — so a bare
    ``FROM some_cte`` (CTE/derived) is never treated as an upstream. The key is
    the SQL alias when present, else the id itself (covers an unaliased upstream).
    """
    out: dict[str, str] = {}
    for m in _SOURCE_RE.finditer(block):
        sid = m.group("id")
        schema_qualified = (m.group("s1") in _SCHEMA_SENTINELS) or (
            m.group("s2") in _SCHEMA_SENTINELS
        )
        if not schema_qualified or sid not in depends_on_ids:
            continue
        alias = m.group("alias")
        if alias and alias.lower() not in _SQL_KEYWORDS:
            out[alias] = sid
        else:
            out[sid] = sid  # unaliased upstream — reference by bare id/column
    return out


_QUALIFIED_REF_RE = re.compile(r"\b(?P<alias>" + _IDENT + r")\.(?P<col>" + _IDENT + r"|\*)")
# A bare `*` projection ITEM (delimited by SELECT / `,` / end) anywhere in the
# SELECT list — e.g. `SELECT *`, `SELECT a, *`, `SELECT *, b`. Bounded so it does
# not match multiplication (`a * b`) or a qualified `t.*` (handled separately).
_BARE_STAR_ITEM_RE = re.compile(r"(?:^|,)\s*\*\s*(?:,|$)")
_PAREN_GROUP_RE = re.compile(r"\([^()]*\)")


def _has_bare_star_projection(block: str) -> bool:
    """True if the block's SELECT list contains a bare ``*`` item.

    Isolates the projection (between this block's ``SELECT`` and its ``FROM``),
    blanks parenthesised groups so ``COUNT(*)`` / function args don't count, and
    looks for a ``*`` that stands as its own select-list item. A qualified
    ``<alias>.*`` is NOT matched here (the qualified-ref scan handles it).
    """
    m = re.search(r"\bSELECT\b(?P<proj>.*?)\bFROM\b", block, re.IGNORECASE | re.DOTALL)
    proj = m.group("proj") if m else ""
    if not proj:
        return False
    prev = None
    while prev != proj:  # strip nested parens (function args, COUNT(*))
        prev = proj
        proj = _PAREN_GROUP_RE.sub(" ", proj)
    return _BARE_STAR_ITEM_RE.search(proj) is not None


def extract_upstream_reads(sql: str, *, depends_on_ids: set[str]) -> UpstreamReads:
    """Extract a node's upstream column demands from its pre-render SQL.

    ``depends_on_ids`` is the set of the node's declared upstream ids
    (``dependsOn.bronze`` + ``dependsOn.silver``); only sources in this set are
    considered upstream.
    """
    result = UpstreamReads()
    neutral, col_symbol = _neutralize_tokens(sql)

    # COA roles are global to the SQL (standalone tokens, no alias).
    result.coa_roles = {f"$coa.{role}" for role in _COA_TOKEN_RE.findall(sql)}

    for block in _split_blocks(neutral):
        aliases = _block_upstream_aliases(block, depends_on_ids)
        if not aliases:
            continue
        # Map a bare-id "alias" (unaliased upstream) so qualified refs to it work,
        # and remember the set of upstream aliases for wildcard / bare handling.
        upstream_alias_set = set(aliases)

        # Qualified references <alias>.<col> / <alias>.*
        for m in _QUALIFIED_REF_RE.finditer(block):
            alias, col = m.group("alias"), m.group("col")
            sid = aliases.get(alias)
            if sid is None:
                continue  # alias not an upstream in this block → ignore
            if col == "*":
                result.wildcard_sources.add(sid)
                continue
            sym = col_symbol.get(col.lower(), col)  # sentinel → $column.x, else literal
            result._add(sid, sym)

        # A bare ``*`` projection item (anywhere in the SELECT list, not just
        # first) in a block with an upstream source → wildcard read of every
        # upstream in the block (can't prove which columns). Catches
        # `SELECT *`, `SELECT a, *`, `SELECT *, b` alike.
        if _has_bare_star_projection(block):
            result.wildcard_sources.update(aliases.values())

        # Bare identifiers (no qualifier) for the warn-only check. Collect simple
        # identifiers that are not keywords, not schema sentinels, not aliases,
        # and not column-token sentinels (those are real $column demands handled
        # below). The validator filters these against the upstream outputSchema.
        # Only meaningful when the block has exactly one upstream (else ambiguous).
        if len(set(aliases.values())) == 1:
            (only_sid,) = set(aliases.values())
            # Residual = block with qualified refs (<alias>.<col>) and AS-targets
            # removed, so only *truly unqualified* column-position identifiers
            # remain (a qualified ref's column part and an output alias are NOT
            # bare reads).
            residual = _QUALIFIED_REF_RE.sub(" ", block)
            residual = re.sub(r"\bAS\s+" + _IDENT, " ", residual, flags=re.IGNORECASE)
            for tok in re.findall(r"\b" + _IDENT + r"\b", residual):
                low = tok.lower()
                if (
                    low in _SQL_KEYWORDS
                    or tok in _SCHEMA_SENTINELS
                    or tok in upstream_alias_set
                    or tok == only_sid
                    or low.startswith(_COL_SENTINEL_PREFIX)
                ):
                    continue
                result.bare_identifiers.add(tok)
            # A bare column-token read in a single-upstream block is a real
            # $column demand on that upstream.
            for sentinel, sym in col_symbol.items():
                if re.search(r"\b" + re.escape(sentinel) + r"\b", block, re.IGNORECASE):
                    result._add(only_sid, sym)

    return result


__all__ = ["UpstreamReads", "extract_upstream_reads"]

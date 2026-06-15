#!/usr/bin/env python3
"""Change-strategy planner for the ``mart-author`` skill.

Makes the **safety-critical** authoring decision executable and testable rather
than leaving it to prose: given where each field the business logic needs is
sourced (existing materialized layer / Fusion PVO source only / nowhere), pick
the **lowest-cost, additive, non-destructive** change on the ladder and emit a
node spec with the medallion invariants pre-stamped.

Hard rules this enforces (the "don't touch living delta" contract):
  * NEVER alter an existing node's grain/natural-key or rewrite an existing
    bronze/silver table (they may be terabytes). New bronze is ADDITIVE only.
  * Reading existing tables is fine; reprocessing them is not — so a new gold
    node over existing silver/bronze is preferred over a new extract.
  * A field that exists nowhere (not even at the PVO source) is a HARD GAP —
    do not fabricate a way to serve it.

The change-strategy ladder (cheapest first):
  rung_3_add_column           — derive from columns already in ONE existing
                                table's sources; additive column (no grain change)
  rung_1_new_gold             — new aggregate/business mart over EXISTING bronze/silver
  rung_2_new_silver           — new conformed/typed node over EXISTING bronze
  rung_4_new_bronze_plus_node — a needed raw field isn't extracted yet → new
                                bronze_extract (additive) + downstream node

Input JSON (stdin or --input):

    {
      "request": {
        "id": "supplier_payment_efficiency",
        "targetLayer": "gold",                 # gold | silver
        "grain": ["supplier_number","currency_code"],
        "addToExisting": null,                 # or "gold.supplier_spend" to request rung 3
        "isAggregate": true,                   # aggregate marts need currency-in-grain
        "columns": [                           # business output columns (excl. audit)
          {"name":"supplier_name","pii":"medium"},
          {"name":"currency_code","pii":"none"},
          {"name":"on_time_pct","pii":"none"}
        ]
      },
      "fields": [                              # where each REQUIRED source field lives
        {"name":"supplier_number","source":"existing_silver","table":"silver.dim_supplier"},
        {"name":"total_paid","source":"existing_gold","table":"gold.supplier_spend"},
        {"name":"promised_date","source":"pvo_only","pvo":"InvoiceHeaderExtractPVO","sourceColumn":"ApInvoicesPromisedDate"},
        {"name":"currency_code","source":"existing_bronze","table":"bronze.ap_invoices"}
      ]
    }

Output JSON: {decision, reason, blastRadius, requiresNewBronze, missingFields,
              warnings, touchesLivingDelta, nodeSpecs:[...]}.
"""
from __future__ import annotations

import argparse
import json
import sys

# Audit columns every authored node must carry (SOX trail). Stamped onto the
# node spec automatically so the SQL scaffolder can't forget them.
_AUDIT_BY_LAYER = {
    "bronze": ["_extract_ts", "_source_pvo", "_run_id", "_watermark_used"],
    "silver": ["bronze_extract_ts", "bronze_source_pvo", "silver_built_at", "silver_run_id"],
    "gold": ["gold_built_at", "gold_run_id"],
}

# A currency column must appear in the grain of any amount aggregate (no
# single-currency-summed marts on a multi-currency tenant).
_CURRENCY_HINTS = ("currency_code", "currency")

_VALID_SOURCES = {"existing_gold", "existing_silver", "existing_bronze", "pvo_only", "missing"}


def _refresh_for(layer: str, grain: list, is_aggregate: bool) -> dict:
    """Default refresh strategy per the medallion taxonomy, with a reason."""
    if layer == "gold" and is_aggregate:
        return {
            "seed": {"strategy": "replace"},
            "incremental": {"strategy": "replace"},
            "reason": "Aggregate grain — partial MERGE leaves stale rows on status/key flips; "
                      "replace each cycle.",
        }
    # Row-grain node (silver dim / row-grain gold): merge on the natural key.
    return {
        "seed": {"strategy": "replace"},
        "incremental": {
            "strategy": "merge",
            "naturalKey": list(grain),
            "reason": "Row-grain node — NULL-safe MERGE on the natural key.",
        },
    }


def _node_spec(request: dict, layer: str, depends: dict) -> dict:
    cols = [{"name": c.get("name"), "pii": c.get("pii", "REQUIRED-set-explicitly")}
            for c in (request.get("columns") or [])]
    # Stamp mandatory audit columns (PII none) so they're never omitted.
    for ac in _AUDIT_BY_LAYER.get(layer, []):
        if ac not in [c["name"] for c in cols]:
            cols.append({"name": ac, "pii": "none"})
    return {
        "id": request.get("id"),
        "layer": layer,
        "target": request.get("id"),
        "implementation": "bronze_extract" if layer == "bronze" else "sql",
        "dependsOn": depends,
        "grain": list(request.get("grain") or []),
        "refresh": _refresh_for(layer, request.get("grain") or [], bool(request.get("isAggregate"))),
        "columns": cols,
    }


def plan(payload: dict) -> dict:
    request = payload.get("request") or {}
    fields = payload.get("fields") or []
    warnings: list[str] = []

    for f in fields:
        if f.get("source") not in _VALID_SOURCES:
            raise SystemExit(f"field {f.get('name')!r}: source must be one of {sorted(_VALID_SOURCES)}")

    missing = [f["name"] for f in fields if f.get("source") == "missing"]
    pvo_only = [f for f in fields if f.get("source") == "pvo_only"]
    target_layer = request.get("targetLayer", "gold")
    add_to = request.get("addToExisting")

    # Currency-in-grain invariant for aggregates.
    if request.get("isAggregate"):
        grain = [g.lower() for g in (request.get("grain") or [])]
        if not any(any(h in g for h in _CURRENCY_HINTS) for g in grain):
            warnings.append(
                "aggregate mart grain has no currency column — add currency_code to the "
                "grain (currency-in-grain invariant) before authoring."
            )

    # 1. Hard gap: a needed field exists nowhere (not even at source).
    if missing:
        return {
            "decision": "hard_gap",
            "reason": "field(s) not available in any existing table NOR at the Fusion PVO source",
            "blastRadius": "none — cannot serve this request as specified",
            "requiresNewBronze": False,
            "missingFields": sorted(missing),
            "warnings": warnings,
            "touchesLivingDelta": False,
            "nodeSpecs": [],
        }

    # Build dependsOn from where existing fields come from.
    depends: dict[str, list] = {}
    for f in fields:
        src = f.get("source")
        layer = {"existing_gold": "gold", "existing_silver": "silver",
                 "existing_bronze": "bronze"}.get(src)
        if layer:
            dep_id = (f.get("table") or "").split(".")[-1] or f.get("name")
            depends.setdefault(layer, [])
            if dep_id not in [d.get("id") for d in depends[layer]]:
                depends[layer].append({"id": dep_id, "role": "lookup"})

    # 2. Rung 4: a raw field is only at the PVO -> new ADDITIVE bronze extract + node.
    if pvo_only:
        bronze_ids = sorted({f.get("pvo") or f["name"] for f in pvo_only})
        bronze_specs = []
        for f in pvo_only:
            bronze_specs.append({
                "id": f["name"],
                "layer": "bronze",
                "target": f["name"],
                "implementation": "bronze_extract",
                "pvo": f.get("pvo"),
                "sourceColumn": f.get("sourceColumn"),
                "note": "NEW additive bronze extract — never alters an existing bronze table.",
            })
        depends.setdefault("bronze", [])
        for f in pvo_only:
            depends["bronze"].append({"id": f["name"], "role": "primary"})
        downstream = _node_spec(request, target_layer, depends)
        return {
            "decision": "rung_4_new_bronze_plus_node",
            "reason": f"raw field(s) not yet extracted; add additive bronze extract(s) for {bronze_ids} "
                      f"then a {target_layer} node over them + existing tables",
            "blastRadius": "one new bronze extract per missing field + one new downstream node; "
                           "existing tables untouched",
            "requiresNewBronze": True,
            "missingFields": [],
            "warnings": warnings,
            "touchesLivingDelta": False,
            "nodeSpecs": [*bronze_specs, downstream],
        }

    # 3. Rung 3: additive column on an existing single table.
    if add_to:
        return {
            "decision": "rung_3_add_column",
            "reason": f"all source fields already feed {add_to}; add an additive output column "
                      "(no grain/key change)",
            "blastRadius": f"additive column on {add_to} — same grain, no reprocessing of other tables",
            "requiresNewBronze": False,
            "missingFields": [],
            "warnings": [
                *warnings,
                f"verify the new column's grain matches {add_to}'s existing grain; if it "
                "would change the grain, author a new node instead (do not alter the existing one).",
            ],
            "touchesLivingDelta": False,
            "nodeSpecs": [{
                "addColumnTo": add_to,
                "columns": list(request.get("columns") or []),
                "note": "ADDITIVE only — extend outputSchema + SELECT; do not change grain or keys.",
            }],
        }

    # 1/2. New node over existing materialized data (cheapest standalone build).
    decision = "rung_2_new_silver" if target_layer == "silver" else "rung_1_new_gold"
    return {
        "decision": decision,
        "reason": f"all source fields already materialized; build a new {target_layer} node "
                  "over existing bronze/silver (read-only on existing tables)",
        "blastRadius": "one new table; existing bronze/silver read but never rewritten",
        "requiresNewBronze": False,
        "missingFields": [],
        "warnings": warnings,
        "touchesLivingDelta": False,
        "nodeSpecs": [_node_spec(request, target_layer, depends)],
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Pick the lowest-cost additive mart change + emit node spec.")
    ap.add_argument("--input", default="-", help="Path to request JSON, or '-' for stdin.")
    ns = ap.parse_args(argv)
    text = sys.stdin.read() if ns.input == "-" else open(ns.input, encoding="utf-8").read()
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid request JSON: {exc}") from exc
    print(json.dumps(plan(payload), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

# AIDPF Error Codes

This file is the operator-facing reference for `AIDPF-*` codes emitted by the
Fusion bundle plugin. Use it when a CLI command, bootstrap run, seed run,
incremental run, content-pack validation, dashboard validation, or diagnostic
artifact reports an `AIDPF` code.

For conversational recovery, start with `/aidpf-error-triage`; it extracts the
code and diagnostic context, then routes to the right recovery skill or command.

Most structured diagnostics are written under:

```text
.aidp/diagnostics/<run_id>/
```

Some codes are historical, removed, or test-only. They are still listed here so
older reports, ADRs, and tests can be interpreted without guessing.

## Status Values

| Status | Meaning |
|---|---|
| Active | Can be emitted by current runtime, CLI, validation, renderer, or authoring flows. |
| Warn-only | Validation warning; the command can continue unless another error blocks it. |
| Removed | Used by an older phase but no longer emitted in the current flow. |
| Retired | Reserved from a deleted implementation path. |
| Historical | Mentioned in design documents, but not emitted by current code. |
| Test-only | Used only to prove invalid/unknown code handling. |

## Codes

| Code | Area | Status | Meaning | Usual action |
|---|---|---|---|---|
| `AIDPF-1001` | Bundle config | Historical | Planned code for `bundle.yaml` schema version newer than the engine. | Upgrade the plugin/engine or downgrade/migrate the bundle schema. |
| `AIDPF-1010` | Bundle config | Historical | Planned code for an unresolved required environment variable. | Set the env var or replace it with a supported secret reference. |
| `AIDPF-1020` | Bootstrap / diagnostics | Active | Operator identity cannot be resolved. | Set `--operator`, `AIDP_OPERATOR`, or run from a shell where `USER` is set. |
| `AIDPF-1030` | Bundle config | Active | `contentPack.profile` is missing. | Add the profile name under `contentPack.profile` in `bundle.yaml`. |
| `AIDPF-1031` | Bundle config | Active | `bundle.yaml` has no `contentPack` block. | Add a `contentPack` block; Phase 9 uses the content-pack path only. |
| `AIDPF-1032` | Resume | Removed | Resume was not supported under the content-pack backend in older phases. | Use current `--resume`; this code should not appear in current runs. |
| `AIDPF-1033` | Tenant profile | Active | Profile YAML was not found at the resolved path. | Create or point to the correct `profiles/<profile>.yaml`. |
| `AIDPF-1034` | Plan scope | Active | `--datasets` references a node id that is not in the resolved pack. | Fix the dataset/node id or validate the selected content pack. |
| `AIDPF-1036` | Pack validation | Active | Aggregate run-start content-pack validation failure. | Inspect the per-error report for specific `AIDPF-20xx`, `AIDPF-50xx`, `AIDPF-70xx`, or `AIDPF-80xx` codes. |
| `AIDPF-1037` | Pack resolution | Active | Installed content pack name was not found. | Check `contentPack.name` or install/use the intended pack. |
| `AIDPF-1038` | Pack resolution | Active | Resolved pack root exists but has no `pack.yaml`. | Fix the pack path or restore `pack.yaml`. |
| `AIDPF-1039` | Pack staging | Active | SQL path escapes the pack root; traversal was rejected. | Keep SQL paths inside the pack layer. |
| `AIDPF-1040` | Pack staging | Active | Staging provenance root is not present in `chain_roots`. | Treat as an internal staging/overlay consistency issue; validate the overlay chain. |
| `AIDPF-1041` | Pack execution | Historical | Rejected design option for fail-closed bronze precondition. | No current action; current runs use `AIDPF-2071` for bronze readiness. |
| `AIDPF-1042` | Plan scope | Active | `--strict-scope` found a transitive dependency outside the effective roots. | Add the dependency to the selected scope or disable strict scope for exploratory runs. |
| `AIDPF-1043` | Plan scope | Active | `--datasets` includes an id outside the bundle scope. | Add the dataset to the bundle scope or remove it from the CLI filter. |
| `AIDPF-1045` | Plan scope | Active | `--layers` removed every declared root, leaving an empty plan. | Choose layers that include at least one selected root. |
| `AIDPF-1050` | Tenant profile | Active | Tenant profile YAML schema validation failed. | Fix the profile shape and field types. |
| `AIDPF-1051` | Tenant profile | Active | Tenant profile `schemaVersion` is unsupported. | Use a supported profile schema version. |
| `AIDPF-2000` | Pack loading | Active | Generic pack load/schema validation failure. | Read the attached message; fix malformed YAML, missing base packs, or schema violations. |
| `AIDPF-2001` | Overlay merge | Active | Orphan overlay override or overlay cycle. | Override only existing base nodes/fields and remove cycles in `extends`. |
| `AIDPF-2002` | Pack schema | Active | Pack version is not valid SemVer. | Use a SemVer value such as `0.1.0`. |
| `AIDPF-2003` | Pack validation | Active | SQL file declared by a SQL node is missing. | Restore the SQL file or fix the node path. |
| `AIDPF-2004` | Overlay merge | Active | Overlay `extends` version does not match the base pack version contract. | Align the overlay's `extends` version with the base pack. |
| `AIDPF-2010` | Variation points | Active | Required `columnAliases` variation point is unresolved. | Run bootstrap or add the resolved column alias in the tenant profile. |
| `AIDPF-2011` | Variation points | Active | Required `semanticVariants` variation point is unresolved. | Run bootstrap or add the resolved semantic variant in the tenant profile. |
| `AIDPF-2012` | Bootstrap / drift | Active | Bronze schema fingerprint diverged from the pinned profile. | Run `bootstrap --refresh` after verifying the live Fusion/AIDP schema change. |
| `AIDPF-2020` | Node strategy | Active | Merge strategy is missing a natural key. | Add `naturalKey` to the node strategy. |
| `AIDPF-2030` | Node schema | Active | Output schema column is missing PII classification. | Add `pii` classification to every output column. |
| `AIDPF-2040` | Pack DAG | Active | Content-pack dependency graph has a cycle. | Break the circular dependency. |
| `AIDPF-2041` | Pack DAG | Active | Content-pack node depends on an undeclared node. | Fix the dependency id or declare the missing node. |
| `AIDPF-2042` | Node preflight | Active | Required source column is missing. | Add/fix the source column, alias mapping, or required column declaration. |
| `AIDPF-2043` | Node preflight | Active | Watermark column is missing from the source schema. | Fix `watermarkColumn` or the source table/PVO. |
| `AIDPF-2044` | Node preflight | Active | Partition column is missing from the source schema. | Fix `partitionColumns` or the source table/PVO. |
| `AIDPF-2046` | Node preflight | Active | A `$column.*` required-column reference cannot be resolved. | Define the referenced column alias in the tenant profile/bootstrap output. |
| `AIDPF-2047` | Cluster bootstrap | Active | Cluster bootstrap pre-dispatch gate failed. | Fix the reason shown: missing config, conflicting flags, or failed AIDP REST probe. |
| `AIDPF-2048` | Cluster bootstrap | Active | Cluster bootstrap dispatch failed before a valid marker was returned. | Open `.aidp/diagnostics/<run_id>/AIDPF-2048.json` and fix the dispatch failure. |
| `AIDPF-2049` | Cluster bootstrap | Active | Cluster bootstrap marker was invalid or missing. | Inspect `AIDPF-2049.json` and the companion `cluster_stdout.log`. |
| `AIDPF-2050` | Node strategy | Active | Merge strategy is missing a watermark. | Add the required watermark configuration. |
| `AIDPF-2051` | Node strategy | Active | Merge strategy has zero primary sources. | Mark one source as primary. |
| `AIDPF-2052` | Node strategy | Active | Merge strategy has multiple primary sources. | Keep exactly one primary source. |
| `AIDPF-2053` | Node strategy | Active | Merge with multiple bronze sources lacks source roles or a primary role. | Add source roles and identify the primary source. |
| `AIDPF-2054` | Node strategy | Active | Replace-partition strategy is missing partition columns. | Add `partitionColumns`. |
| `AIDPF-2055` | Node strategy | Active | Replace-partition strategy has multiple primary sources. | Keep exactly one primary source. |
| `AIDPF-2056` | Node strategy | Active | Append/unique strategy is missing a natural key. | Add the natural key. |
| `AIDPF-2057` | Node strategy | Active | `aggregate_merge` is deferred/not supported in this release. | Use a supported strategy or defer this node. |
| `AIDPF-2058` | Node strategy | Active | Snapshot strategy is missing a unique quality test. | Add the required uniqueness quality test. |
| `AIDPF-2059` | Node strategy | Active | SCD2 strategy is missing tracked columns. | Add tracked columns for SCD2 change detection. |
| `AIDPF-2060` | Node strategy | Retired | Retired `python_legacy` deprecated invariant. | No current action; Phase 9 deleted the legacy implementation type. |
| `AIDPF-2061` | Node strategy | Retired | Retired `python_legacy` callable-spec invariant. | No current action; Phase 9 deleted the legacy implementation type. |
| `AIDPF-2071` | Runtime gate | Active | Bronze readiness gate failed for silver/gold execution. | Seed or repair the required bronze tables/columns, then rerun. |
| `AIDPF-2072` | Runtime gate | Active | Live Fusion PVO schema drifted from pack/profile expectations. | Review the diagnostic, refresh bootstrap evidence, or update the pack/profile. |
| `AIDPF-2080` | Pack validation | Warn-only | Bronze extract PVO is not in the curated catalog. | Verify it is an intentional custom PVO; the live drift gate catches real typos. |
| `AIDPF-2081` | Bundle validation | Active | Bundle dataset id does not resolve in any pack layer. | Fix the bundle dataset id or add the node to the pack. |
| `AIDPF-2092` | Bronze runtime | Active | Bronze cursor exists but target table/state is inconsistent. | Repair the bronze target/state alignment before rerunning incremental extraction. |
| `AIDPF-3010` | Source preflight | Historical | Planned code for BICC PVO schema mismatch. | Run a metadata probe and update the pack/profile to match the live PVO. |
| `AIDPF-3020` | Custom extractors | Historical | Planned code for custom extractor load failure or invalid returned schema. | Check the extractor import path, signature, and required audit columns. |
| `AIDPF-4001` | Tenant drift | Historical | Planned code for tenant fingerprint change. | Confirm the tenant change and refresh bootstrap/profile evidence. |
| `AIDPF-4020` | Runtime preflight | Historical | Planned code for dropped target preflight failure. | Reseed or recreate the missing target. |
| `AIDPF-4021` | State init | Active | State-table location holds files but is not a valid Delta table and is unregistered (orphaned, non-adoptable). The valid-Delta case self-heals silently (adopt-in-place). | Inspect that one object-storage prefix; if it is leftover garbage from an aborted run, delete ONLY that prefix and re-run seed (`fusion_bundle_state` is disposable run-audit history, not source data). |
| `AIDPF-4030` | Strategy execution | Active | Strategy is not supported by the current content-pack runner. | Change the node strategy or implement support. |
| `AIDPF-4031` | Strategy execution | Active | Target identifier failed the allowlist. | Use a valid three-part target identifier. |
| `AIDPF-4040` | Resume / incremental | Active | Plan-hash drift detected on resume or incremental continuity check. | Confirm the plan change; rerun seed or use the documented repin path only when intentional. |
| `AIDPF-4050` | Runtime locking | Historical | Planned code for cross-run lock held by another active run. | Wait for the holder to finish, or break the lock only after proving the holder is dead. |
| `AIDPF-4060` | State commit | Active | State-row hard commit failed. | Fix the Delta/state-table write failure before retrying. |
| `AIDPF-4061` | State commit | Active | Output watermark regressed. | Investigate source/order changes; do not advance state until monotonicity is restored. |
| `AIDPF-4070` | Runtime schema | Active | Materialized target schema does not match `node.outputSchema`. | Fix SQL casts/aliases or update the declared output schema. |
| `AIDPF-4071` | Runtime schema | Active | Bronze source column required by the pack is missing before ingest. | Fix the live PVO/source column or update the pack/profile. |
| `AIDPF-5001` | SQL renderer | Active | Identifier substitution failed the allowlist. | Fix catalog/schema/table/column identifiers. |
| `AIDPF-5002` | SQL renderer | Active | Unknown template token or variable. | Use a supported renderer token or declare the variable correctly. |
| `AIDPF-5003` | SQL renderer | Active | Variation point is unresolved or undeclared. | Resolve the column/semantic variation point through bootstrap/profile updates. |
| `AIDPF-5010` | SQL renderer | Active | Post-render SQL safety check rejected the SQL. | Remove rejected fragments such as unsafe comments or multiple statements. |
| `AIDPF-5011` | SQL renderer | Active | `{{ profile.<key> }}` resolved to a disallowed value type. | Use scalar profile values supported by the renderer. |
| `AIDPF-5013` | SQL renderer | Active | `profile.snapshotDate` is present but not an ISO-8601 date. | Use `YYYY-MM-DD` or leave the value absent/empty for `CURRENT_DATE()`. |
| `AIDPF-5014` | Builtin dispatch | Active | Builtin node `implementation.callable` is not in the registry. | Use a registered builtin callable id. |
| `AIDPF-6001` | Quality tests | Historical | Planned code for `reconcile_to` quality test failure. | Review source-vs-target aggregation and fix the reconciliation gap. |
| `AIDPF-6020` | Quality tests | Historical | Planned code for custom quality test load failure or invalid return shape. | Check the quality test import path, signature, and result contract. |
| `AIDPF-7001` | Dashboard validation | Active | Dashboard requires an undeclared/missing table or node. | Fix `requires.tables` / `requires.columns` to match pack gold nodes. |
| `AIDPF-7002` | Dashboard delivery | Historical | Planned code for `.bar` content referencing a column not provided by gold. | Re-author the workbook/snapshot against current gold or extend gold to provide the column. |
| `AIDPF-7003` | Dashboard validation | Active | Dashboard requirement type does not match the referenced pack object. | Fix dashboard metadata so table/column requirements match the pack. |
| `AIDPF-7004` | Dashboard validation | Active | Dashboard pack compatibility check failed. | Align `requires.pack.id`, `minVersion`, or `maxVersion` with the active pack. |
| `AIDPF-7005` | Dashboard validation | Active | `security.allowedColumns` contains columns not listed in `requires.columns`. | Make allowed columns a subset of required columns. |
| `AIDPF-8001` | Dashboard security | Historical | Planned code for high-PII column in dashboard validation queries. | Remove the high-PII column or change the dashboard contract. |
| `AIDPF-8002` | Dashboard security | Active | Dashboard exposes `pii: high` columns in requirements or allowed columns. | Remove high-PII columns or redesign the dashboard security model. |
| `AIDPF-8010` | Quality tests | Active | Quality test failed. | Inspect the failed quality rule and correct data or node logic. |
| `AIDPF-8011` | Quality tests | Active | Quality test is deferred or unsupported. | Implement the quality rule or accept the deferred status intentionally. |
| `AIDPF-9999` | Diagnostics | Test-only | Intentionally invalid/unknown code used by tests. | If seen outside tests, treat it as malformed diagnostic data. |

## Related Non-AIDPF Codes

Dispatch-layer codes such as `DISPATCH_*` are not `AIDPF` codes. They describe
transport or notebook-dispatch failures and may be wrapped by `AIDPF-2048` or
`AIDPF-2049` during cluster bootstrap flows.

## Exit Codes

| Exit code | Related code | Meaning |
|---|---|---|
| `14` | `AIDPF-2012` | Reserved schema-drift exit for active bootstrap/profile fingerprint drift. |

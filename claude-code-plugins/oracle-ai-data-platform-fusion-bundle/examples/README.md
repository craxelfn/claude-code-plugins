# Example bundles

Three sample `bundle.yaml` files for different operator scenarios.

| File | Backend | When to use |
|---|---|---|
| `minimal_gl_only.yaml` | v1 legacy-python | Single-pipeline smoke test — `gl_period_balances` only. |
| `full_finance.yaml` | v1 legacy-python | Full v1 finance pipeline (suppliers, AR, AP, PO, SCM). |
| `fusion-finance-starter.yaml` | **v2 content-pack** | Phase 3+: bundle wires `contentPack: { name: fusion-finance-starter, profile: finance-default }`, materialises silver + gold from pack SQL templates. |

The v2 bundle reads its tenant profile from `examples/profiles/finance-default.yaml` (resolution path: `<bundle.yaml.parent>/profiles/<profile>.yaml` per PLAN §9.5.7).

## Running the v2 starter

```
aidp-fusion-bundle run \
  --inline \
  --mode seed \
  --execution-backend content-pack \
  --bundle examples/fusion-finance-starter.yaml
```

The v1 bundles continue to work under their default backend without changes — Phase 3 adds the v2 wiring alongside, not in place of, the v1 fixtures.

# Examples

Current examples focus on the Phase 9 content-pack workflow.

| Example | Purpose |
|---|---|
| [full-finance-starter/](full-finance-starter/) | Default customer bundle scaffold used by `aidp-fusion-bundle init`. Enables the full shipped finance starter scope and includes `bundle.yaml` plus `aidp.config.yaml`. |
| [minimal-bundle/](minimal-bundle/) | Narrow scaffold used by `aidp-fusion-bundle init --template minimal-bundle`. Builds only the `supplier_spend` chain. |
| [overlay-pack/](overlay-pack/) | Additive content-pack overlay that adds a new gold mart without changing the shipped starter pack. See [../docs/mart_overlay_authoring.md](../docs/mart_overlay_authoring.md). |
| [fusion-finance-starter.yaml](fusion-finance-starter.yaml) | Older single-file content-pack bundle example kept for compatibility. Prefer `full-finance-starter/` for new users. |
| [aidp.config.example.yaml](aidp.config.example.yaml) | Older standalone AIDP config example kept for compatibility. Prefer `full-finance-starter/aidp.config.yaml` for new users. |

The old `minimal_gl_only.yaml` and `full_finance.yaml` examples are legacy
fixtures from the pre-Phase-9 runner. They remain in the repository for tests
and reference, but new projects should start from `aidp-fusion-bundle init`.

## Recommended New-User Path

```bash
mkdir my-fusion-lake
cd my-fusion-lake
aidp-fusion-bundle init
aidp-fusion-bundle validate
aidp-fusion-bundle dashboard mcp-setup --connector-js <path-to-oac-mcp-connect.js>
aidp-fusion-bundle bootstrap --check-iam
aidp-fusion-bundle run --mode seed --dry-run
```

For the full setup guide, see [../docs/project_setup.md](../docs/project_setup.md).

# Project Setup

Use this guide when starting from a fresh checkout or preparing a new customer
bundle. After setup is complete, continue with the operator workflow in
[../workflow.md](../workflow.md).

Scaffolded templates live under [../examples/](../examples/). For a first
customer project, `aidp-fusion-bundle init` writes the current
[../examples/minimal-bundle/](../examples/minimal-bundle/) starter.

## What You Are Setting Up

There are two working directories involved:

| Directory | Purpose |
|---|---|
| Plugin checkout | This repository. It provides the CLI, skills, content packs, and workbook tooling. |
| Customer bundle directory | A separate project created with `aidp-fusion-bundle init`; it holds `bundle.yaml`, `aidp.config.yaml`, tenant profiles, evidence, and overlays. |

Do not author customer changes inside the shipped starter pack. New customer
medallion work belongs in overlays under the customer bundle directory.

## Local Prerequisites

| Requirement | Why |
|---|---|
| Python 3.10+ | Required by `pyproject.toml`; tested with Python 3.10, 3.11, and 3.12 classifiers. |
| Node.js 18+ | Required by OAC MCP connector and workbook-authoring tools. |
| Git | Needed for a source checkout. |
| OCI CLI | Needed only for OCI-side checks, Object Storage work, or manual REST-dispatch troubleshooting. |
| Claude Code | Needed for the conversational skill/autopilot experience. |
| OAC MCP connector zip | Downloaded from OAC Profile -> MCP Connect. |

## Install The Plugin CLI

From this repository:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e .
```

For contributor/test work:

```bash
pip install -e '.[test]'
make test
```

Smoke-check the CLI:

```bash
aidp-fusion-bundle --help
aidp-fusion-bundle content-pack list
aidp-fusion-bundle content-pack info fusion-finance-starter
```

## External Prerequisites

Before a real bootstrap or seed, confirm these exist:

| System | Required setup |
|---|---|
| Fusion BICC | Fusion user with BICC privileges. |
| Fusion BICC | External Storage profile configured in the BICC console. |
| AIDP | AI Data Platform OCID, workspace, and cluster. |
| AIDP | Credential-store entry for the Fusion BICC password, usually loaded as `FUSION_BICC_PASSWORD`. |
| OAC | OAC instance where the AIDP connection, dataset, and workbook will live. |
| OAC | User for operator MCP setup; use least privilege, especially because OAC MCP v1.4 exposes write/delete/ACL tools. |

For REST job dispatch details, including AIDP credential-store setup, see
[rest_dispatch_setup.md](rest_dispatch_setup.md).

## Create A Customer Bundle

Create the customer project outside the plugin checkout. From an empty customer
project directory, run the current Phase 9 scaffold:

```bash
mkdir my-fusion-lake
cd my-fusion-lake
aidp-fusion-bundle init
```

The default scaffold uses the Phase 9 content-pack shape:

```yaml
contentPack:
  name: fusion-finance-starter
  profile: finance-default
```

Resolve AIDP workspace and cluster coordinates:

```bash
aidp-fusion-bundle init-config \
  --aidp-id <aidp-ocid> \
  --workspace "<workspace-name>" \
  --cluster "<cluster-name>"
```

Then validate:

```bash
aidp-fusion-bundle validate
```

## Configure Operator OAC MCP

Set up OAC MCP before the OAC phases of autopilot:

```bash
aidp-fusion-bundle dashboard mcp-setup \
  --connector-js <path-to-oac-mcp-connect.js>
```

Then restart or reconnect Claude Code and verify `oac-mcp-server` is connected.
Do not treat a disconnected MCP server as proof that an OAC dataset or workbook
does not exist.

Full setup and troubleshooting details are in
[oac_mcp_setup.md](oac_mcp_setup.md).

## Bootstrap

Run bootstrap after the bundle config and OAC MCP setup are ready:

```bash
aidp-fusion-bundle bootstrap --check-iam
```

Bootstrap probes prerequisites and pins tenant variation into:

```text
profiles/<contentPack.profile>.yaml
```

If bootstrap reports an `AIDPF-*` code, use
[aidpf-error-codes.md](aidpf-error-codes.md).

## First Seed

Preview first:

```bash
aidp-fusion-bundle run --mode seed --dry-run
```

Then run seed only after confirming the target is safe to populate:

```bash
aidp-fusion-bundle run --mode seed
```

The seed skill is intentionally fail-closed because the current CLI cannot
prove a physical target is empty in every environment.

## OAC Connection And Dataset

After the needed AIDP gold table exists, create the OAC data surface manually.

First, generate the AIDP connection JSON:

```bash
aidp-fusion-bundle dashboard install --target oac \
  --oac-url <oac-url> \
  --print-only \
  ...connection args...
```

Then in OAC:

```text
Data -> Connections -> Create -> Oracle AI Data Platform
```

Upload the generated JSON and private key PEM, then create the dataset over the
advised AIDP gold table(s).

This step is manual for two reasons:

- OAC's public REST validator does not reliably accept first-time AIDP
  `idljdbc` connection creation.
- OAC MCP can search, describe, query, and save catalog content, but it does
  not expose a create-dataset tool.

The full explanation is in
[../workflow.md](../workflow.md#why-oac-connection-and-dataset-are-manual).

## Workbook Authoring

After the OAC dataset exists, resume autopilot or use `workbook-authoring`.
The skill should:

- find the dataset with OAC MCP,
- call `describe_data`,
- bind workbook JSON to the dataset XSA reference,
- save through `save_catalog_content` when the OAC user has that capability.

See [oac_workbook_authoring_e2e.md](oac_workbook_authoring_e2e.md) for the
binding and save mechanics.

## Day-2 Refresh

After a successful seed:

```bash
aidp-fusion-bundle run --mode incremental
```

If a run is interrupted:

```bash
aidp-fusion-bundle status
aidp-fusion-bundle run --mode seed --resume <run_id>
```

Common drift and failure codes are documented in
[aidpf-error-codes.md](aidpf-error-codes.md).

## Setup Checklist

- Plugin CLI installed with `pip install -e .`.
- `aidp-fusion-bundle content-pack list` works.
- Customer bundle created with `aidp-fusion-bundle init`.
- `bundle.yaml` has a `contentPack` block.
- `aidp.config.yaml` resolves the AIDP workspace and cluster.
- Fusion BICC user and External Storage profile exist.
- AIDP credential-store entry for the Fusion BICC password exists.
- OAC MCP connector is staged with `dashboard mcp-setup`.
- Claude Code has been restarted or reconnected and `oac-mcp-server` is live.
- `bootstrap --check-iam` completes.
- Seed dry-run is reviewed before real seed.
- OAC AIDP connection and dataset are created manually.
- Workbook authoring can describe the dataset and save catalog content.

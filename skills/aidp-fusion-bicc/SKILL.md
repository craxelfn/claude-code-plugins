---
description: Trigger a Fusion BICC extract job, wait for completion, then read the resulting gzipped CSV from OCI Object Storage into a Spark DataFrame. Use when the user mentions BICC, Fusion bulk extract, BI Cloud Connector, or needs >50k rows from Fusion. HTTP Basic auth only. The OCI Object Storage read uses cluster-level auth (Spark `oci://`).
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-fusion-bicc` — Fusion BICC bulk extract → Spark

## When to use
- User wants a **bulk** extract from Fusion (millions of rows, daily snapshots, full-table loads).
- User mentions: "BICC", "Fusion bulk extract", "BI Cloud Connector", "PVO extract".
- Live REST paging would be too slow (>50k rows or daily refresh).

## When NOT to use
- For small/live REST queries → use [`aidp-fusion-rest`](../aidp-fusion-rest/SKILL.md).

## Prerequisites in the AIDP notebook
1. `pip install requests` (usually pre-installed on the cluster).
2. Helpers on `sys.path`.
3. Fusion BICC offering credentials (HTTP Basic) for the trigger side. The user must have BICC privileges in Fusion — a regular Fusion REST user (e.g. Finance Manager persona) typically does NOT.
4. OCI Object Storage namespace + bucket where BICC drops the extract.
5. The AIDP Spark cluster's `oci://` HDFS connector must be configured at the cluster level (the `tpcds` cluster has this). The user does NOT need to supply OCI API keys from the notebook — the cluster handles `oci://` reads with its own service auth.

## Auth: HTTP Basic only (both sides)

### Side 1 — Trigger BICC extract

```python
import os
from oracle_ai_data_platform_connectors.auth import http_basic_session
from oracle_ai_data_platform_connectors.rest.fusion import trigger_bicc_extract

session = http_basic_session(
    username=os.environ["FUSION_BICC_USER"],
    password=os.environ["FUSION_BICC_PASSWORD"],
    base_url=os.environ["FUSION_BICC_BASE_URL"],
)
prefix = trigger_bicc_extract(
    session=session,
    base_url=os.environ["FUSION_BICC_BASE_URL"],
    offering=os.environ["FUSION_BICC_OFFERING"],
    poll_interval_seconds=30,
    timeout_seconds=3600,
)
print("BICC extract landed at prefix:", prefix)
```

### Side 2 — Read CSV from OCI Object Storage (Spark `oci://`)

The Spark cluster reads `oci://...` URIs with its pre-configured OCI auth — no user-supplied API key needed in the notebook.

```python
from oracle_ai_data_platform_connectors.rest.fusion import (
    read_bicc_csv_from_object_storage,
)

df = read_bicc_csv_from_object_storage(
    spark=spark,
    namespace=os.environ["OCI_NAMESPACE"],
    bucket=os.environ["OCI_BUCKET_BICC"],
    prefix=prefix,
)
print("rows:", df.count())
df.printSchema()
```

If `oci://` reads fail with auth errors, the cluster-level OCI HDFS connector isn't configured properly. Fix at the cluster level (Cluster → Settings → OCI auth profile), not in the notebook.

## Gotchas
- **BICC privileges** — the Fusion user must have a BICC-enabled role (e.g. `BIA_ADMINISTRATOR_DUTY`). A standard Fusion REST user (Finance Manager) returns HTML auth pages instead of BICC JSON when probing offering endpoints.
- **BICC extract can take minutes to hours.** The helper polls every 30s with a 1h timeout — bump `timeout_seconds` for big offerings.
- **Manifest file** — BICC writes a `MANIFEST.MF` alongside the CSVs listing files + checksums. The helper relies on the `outputPrefix` from the job-status response; if Oracle changes the response shape, update the helper.
- **Schema inference is slow** for big CSVs. Pass an explicit `schema=StructType([...])` for repeat runs.
- **Network** — BICC trigger endpoint is on the public Fusion pod. OCI Object Storage is reached via `oci://` from Spark (uses cluster's OCI config — not user creds).
- **Cleanup** — BICC doesn't auto-delete old extracts. Schedule a separate cleanup job if you don't want the bucket to grow unbounded.

## References
- Helpers: [scripts/oracle_ai_data_platform_connectors/rest/fusion.py](../../scripts/oracle_ai_data_platform_connectors/rest/fusion.py)
- BICC docs: https://docs.oracle.com/en/cloud/saas/applications-common/24a/oafsm/

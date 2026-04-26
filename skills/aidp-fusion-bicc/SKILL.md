---
description: Trigger a Fusion BICC extract job, wait for completion, then read the resulting gzipped CSV from OCI Object Storage into a Spark DataFrame. Use when the user mentions BICC, Fusion bulk extract, BI Cloud Connector, or needs >50k rows from Fusion. Covers HTTP Basic (BICC trigger) + API Key (OCI Object Storage side).
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
1. `pip install requests` + OCI SDK (already on AIDP cluster).
2. Helpers on `sys.path`.
3. Fusion BICC offering credentials (HTTP Basic) for the trigger side.
4. OCI Object Storage namespace + bucket where BICC drops the extract.
5. OCI API key (inline PEM) or Vault-stored credentials for reading from Object Storage.
6. The Spark cluster must already be configured to read `oci://...` paths (`tpcds` cluster has this; if not, add the OCI HDFS connector).

## Auth flow (two-sided)

### Side 1 — Trigger BICC extract (HTTP Basic)

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

(If your AIDP cluster needs explicit OCI credentials for `oci://` reads, configure them at cluster level — using inline PEM via
`from_inline_pem` is not directly compatible with the cluster-level HDFS connector. Set the API key on the cluster's OCI profile.)

## Gotchas
- **BICC extract can take minutes to hours.** The helper polls every 30s with a 1h timeout — bump `timeout_seconds` for big offerings.
- **Manifest file** — BICC writes a `MANIFEST.MF` alongside the CSVs listing files + checksums. The helper relies on the `outputPrefix` from the job-status response; if Oracle changes the response shape, update the helper.
- **Schema inference is slow** for big CSVs. Pass an explicit `schema=StructType([...])` for repeat runs.
- **Network** — BICC trigger endpoint is on the public Fusion pod. OCI Object Storage is reached via `oci://` from Spark (uses cluster's OCI config).
- **Cleanup** — BICC doesn't auto-delete old extracts. Schedule a separate cleanup job if you don't want the bucket to grow unbounded.

## References
- Helpers: [scripts/oracle_ai_data_platform_connectors/rest/fusion.py](../../scripts/oracle_ai_data_platform_connectors/rest/fusion.py)
- BICC docs: https://docs.oracle.com/en/cloud/saas/applications-common/24a/oafsm/

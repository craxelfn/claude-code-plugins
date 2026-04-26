---
description: Read or write a generic Oracle Database (Compute, Base DB, on-prem) from an AIDP notebook via the AIDP `aidataplatform` Spark format handler. Use when the user has a non-Autonomous, non-Exadata Oracle DB — e.g. Oracle on OCI Compute, Oracle Base DB, or on-premise Oracle 19c/21c/23ai. Auth is host/port + database + user/password.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-oracle-db` — Generic Oracle Database via AIDP `aidataplatform`

For Oracle DB instances that are **not** Autonomous (ALH/ADW/ATP) and **not** Exadata Cloud Service. Examples: Oracle on OCI Compute, Oracle Base Database Service, on-premise Oracle 19c / 21c / 23ai.

## When to use
- The target is a self-managed Oracle DB on Compute, Base DB, or on-prem.
- Mentioned: "Oracle DB", "Oracle Compute", "Base DB", "on-prem Oracle", "Oracle 19c / 21c".

## When NOT to use
- For Autonomous (ALH / ADW / ATP) → [`aidp-alh`](../aidp-alh/SKILL.md). Different `type` (`ORACLE_ALH` / `ORACLE_ATP`) and wallet-based auth.
- For Exadata Cloud Service → [`aidp-exacs`](../aidp-exacs/SKILL.md). Different `type` (`ORACLE_EXADATA`) and uses Spark JDBC + NNE rather than `aidataplatform` format.

## Read
```python
import os
from oracle_ai_data_platform_connectors.aidataplatform import (
    AIDP_FORMAT, aidataplatform_options,
)

opts = aidataplatform_options(
    type="ORACLE_DB",
    host=os.environ["ORADB_HOST"],
    port=int(os.environ.get("ORADB_PORT", "1521")),
    database_name=os.environ["ORADB_NAME"],   # service name or PDB name
    user=os.environ["ORADB_USER"],
    password=os.environ["ORADB_PASSWORD"],
    schema=os.environ["ORADB_SCHEMA"],        # Oracle schema = user-owned namespace
    table=os.environ["ORADB_TABLE"],
)
df = spark.read.format(AIDP_FORMAT).options(**opts).load()
df.show(5)
```

## Write
```python
opts = aidataplatform_options(
    type="ORACLE_DB",
    host=os.environ["ORADB_HOST"],
    port=int(os.environ.get("ORADB_PORT", "1521")),
    database_name=os.environ["ORADB_NAME"],
    user=os.environ["ORADB_USER"],
    password=os.environ["ORADB_PASSWORD"],
    schema=os.environ["ORADB_SCHEMA"],
    table=os.environ["ORADB_TARGET_TABLE"],
    extra={"write.mode": "CREATE"},
)
df.write.format(AIDP_FORMAT).options(**opts).save()
```

## Gotchas
- **`database.name` is the SERVICE NAME** (e.g. `ORCLPDB1`, `XEPDB1`), not the SID. The connector internally builds a `jdbc:oracle:thin:@//host:port/service` URL.
- **Network reachability** — the DB must be reachable from the AIDP cluster's VCN. On-prem Oracle requires DRG / VPN / FastConnect; Oracle on Compute requires a route to the Compute subnet.
- **Schema vs user** — for a typical Oracle DB the schema is owned by the connecting user. `schema=ORADB_USER` works in many cases; if the table lives under a different schema, supply that.
- **No wallet path here** — wallet-based mTLS is for Autonomous (`aidp-alh`). On-prem Oracle uses the password flow over a network-level secured channel (or NNE if the server enforces it).
- **For ExaCS specifically**, even though Exadata is technically Oracle DB, use [`aidp-exacs`](../aidp-exacs/SKILL.md) instead — it covers RAC SCAN proxy + AES256 NNE explicitly, which the generic `ORACLE_DB` path doesn't.

## References
- Helper: [scripts/oracle_ai_data_platform_connectors/aidataplatform.py](../../scripts/oracle_ai_data_platform_connectors/aidataplatform.py)
- Official sample: [oracle-samples/oracle-aidp-samples → `data-engineering/ingestion/Read_Write_Oracle_Ecosystem_Connectors.ipynb`](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Read_Write_Oracle_Ecosystem_Connectors.ipynb)

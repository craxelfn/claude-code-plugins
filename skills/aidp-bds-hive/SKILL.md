---
description: Connect from an AIDP notebook to Oracle Big Data Service (BDS) HiveServer2 via Spark JDBC. Use when the user mentions BDS, Big Data Service, Hive, Hadoop, or wants to query a Hive table from Spark in AIDP. Covers LDAP (default) and Kerberos (with prerequisites and live `kinit` test).
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-bds-hive` — Big Data Service Hive via Spark JDBC

## When to use
- User wants to read a Hive table on Oracle Big Data Service from an AIDP notebook.
- User mentions: "BDS", "Big Data Service", "Hive", "Hadoop on OCI", "HiveServer2".

## When NOT to use
- For non-BDS Hive (on-prem, EMR, etc.) — same skill works in principle, but the auth defaults are tuned for BDS LDAP/Kerberos.
- For Oracle DB sources → use [`aidp-alh`](../aidp-alh/SKILL.md), [`aidp-atp`](../aidp-atp/SKILL.md), or [`aidp-exacs`](../aidp-exacs/SKILL.md).

## Prerequisites in the AIDP notebook
1. **Hive JDBC driver is NOT pre-installed** on AIDP clusters. Upload to a Volume and attach via Cluster → Libraries (cluster restart required):
   - `hive-jdbc-<ver>-standalone.jar` (preferred — bundles transitive deps)
   - or `hive-jdbc-<ver>.jar` + `hadoop-common-<ver>.jar` + `hadoop-hdfs-client-<ver>.jar`
2. **Network reachability**: BDS lives in a private subnet. Customer must have set up VCN peering / DRG / NSG rules between AIDP VCN and BDS subnet on port 10000 (or 10001 for SSL). Test:
   ```python
   import socket; s = socket.socket(); s.settimeout(5)
   s.connect((BDS_HS2_HOST, 10000))   # raises if blocked
   ```
3. Helpers on `sys.path`.

## Auth: pick one

### Option A — LDAP (recommended default)

Simpler than Kerberos: no keytab management, no `kinit` binary requirement.

```python
import os
from oracle_ai_data_platform_connectors.jdbc import (
    build_hive_jdbc_url, spark_hive_jdbc_options,
)

url = build_hive_jdbc_url(
    host=os.environ["BDS_HS2_HOST"],
    port=int(os.environ.get("BDS_HS2_PORT", "10000")),
    database=os.environ.get("BDS_HS2_DATABASE", "default"),
    auth="ldap",
)
opts = spark_hive_jdbc_options(
    url=url,
    user=os.environ["BDS_LDAP_USER"],
    password=os.environ["BDS_LDAP_PASSWORD"],
)
df = spark.read.format("jdbc").options(**opts).option("dbtable", "default.my_table").load()
df.show(5)
```

### Option B — Kerberos (security-mandated environments)

**Caveat:** AIDP cluster images may not ship MIT Kerberos client (`krb5-user`). The helper raises `FileNotFoundError` if `kinit` isn't on PATH. Verify before relying on this option (see live-test row 10).

```python
import os
from oracle_ai_data_platform_connectors.jdbc import (
    build_hive_jdbc_url, spark_hive_jdbc_options,
)
from oracle_ai_data_platform_connectors.jdbc.hive import kerberos_kinit

# Keytab MUST live under /tmp (FUSE caveats apply to /Workspace).
# Pull from OCI Vault as base64 and decode locally if needed.
kerberos_kinit(
    principal=os.environ["BDS_KRB_PRINCIPAL"],   # e.g. user@EXAMPLE.COM
    keytab_path=os.environ["BDS_KRB_KEYTAB_PATH"],
)

url = build_hive_jdbc_url(
    host=os.environ["BDS_HS2_HOST"],
    port=10000,
    database=os.environ.get("BDS_HS2_DATABASE", "default"),
    auth="kerberos",
    principal=f"hive/{os.environ['BDS_HS2_HOST']}@{os.environ['BDS_HIVE_REALM']}",
)
opts = spark_hive_jdbc_options(url=url)   # no user/password — TGT covers auth
df = spark.read.format("jdbc").options(**opts).option("dbtable", "default.my_table").load()
```

## Run the query

```python
df = (
    spark.read.format("jdbc").options(**opts)
        .option("dbtable", "default.events")
        .load()
)
```

For Hive-style large reads, partition on a numeric column:

```python
df = (spark.read.format("jdbc").options(**opts)
       .option("dbtable", "(SELECT id, payload FROM events) t")
       .option("partitionColumn", "id")
       .option("lowerBound", "1").option("upperBound", "100000000")
       .option("numPartitions", "16")
       .load())
```

## Gotchas
- **JAR shipping** — `spark.jars` works for ad-hoc but is slow on every job; prefer Cluster Library + restart for durable use.
- **Keytab path** — must be `/tmp/...`, not `/Workspace/...` (FUSE).
- **`kinit` may not exist** on the AIDP cluster image. If `FileNotFoundError("kinit not found on PATH")` fires, fall back to LDAP or ask Oracle support to add `krb5-user` to the cluster image.
- **Hive principal format** — `hive/<host>@<REALM>` is the convention; the realm string is case-sensitive.
- **Port 10000 vs 10001** — 10000 plain, 10001 SSL/HTTP. Pass `ssl=True` to `build_hive_jdbc_url` if you need SSL.

## References
- Helpers: [scripts/oracle_ai_data_platform_connectors/jdbc/hive.py](../../scripts/oracle_ai_data_platform_connectors/jdbc/hive.py)
- AIDP networking constraints: `Claude context/AIDP/AIDP Context/AIDP/aidp_internal_pe_architecture.md`.

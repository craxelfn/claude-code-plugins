---
description: Connect from an AIDP notebook to Oracle Autonomous Database (ATP / ADW) via Spark JDBC. Use when the user mentions ATP, ADW, Autonomous Database, autonomous transaction processing, or wants to query an Autonomous DB from Spark in AIDP. Covers wallet (mTLS) and IAM DB-Token (with executor-side refresh for long jobs).
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-atp` — Oracle Autonomous Database via Spark JDBC

## When to use
- User wants to read or write an ATP / ADW table from an AIDP notebook.
- User mentions: "ATP", "ADW", "Autonomous Database", "autonomous transaction processing".
- User has an ATP wallet ZIP, or wants to use IAM DB-Token authentication.

## When NOT to use
- For Oracle AI Lakehouse → use [`aidp-alh`](../aidp-alh/SKILL.md).
- For ExaCS / on-prem Oracle → use [`aidp-exacs`](../aidp-exacs/SKILL.md).

## Prerequisites in the AIDP notebook
1. Oracle JDBC driver on the cluster classpath (`ojdbc11.jar`).
2. `sys.path.insert(0, "/Workspace/Shared/oracle_ai_data_platform_connectors/scripts")` (or wherever you've uploaded the helpers).
3. ATP wallet ZIP **or** compartment OCID + API key for DB-Token.

## Auth: pick one

### Option A — Wallet (mTLS, recommended default)

```python
import os
from oracle_ai_data_platform_connectors.auth import write_wallet_to_tmp
from oracle_ai_data_platform_connectors.jdbc import (
    build_oracle_jdbc_url, spark_jdbc_options_wallet,
)

tns_admin = write_wallet_to_tmp(
    wallet="/path/to/atp-wallet.zip",
    target_dir="/tmp/wallet/atp",
)

url = build_oracle_jdbc_url(
    tns_alias=os.environ["ATP_TNS_SERVICE"],   # e.g. "atp_high"
    tns_admin=tns_admin,
)

opts = spark_jdbc_options_wallet(
    url=url,
    user=os.environ["ATP_USER"],
    password=os.environ["ATP_PASSWORD"],
)
df = spark.read.format("jdbc").options(**opts).option("dbtable", "MY_TABLE").load()
df.show(5)
```

### Option B — IAM DB-Token (recommended for long-running jobs)

This is the IMFA IoT pattern: 3/3 successful runs over multi-hour jobs with on-executor token refresh.

```python
import os
from oracle_ai_data_platform_connectors.auth import generate_db_token
from oracle_ai_data_platform_connectors.auth.dbtoken import refresh_on_executors
from oracle_ai_data_platform_connectors.jdbc import (
    build_oracle_jdbc_url, spark_jdbc_options_dbtoken,
)

token_dir = generate_db_token(
    compartment_ocid=os.environ["ATP_COMPARTMENT_OCID"],
    target_dir="/tmp/dbcred_atp",
)

url = build_oracle_jdbc_url(
    tns_alias=os.environ["ATP_TNS_SERVICE"],
    tns_admin=os.environ.get("ATP_WALLET_PATH", "/tmp/wallet/atp"),
)
opts = spark_jdbc_options_dbtoken(url=url, token_dir=token_dir)

# Driver-side query (token < 25 min old)
df = spark.read.format("jdbc").options(**opts).option("dbtable", "MY_TABLE").load()

# Executor-side refresh for long mapPartitions jobs:
refresh = refresh_on_executors(spark, os.environ["ATP_COMPARTMENT_OCID"], "/tmp/dbcred_atp")
result = df.rdd.mapPartitions(lambda part: refresh(part)).toDF()
```

### Option C — API Key + inline OCI config (admin / control-plane ops)

For operations that hit OCI control-plane (e.g., starting/stopping the ATP instance, rotating keys), use API Key auth without writing PEM to FUSE:

```python
from oracle_ai_data_platform_connectors.auth import from_inline_pem
config = from_inline_pem(
    user_ocid=os.environ["OCI_USER_OCID"],
    tenancy_ocid=os.environ["OCI_TENANCY_OCID"],
    fingerprint=os.environ["OCI_FINGERPRINT"],
    private_key_pem=os.environ["OCI_PRIVATE_KEY_PEM"],
    region=os.environ["OCI_REGION"],
)
import oci
db_client = oci.database.DatabaseClient(config=config)
```

## Run the query

```python
df = (
    spark.read.format("jdbc").options(**opts)
        .option("dbtable", "(SELECT id, name FROM customers) t")
        .option("partitionColumn", "id")
        .option("lowerBound", "1")
        .option("upperBound", "1000000")
        .option("numPartitions", "8")
        .load()
)
```

## Gotchas
- **Wallet to `/tmp/wallet/atp/`** — `/Workspace` breaks the JDBC driver's wallet reads.
- **`os.chmod` doesn't work on FUSE** — helper uses `os.open(..., 0o666)` to make wallet/token files world-readable up-front.
- **`oracle.jdbc.timezoneAsRegion=false`** — set by helpers; avoids the TZ region warning.
- **DB-Token expires in 60 min;** helper refreshes at 25 min on each executor. For >24h streams, plan to restart the stream after each refresh window.
- **`user`/`password` MUST be unset** in DB-Token mode — the JDBC driver reads the token from `oracle.jdbc.tokenLocation`. Helper omits them automatically.

## References
- Helpers: [scripts/oracle_ai_data_platform_connectors/jdbc/oracle.py](../../scripts/oracle_ai_data_platform_connectors/jdbc/oracle.py)
- DB-Token helper: [scripts/oracle_ai_data_platform_connectors/auth/dbtoken.py](../../scripts/oracle_ai_data_platform_connectors/auth/dbtoken.py)
- IMFA IoT validation case: prior IoT customer-tenancy DB-Token implementation.

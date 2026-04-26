---
description: Connect from an AIDP notebook to Oracle Exadata Cloud Service (ExaCS) via Spark JDBC over TCPS. Use when the user mentions ExaCS, Exadata, Exadata Cloud, or has a private-subnet Oracle DB on port 1522. Covers wallet (mTLS), IAM DB-Token (IAM-enabled clusters), and legacy DB user/password.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-exacs` — Oracle Exadata Cloud Service via Spark JDBC

## When to use
- User wants to read or write an ExaCS PDB from an AIDP notebook.
- User mentions: "ExaCS", "Exadata Cloud", "private-subnet Oracle DB", "TCPS port 1522".

## When NOT to use
- For Autonomous DB → use [`aidp-atp`](../aidp-atp/SKILL.md).
- For Oracle AI Lakehouse → use [`aidp-alh`](../aidp-alh/SKILL.md).

## Prerequisites in the AIDP notebook
1. Oracle JDBC driver on classpath.
2. Helpers on `sys.path`.
3. Network reachability AIDP→ExaCS (private subnet — usually requires VCN peering / DRG / RCE; confirm with `nc -zv <host> 1522`).

## Auth: pick one

### Option A — Wallet (TCPS, recommended default)

```python
import os
from oracle_ai_data_platform_connectors.auth import write_wallet_to_tmp
from oracle_ai_data_platform_connectors.jdbc import (
    build_oracle_jdbc_url, spark_jdbc_options_wallet,
)

tns_admin = write_wallet_to_tmp(
    wallet="/path/to/exacs-wallet.zip",
    target_dir="/tmp/wallet/exacs",
)

# Direct host/port/service form (ExaCS typically doesn't expose _high/_medium aliases)
url = build_oracle_jdbc_url(
    host=os.environ["EXACS_HOST"],
    port=int(os.environ.get("EXACS_PORT", "1522")),
    service_name=os.environ["EXACS_SERVICE_NAME"],
    use_tcps=True,
)
opts = spark_jdbc_options_wallet(
    url=url,
    user=os.environ["EXACS_USER"],
    password=os.environ["EXACS_PASSWORD"],
)
df = spark.read.format("jdbc").options(**opts).option("dbtable", "MY_TABLE").load()
df.show(5)
```

### Option B — IAM DB-Token (only for IAM-enabled ExaCS clusters)

ExaCS supports IAM DB-Token only when explicitly IAM-enabled (`ALTER SYSTEM SET IDENTITY_PROVIDER_TYPE='OCI_IAM'`). Skip this option if your cluster is on classic (non-IAM) auth.

```python
import os
from oracle_ai_data_platform_connectors.auth import generate_db_token
from oracle_ai_data_platform_connectors.jdbc import (
    build_oracle_jdbc_url, spark_jdbc_options_dbtoken,
)

token_dir = generate_db_token(
    compartment_ocid=os.environ["EXACS_COMPARTMENT_OCID"],
    target_dir="/tmp/dbcred_exacs",
)

url = build_oracle_jdbc_url(
    host=os.environ["EXACS_HOST"],
    port=1522,
    service_name=os.environ["EXACS_SERVICE_NAME"],
)
opts = spark_jdbc_options_dbtoken(url=url, token_dir=token_dir)
df = spark.read.format("jdbc").options(**opts).option("dbtable", "MY_TABLE").load()
```

### Option C — Legacy DB user/password

For non-IAM ExaCS clusters where the wallet flow is overkill (e.g., on-prem-style migrations):

```python
import os
from oracle_ai_data_platform_connectors.jdbc import (
    build_oracle_jdbc_url, spark_jdbc_options_password,
)

# Use plain TCP only if the cluster is on a private subnet and you accept no encryption.
# Otherwise keep use_tcps=True and provide the wallet's cwallet.sso under TNS_ADMIN.
url = build_oracle_jdbc_url(
    host=os.environ["EXACS_HOST"],
    port=1521,
    service_name=os.environ["EXACS_SERVICE_NAME"],
    use_tcps=False,
)
opts = spark_jdbc_options_password(
    url=url,
    user=os.environ["EXACS_USER"],
    password=os.environ["EXACS_PASSWORD"],
)
df = spark.read.format("jdbc").options(**opts).option("dbtable", "MY_TABLE").load()
```

## Gotchas
- **Network path matters most.** ExaCS sits in a private subnet; AIDP runs in its own VCN. Confirm a route exists (`nc -zv <host> 1522`) before chasing JDBC errors.
- **TCPS wallet must include `cwallet.sso`** for SSO-style auto-login — that's the file the JDBC driver reads, not `ewallet.p12`.
- **No IMDS access from AIDP** — Instance Principal flows that work elsewhere on OCI compute will fail here. Use API Key + inline PEM.
- **Port 1522 vs 1521** — TCPS is 1522; plain TCP is 1521. Clusters often expose only 1522 by policy.

## References
- Helpers: [scripts/oracle_ai_data_platform_connectors/jdbc/oracle.py](../../scripts/oracle_ai_data_platform_connectors/jdbc/oracle.py)
- AIDP private endpoint design: `Claude context/AIDP/AIDP Context/AIDP/aidp_internal_pe_architecture.md` (peer notes via memory `oci_private_endpoint_design.md`).

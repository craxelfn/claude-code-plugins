---
description: Pull data from Oracle Fusion ERP / HCM / SCM REST APIs into a Spark DataFrame from an AIDP notebook. Use when the user mentions Fusion ERP, Fusion REST API, FA REST, Cloud ERP, or wants live data from a Fusion pod. Covers HTTP Basic (default) and OAuth (v0.2). For volumes >499 rows/page or bulk extracts, route to aidp-fusion-bicc.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-fusion-rest` — Fusion ERP / HCM / SCM REST → Spark

## When to use
- User wants to pull a small-to-medium volume of records from Fusion REST APIs (`/fscmRestApi/`, `/hcmRestApi/`, etc.) into a Spark DataFrame.
- User mentions: "Fusion ERP", "Fusion REST", "FA REST", "Cloud ERP API".
- Total expected rows fit comfortably in memory (≤ ~50k); for >499 rows the helper auto-pages, but for bulk → BICC is faster.

## When NOT to use
- For **bulk extracts** (>50k rows, daily snapshots) → use [`aidp-fusion-bicc`](../aidp-fusion-bicc/SKILL.md). Fusion's REST surface is hard-capped at 499 rows/page (MOS Doc ID 2429019.1) — pulling millions paginated is slow.
- For EPM Cloud Planning → use [`aidp-epm-cloud`](../aidp-epm-cloud/SKILL.md).
- For Essbase MDX → use [`aidp-essbase`](../aidp-essbase/SKILL.md).

## Prerequisites in the AIDP notebook
1. `pip install requests pandas` (usually already on the cluster).
2. Helpers on `sys.path`.
3. Fusion pod URL + Basic credentials, OR OAuth client + private key.

## Auth: pick one

### Option A — HTTP Basic (recommended default, v0.1)

```python
import os
from oracle_ai_data_platform_connectors.auth import http_basic_session
from oracle_ai_data_platform_connectors.rest.fusion import (
    fetch_paged, rows_to_spark_dataframe,
)

session = http_basic_session(
    username=os.environ["FUSION_USER"],
    password=os.environ["FUSION_PASSWORD"],
    base_url=os.environ["FUSION_BASE_URL"],
)

rows = fetch_paged(
    session=session,
    base_url=os.environ["FUSION_BASE_URL"],
    path="/fscmRestApi/resources/11.13.18.05/invoices",
    fields="InvoiceId,InvoiceNumber,InvoiceAmount,InvoiceDate",
    extra_params={"q": "InvoiceDate >= '2026-01-01'"},
)

df = rows_to_spark_dataframe(spark, rows)
df.show(5)
print("rows:", df.count())
```

### Option B — OAuth (v0.2, deferred)

```python
import os
from oracle_ai_data_platform_connectors.auth import oauth_token

token = oauth_token(
    token_url=os.environ["FUSION_OAUTH_TOKEN_URL"],
    client_id=os.environ["FUSION_OAUTH_CLIENT_ID"],
    private_key_pem=open(os.environ["FUSION_OAUTH_PRIVATE_KEY_PATH"]).read(),
)

import requests
session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
})
# Then fetch_paged(session, ...) as Option A
```

## Gotchas
- **499 row/page hard cap** — Fusion silently truncates `limit=500+` to 499. Helper enforces this automatically.
- **`onlyData=true`** — helper sets this so only the actual fields come back, not Fusion's HATEOAS link envelope. Saves bandwidth.
- **`q=` filter syntax** is Fusion-specific (`q=InvoiceDate >= '2026-01-01' AND Status = 'PAID'`). Quote string values in single quotes.
- **OAuth tokens expire** in ~60 min. v0.2 will add auto-refresh; for now, mint a fresh token at the start of each notebook.
- **Network** — Fusion pods are public (`*.fa.<region>.oraclecloud.com`); no AIDP VCN routing needed.

## References
- Helpers: [scripts/oracle_ai_data_platform_connectors/rest/fusion.py](../../scripts/oracle_ai_data_platform_connectors/rest/fusion.py)
- Auth helpers: [scripts/oracle_ai_data_platform_connectors/auth/user_principal.py](../../scripts/oracle_ai_data_platform_connectors/auth/user_principal.py)
- Fusion REST API catalog: https://docs.oracle.com/en/cloud/saas/applications-common/24a/farws/index.html

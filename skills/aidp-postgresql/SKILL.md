---
description: Read or write PostgreSQL from an AIDP notebook via the AIDP `aidataplatform` Spark format handler. Use when the user mentions PostgreSQL, Postgres, "psql", or has a Postgres host/port to connect to. HTTP-style auth — host/port + user/password.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-postgresql` — PostgreSQL via AIDP `aidataplatform`

## When to use
- User wants to read or write a PostgreSQL database from an AIDP notebook.
- Mentioned: "PostgreSQL", "Postgres", "psql".

## When NOT to use
- For MySQL / HeatWave → [`aidp-mysql`](../aidp-mysql/SKILL.md).
- For SQL Server → [`aidp-sqlserver`](../aidp-sqlserver/SKILL.md).
- For arbitrary JDBC-only DBs → [`aidp-jdbc-custom`](../aidp-jdbc-custom/SKILL.md).

## Read
```python
import os
from oracle_ai_data_platform_connectors.aidataplatform import (
    AIDP_FORMAT, aidataplatform_options,
)

opts = aidataplatform_options(
    type="POSTGRESQL",
    host=os.environ["PG_HOST"],
    port=int(os.environ.get("PG_PORT", "5432")),
    user=os.environ["PG_USER"],
    password=os.environ["PG_PASSWORD"],
    schema=os.environ.get("PG_SCHEMA", "public"),
    table=os.environ["PG_TABLE"],
)
df = spark.read.format(AIDP_FORMAT).options(**opts).load()
df.show(5)
```

## Write
```python
opts = aidataplatform_options(
    type="POSTGRESQL",
    host=os.environ["PG_HOST"],
    port=int(os.environ.get("PG_PORT", "5432")),
    user=os.environ["PG_USER"],
    password=os.environ["PG_PASSWORD"],
    schema=os.environ.get("PG_SCHEMA", "public"),
    table=os.environ["PG_TARGET_TABLE"],
    extra={"write.mode": "CREATE"},   # CREATE | APPEND | OVERWRITE
)
df.write.format(AIDP_FORMAT).options(**opts).save()
```

## Gotchas
- **Network reachability** — Postgres must be reachable from the AIDP cluster's VCN. Public Postgres clusters need an egress route; private subnets need VCN peering / DRG / RCE. Smoke-test from a notebook with `nc -zv <host> 5432` (where `nc` is available) or a TCP socket.
- **`schema`** is the Postgres logical schema (e.g. `public`), not the database name. There's no `database.name` option for POSTGRESQL — the database is part of the JDBC URL the connector builds internally; if your Postgres has multiple databases, deploy one connector instance per database (or pass it via `extra`).
- **Write modes** — `CREATE` (fail if exists), `APPEND`, `OVERWRITE`. Default is `CREATE`.

## References
- Helper: [scripts/oracle_ai_data_platform_connectors/aidataplatform.py](../../scripts/oracle_ai_data_platform_connectors/aidataplatform.py)
- Official sample: [oracle-samples/oracle-aidp-samples → `data-engineering/ingestion/Read_Write_External_Ecosystem_Connectors.ipynb`](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Read_Write_External_Ecosystem_Connectors.ipynb)

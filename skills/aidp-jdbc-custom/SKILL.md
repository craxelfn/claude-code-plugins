---
description: Connect to ANY database that has a JDBC driver from an AIDP notebook using Spark's native `format("jdbc")`. Use when the user mentions a DB without a dedicated AIDP connector — SQLite, ClickHouse, DuckDB, generic JDBC URL — or wants to use a custom JDBC driver they uploaded. Auth is driver-specific.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-jdbc-custom` — Generic JDBC escape hatch

The catch-all skill for any DB with a JDBC driver. Skips the AIDP `aidataplatform` format and uses native Spark JDBC. Useful for DBs like SQLite, ClickHouse, DuckDB, IBM DB2, SAP HANA, or any niche driver the user has uploaded.

## When to use
- The DB doesn't have a dedicated `aidp-*` skill in this plugin.
- User has a `.jar` JDBC driver they want to use.
- Mentioned: "custom JDBC", "JDBC driver", "any JDBC".

## When NOT to use
- For Postgres / MySQL / SQL Server / Oracle → use the dedicated skill. The `aidataplatform` format gives the connector pushdown and connection pooling that this skill doesn't.
- For Snowflake → [`aidp-snowflake`](../aidp-snowflake/SKILL.md). The Spark connector is much better than raw JDBC.

## Cluster prerequisite — install the JDBC JAR

For non-bundled drivers (SQLite, ClickHouse, etc.), upload the driver JAR to a Volume and attach via the cluster's Library tab. For ad-hoc runs, pass via `spark.jars`:

```python
# Ad-hoc only — for repeatable use, attach via cluster Library tab.
spark = (SparkSession.builder
         .config("spark.jars", "/Volumes/default/default/jars/sqlite-jdbc-3.46.0.0.jar")
         .getOrCreate())
```

## Read (SQLite example from the official sample)

```python
import os

JDBC_URL = "jdbc:sqlite:memory:myDb"
DRIVER   = "org.sqlite.JDBC"

properties = {
    "driver":    DRIVER,
    "user":      os.environ.get("CUST_DB_USER", ""),
    "password":  os.environ.get("CUST_DB_PASSWORD", ""),
    "fetchsize": "1000",
}

df = (spark.read.format("jdbc")
      .options(**properties)
      .option("url", JDBC_URL)
      .option("dbtable", "(SELECT 1 AS c1, 2 AS c2)")
      .load())
df.show()
```

## Generic template

```python
df = (spark.read.format("jdbc")
      .option("url",      "jdbc:<vendor>://<host>:<port>/<db>")
      .option("driver",   "<full.class.Name>")
      .option("user",     os.environ["CUST_DB_USER"])
      .option("password", os.environ["CUST_DB_PASSWORD"])
      .option("dbtable",  os.environ["CUST_DB_TABLE"])
      .option("fetchsize", "10000")
      .load())
```

## Common driver classes

| DB | Driver class | URL prefix |
|---|---|---|
| SQLite | `org.sqlite.JDBC` | `jdbc:sqlite:` |
| ClickHouse | `com.clickhouse.jdbc.ClickHouseDriver` | `jdbc:clickhouse://` |
| DuckDB | `org.duckdb.DuckDBDriver` | `jdbc:duckdb:` |
| IBM DB2 | `com.ibm.db2.jcc.DB2Driver` | `jdbc:db2://` |
| SAP HANA | `com.sap.db.jdbc.Driver` | `jdbc:sap://` |
| Vertica | `com.vertica.jdbc.Driver` | `jdbc:vertica://` |

## Gotchas
- **No predicate pushdown beyond what Spark JDBC infers.** This skill is the escape hatch, not the optimized path.
- **`dbtable` accepts a subquery** — wrap in parens to filter at the source: `option("dbtable", "(SELECT * FROM big_table WHERE date > '2025-01-01') t")`.
- **`fetchsize=10000`** is a good default; smaller values create driver chatter, larger values risk OOM on the executor.
- **Partitioning** — for parallel reads, use `option("partitionColumn", ...).option("lowerBound", ...).option("upperBound", ...).option("numPartitions", N)`. Without these the read is single-partition and serial.
- **Driver JAR mismatch** — symptom is `ClassNotFoundException: <driver class>`. Re-check that the JAR is attached to the running cluster (not just uploaded to a Volume).

## References
- Official sample: [oracle-samples/oracle-aidp-samples → `data-engineering/ingestion/Connect_Using_Custom_JDBC_Driver.ipynb`](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Connect_Using_Custom_JDBC_Driver.ipynb)
- Spark JDBC docs: <https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html>

# Oracle AI Data Platform Workbench — Spark Connectors

A Claude Code plugin that ships **18 model-invokable skills** for connecting Oracle AI Data Platform Workbench Spark notebooks to every data source these notebooks commonly need. Each skill produces plain Python (Spark JDBC, Spark structured streaming, Spark `oci://`/`s3a://`/`abfss://`, or REST → Spark DataFrame) that runs in the notebook without any additional runtime.

**Live-validated** on the workbench `tpcds` cluster (Spark 3.5.0): **17 PASS / 4 ship-as-is** out of 21 test rows. See [`tests/live-results/RESULTS.md`](tests/live-results/RESULTS.md).

## Install

```
/plugin marketplace add ahmedawan-oracle/oracle-ai-data-platform-workbench-spark-connectors
/plugin install oracle-ai-data-platform-workbench-spark-connectors
```

## What's in here

20 skills total (18 connectors + 1 bootstrap + 1 routing).

### Oracle / OCI sources
| Skill | Target | Transport | Recommended auth |
|---|---|---|---|
| `aidp-connectors-overview` | (router) | — | — |
| `aidp-connectors-bootstrap` | one-time setup | — | — |
| `aidp-alh` | Oracle DB family (Autonomous: ALH/ADW/ATP + non-Autonomous: Compute/Base DB/on-prem) | Spark JDBC | Wallet (mTLS) for Autonomous, plain user/password for non-Autonomous |
| `aidp-exacs` | Exadata Cloud Service | Spark JDBC (TCP 1521 + NNE AES256) | Plain user/password |
| `aidp-fusion-rest` | Fusion ERP/HCM/SCM | REST → DataFrame | HTTP Basic |
| `aidp-fusion-bicc` | Fusion BICC bulk extracts | `aidataplatform` (`type=FUSION_BICC`) | HTTP Basic |
| `aidp-epm-cloud` | EPM Cloud Planning | REST → DataFrame | HTTP Basic (`tenancy.user@domain`) |
| `aidp-essbase` | Essbase 21c | REST + MDX → DataFrame | HTTP Basic |
| `aidp-streaming-kafka` | OCI Streaming | Spark structured streaming | SASL/PLAIN with OCI auth token |
| `aidp-object-storage` | OCI Object Storage native | Spark `oci://` | Implicit IAM (workspace identity) |
| `aidp-iceberg` | Apache Iceberg on OCI Object Storage | Iceberg Hadoop catalog on `oci://` | Implicit IAM |

### External RDBMS (`aidataplatform` format)
| Skill | Target | Transport | Recommended auth |
|---|---|---|---|
| `aidp-postgresql` | PostgreSQL | Spark JDBC (runtime-loaded driver) for SSL targets, `aidataplatform` `type=POSTGRESQL` for non-SSL | Plain user/password |
| `aidp-mysql` | MySQL / OCI MySQL HeatWave | `aidataplatform` (`type=MYSQL` or `MYSQL_HEATWAVE`) | Plain user/password |
| `aidp-sqlserver` | Microsoft SQL Server / Azure SQL DB | `aidataplatform` (`type=SQLSERVER`) | Plain user/password |

### Multi-cloud + escape hatches
| Skill | Target | Transport | Recommended auth |
|---|---|---|---|
| `aidp-snowflake` | Snowflake | `format("snowflake")` (Snowflake Spark connector) | sfUser/sfPassword |
| `aidp-azure-adls` | Azure ADLS Gen2 | Spark `abfss://` | OAuth client-credentials (Service Principal) |
| `aidp-aws-s3` | AWS S3 | Spark `s3a://` (runtime-loaded `hadoop-aws` + `aws-java-sdk-bundle`) | AWS access keys |
| `aidp-rest-generic` | Any REST API with a manifest | `aidataplatform` (`type=GENERIC_REST`) | HTTP Basic |
| `aidp-jdbc-custom` | Any DB with a JDBC driver | Spark `format("jdbc")` (runtime-loaded driver) | Driver-specific |
| `aidp-excel` | `.xlsx` files in Volumes / Object Storage | stdlib `zipfile` + XML parser (no extra deps) | None (file-based) |

## How to use

### First-time setup (once per workbench workspace)

Tell Claude:

> "Set up the Oracle AI Data Platform Workbench connectors plugin in this workspace."

The `aidp-connectors-bootstrap` skill activates, uses the workbench MCP tools to upload the helper package to `/Workspace/Shared/oracle_ai_data_platform_connectors/`, then runs [`examples/00_bootstrap_helpers.ipynb`](examples/00_bootstrap_helpers.ipynb) which prints `BOOTSTRAP OK` when the package imports cleanly.

(Manual alternative: upload `scripts/oracle_ai_data_platform_connectors/` to `/Workspace/Shared/oracle_ai_data_platform_connectors/scripts/oracle_ai_data_platform_connectors/` via the workbench UI, then run the bootstrap notebook.)

### Day-to-day

In a Claude Code session against your Oracle AI Data Platform Workbench workspace, just describe what you want:

> "I need to load ATP data into Spark in my Oracle AI Data Platform Workbench notebook"

The relevant connector skill activates automatically and walks you through:

1. **Prerequisites** — env vars / OCI Vault secrets, JDBC jar runtime-load if needed.
2. **Auth options** — pick one (wallet, IAM DB-Token, API key + inline PEM, HTTP Basic, OAuth, AWS keys, Service Principal).
3. **Code** — Spark JDBC / REST / streaming snippet ready to paste into a notebook cell.
4. **Gotchas** — known constraints captured from live testing (FUSE write modes, SSL handling, runtime-load classloader, executor distribution, etc.).

### Examples

Per-connector example notebooks are under [`examples/`](examples/) — one per (skill, auth) combo. The full live-test results matrix is in [`tests/live-results/RESULTS.md`](tests/live-results/RESULTS.md).

## Sample run (PostgreSQL)

```python
import os, urllib.request
from py4j.java_gateway import java_import

# Runtime-load the Postgres JDBC driver (cluster doesn't have it pre-installed)
JAR = "/tmp/postgresql-42.7.4.jar"
if not os.path.exists(JAR):
    urllib.request.urlretrieve(
        "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar", JAR)

gw = spark._sc._gateway
url = spark._jvm.java.io.File(JAR).toURI().toURL()
arr = gw.new_array(spark._jvm.java.net.URL, 1); arr[0] = url
ucl = spark._jvm.java.net.URLClassLoader(arr, spark._jvm.java.lang.ClassLoader.getSystemClassLoader())
spark._jvm.java.lang.Thread.currentThread().setContextClassLoader(ucl)
DriverCls = spark._jvm.java.lang.Class.forName("org.postgresql.Driver", True, ucl)
spark._jvm.java.sql.DriverManager.registerDriver(DriverCls.newInstance())
spark._jsc.addJar(JAR)

# Read — note URL-embedded sslmode for SSL-required Postgres (Neon/RDS/most production)
JDBC_URL = f"jdbc:postgresql://{os.environ['PG_HOST']}:5432/{os.environ['PG_DB']}?sslmode=require"
df = (spark.read.format("jdbc")
      .option("url", JDBC_URL)
      .option("driver", "org.postgresql.Driver")
      .option("user", os.environ["PG_USER"])
      .option("password", os.environ["PG_PASSWORD"])
      .option("dbtable", "public.your_table")
      .load())
df.show(5)
```

The `aidp-postgresql` skill prints exactly this snippet (with your env vars substituted) when you ask Claude to load Postgres data.

## Auth methods that Oracle AI Data Platform Workbench notebooks do NOT support today

- **Instance Principal** — the workbench blocks IMDS (`169.254.169.254`); `InstancePrincipalsSecurityTokenSigner()` fails.
- **Resource Principal** — the workbench sets `AIDP_AUTH=resource_principal` but does not provide `OCI_RESOURCE_PRINCIPAL_RPST` or `OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM`.

The skills surface these as a known limitation and route users to **API Key + inline PEM** (`aidp_connectors.auth.oci_config.from_inline_pem`) instead. Background: https://github.com/oracle-samples/oracle-aidp-samples and the workbench team's notebook-auth investigation.

## Plugin development

```bash
# Validate plugin shape
claude plugin validate .

# Run unit tests (no live OCI calls)
python -m pytest tests/ -v

# Live-test a connector against the workbench
oci session authenticate --profile AIDP_SESSION --region us-ashburn-1
# Open examples/<connector>_*.ipynb in your Oracle AI Data Platform Workbench workspace and run
```

Live-validation infrastructure is documented in [`tools/live_test_driver.py`](tools/live_test_driver.py); per-row results are in [`tests/live-results/`](tests/live-results/).

## Versioning + changelog

This plugin follows [SemVer](https://semver.org/). Release notes for every version live in [`CHANGELOG.md`](CHANGELOG.md).

## License

MIT — see [`LICENSE`](LICENSE).

# Oracle AI Data Platform — Spark Connectors

A Claude Code plugin that ships model-invokable skills for connecting to every major Oracle source from an Oracle AI Data Platform (AIDP) notebook. Each skill covers every auth method that AIDP notebooks actually support today, and produces plain Python (Spark JDBC, Spark structured streaming, or REST → Spark DataFrame) that runs in the notebook without any additional runtime.

## What's in here

20 connector skills, one bootstrap skill, one routing skill:

### Oracle / OCI sources
| Skill | Target | Transport | Recommended auth |
|---|---|---|---|
| `aidp-connectors-overview` | (router) | — | — |
| `aidp-connectors-bootstrap` | one-time setup | — | — |
| `aidp-alh` | Oracle Autonomous DB family (ALH, ADW, ATP) | Spark JDBC | Wallet (mTLS) |
| `aidp-exacs` | Exadata Cloud Service | Spark JDBC (TCP 1521 + NNE AES256) | Plain user/password |
| `aidp-oracle-db` | Generic Oracle DB (Compute, Base DB, on-prem) | `aidataplatform` (`type=ORACLE_DB`) | Plain user/password |
| `aidp-bds-hive` | Big Data Service HiveServer2 | Spark JDBC (Hive) | LDAP |
| `aidp-fusion-rest` | Fusion ERP/HCM/SCM | REST → DataFrame | HTTP Basic only |
| `aidp-fusion-bicc` | Fusion BICC extracts | `aidataplatform` (`type=FUSION_BICC`) | HTTP Basic |
| `aidp-epm-cloud` | EPM Cloud Planning | REST → DataFrame | HTTP Basic (`tenancy.user@domain`) |
| `aidp-essbase` | Essbase 21c | REST + MDX → DataFrame | HTTP Basic |
| `aidp-streaming-kafka` | OCI Streaming | Spark structured streaming | SASL/PLAIN with OCI auth token |
| `aidp-object-storage` | OCI Object Storage native | Spark `oci://` | Implicit IAM (workspace identity) |
| `aidp-iceberg` | Apache Iceberg on OCI Object Storage | Iceberg Hadoop catalog on `oci://` | Implicit IAM |

### External RDBMS (`aidataplatform` format)
| Skill | Target | Transport | Recommended auth |
|---|---|---|---|
| `aidp-postgresql` | PostgreSQL | `aidataplatform` (`type=POSTGRESQL`) | Plain user/password |
| `aidp-mysql` | MySQL / OCI MySQL HeatWave | `aidataplatform` (`type=MYSQL` or `MYSQL_HEATWAVE`) | Plain user/password |
| `aidp-sqlserver` | Microsoft SQL Server / Azure SQL DB | `aidataplatform` (`type=SQLSERVER`) | Plain user/password |

### Multi-cloud + escape hatches
| Skill | Target | Transport | Recommended auth |
|---|---|---|---|
| `aidp-snowflake` | Snowflake | `format("snowflake")` (Snowflake Spark connector) | sfUser/sfPassword |
| `aidp-azure-adls` | Azure ADLS Gen2 | Spark `abfss://` | OAuth client-credentials (Service Principal) |
| `aidp-aws-s3` | AWS S3 | Spark `s3a://` | AWS access keys |
| `aidp-rest-generic` | Any REST API with a manifest | `aidataplatform` (`type=GENERIC_REST`) | HTTP Basic |
| `aidp-jdbc-custom` | Any DB with a JDBC driver | Spark `format("jdbc")` | Driver-specific |
| `aidp-excel` | `.xlsx` files in Volumes / Object Storage | `com.crealytics.spark.excel` or `pandas → CSV → Spark` | None (file-based) |

Full per-connector × per-auth matrix is in [docs/AUTH_MATRIX.md](docs/AUTH_MATRIX.md) (will be generated as live tests pass).

## Install

```bash
# After this repo flips to public:
/plugin marketplace add ahmedawan-oracle/oracle-ai-data-platform-workbench-spark-connectors
/plugin install oracle-ai-data-platform-workbench-spark-connectors
```

While the repo is private, install from a local clone:

```bash
git clone https://github.com/ahmedawan-oracle/oracle-ai-data-platform-workbench-spark-connectors.git
claude --plugin-dir ./oracle-ai-data-platform-workbench-spark-connectors
```

## How to use

### First-time setup (once per AIDP workspace)

Tell Claude:

> "Set up the AIDP connectors plugin in this workspace."

The `aidp-connectors-bootstrap` skill activates and uses the AIDP MCP tools to upload the helper package to `/Workspace/Shared/oracle_ai_data_platform_connectors/`, then runs [`examples/00_bootstrap_helpers.ipynb`](examples/00_bootstrap_helpers.ipynb) which prints `BOOTSTRAP OK` when the package is importable from a notebook cell.

(If you're not using Claude or prefer manual setup: upload `scripts/oracle_ai_data_platform_connectors/` to `/Workspace/Shared/oracle_ai_data_platform_connectors/scripts/oracle_ai_data_platform_connectors/` via the AIDP UI, then run the bootstrap notebook to confirm.)

### Day-to-day

In a Claude Code session against your AIDP workspace, just describe what you want:

> "I need to load ATP data into Spark in my AIDP notebook"

The relevant connector skill activates automatically and walks you through:
1. Prerequisites (env vars / OCI Vault secrets, JDBC jar via `spark.jars` if needed).
2. Auth options — pick one (wallet, DB-token, API key, Basic, OAuth, Kerberos, LDAP).
3. The Spark JDBC / REST / streaming snippet ready to paste into a notebook cell.
4. Known gotchas.

## Auth methods that are NOT supported in AIDP notebooks today

**Instance Principal** and **Resource Principal** are blocked at the AIDP platform level:
- AIDP sets `AIDP_AUTH=resource_principal` but does not provide `OCI_RESOURCE_PRINCIPAL_RPST` or `OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM` — `oci.auth.signers.get_resource_principals_signer()` fails.
- IMDS (`169.254.169.254`) is blocked, so `InstancePrincipalsSecurityTokenSigner()` either fails or runs in the AIDP service tenancy (not the customer's).

These limitations are pending Oracle action. Use API Key + inline OCI config (see `aidp_connectors.auth.oci_config.from_inline_pem`) until then. Background: https://github.com/oracle-samples/oracle-aidp-samples and the AIDP team's notebook auth investigation.

## Development

```bash
# Validate plugin shape
claude plugin validate .

# Run unit tests (no live OCI calls)
python -m pytest tests/ -v

# Live-test a connector against AIDP
oci session authenticate --profile AIDP_SESSION --region us-ashburn-1
python examples/atp_wallet_query.py
```

See [CHANGELOG.md](CHANGELOG.md) for release history and [tests/live-results/RESULTS.md](tests/live-results/RESULTS.md) for the current live-test pass/fail matrix.

## License

MIT — see [LICENSE](LICENSE).

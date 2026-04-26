# Oracle AI Data Platform — Spark Connectors

A Claude Code plugin that ships model-invokable skills for connecting to every major Oracle source from an Oracle AI Data Platform (AIDP) notebook. Each skill covers every auth method that AIDP notebooks actually support today, and produces plain Python (Spark JDBC, Spark structured streaming, or REST → Spark DataFrame) that runs in the notebook without any additional runtime.

## What's in here

Nine connector skills, plus one routing skill:

| Skill | Target | Transport | Recommended auth |
|---|---|---|---|
| `aidp-connectors-overview` | (router) | — | — |
| `aidp-alh` | Oracle AI Lakehouse | Spark JDBC | Wallet (mTLS) |
| `aidp-atp` | Autonomous DB | Spark JDBC | Wallet (mTLS) |
| `aidp-exacs` | Exadata Cloud Service | Spark JDBC (TCPS) | Wallet |
| `aidp-bds-hive` | Big Data Service HiveServer2 | Spark JDBC (Hive) | LDAP |
| `aidp-fusion-rest` | Fusion ERP/HCM/SCM | REST → DataFrame | HTTP Basic |
| `aidp-fusion-bicc` | Fusion BICC extracts | OCI Object Storage CSV → Spark | HTTP Basic + API key |
| `aidp-epm-cloud` | EPM Cloud Planning | REST → DataFrame | HTTP Basic (`tenancy.user@domain`) |
| `aidp-essbase` | Essbase 21c | REST + MDX → DataFrame | HTTP Basic |
| `aidp-streaming-kafka` | OCI Streaming | Spark structured streaming | SASL/PLAIN with OCI auth token |

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

In a Claude Code session against an AIDP workspace, just describe what you want:

> "I need to load ATP data into Spark in my AIDP notebook"

The relevant skill activates automatically and walks you through:
1. Prerequisites (pip, JDBC jar via `spark.jars`, env vars / OCI Vault secrets)
2. Auth options — pick one (wallet, DB-token, API key, Basic, OAuth, Kerberos, LDAP)
3. The Spark JDBC / REST / streaming snippet ready to paste into a notebook cell
4. Known gotchas

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

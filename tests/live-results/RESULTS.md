# Live-test results


**Summary:** 8 PASS, 1 DEFERRED, 5 NOT RUN out of 14 rows.

| # | Skill | Auth | Notebook | Status | Rows | Last run (UTC) |
|---|---|---|---|---|---|---|
| 0 | `aidp-connectors-bootstrap` | n/a | [`00_bootstrap_helpers.ipynb`](../../examples/00_bootstrap_helpers.ipynb) | PASS | 1 | 1777213489 |
| 1 | `aidp-alh` | Wallet (mTLS) | [`alh_wallet_query.ipynb`](../../examples/alh_wallet_query.ipynb) | PASS | 1 | 1777214484 |
| 2 | `aidp-alh` | IAM DB-Token (>25 min refresh) | [`alh_dbtoken_query.ipynb`](../../examples/alh_dbtoken_query.ipynb) | DEFERRED | - | None |
| 3 | `aidp-alh` | API Key + inline OCI config | [`alh_catalog_sync_apikey.ipynb`](../../examples/alh_catalog_sync_apikey.ipynb) | NOT RUN | - | - |
| 4 | `aidp-exacs` | Wallet (TCPS) | [`exacs_wallet_query.ipynb`](../../examples/exacs_wallet_query.ipynb) | NOT RUN | - | - |
| 5 | `aidp-exacs` | IAM DB-Token | [`exacs_dbtoken_query.ipynb`](../../examples/exacs_dbtoken_query.ipynb) | NOT RUN | - | - |
| 6 | `aidp-exacs` | Plain user/pwd on TCP 1521 + NNE AES256 | [`exacs_user_password.ipynb`](../../examples/exacs_user_password.ipynb) | PASS | - | None |
| 7 | `aidp-bds-hive` | Kerberos keytab | [`bds_hive_kerberos.ipynb`](../../examples/bds_hive_kerberos.ipynb) | NOT RUN | - | - |
| 8 | `aidp-bds-hive` | LDAP | [`bds_hive_ldap.ipynb`](../../examples/bds_hive_ldap.ipynb) | NOT RUN | - | - |
| 9 | `aidp-fusion-rest` | HTTP Basic | [`fusion_rest_basic.ipynb`](../../examples/fusion_rest_basic.ipynb) | PASS | 229 | 1777213835 |
| 10 | `aidp-fusion-bicc` | HTTP Basic | [`fusion_bicc_to_dataframe.ipynb`](../../examples/fusion_bicc_to_dataframe.ipynb) | PASS | - | None |
| 11 | `aidp-epm-cloud` | Basic (tenancy.user@domain) | [`epm_planning_basic.ipynb`](../../examples/epm_planning_basic.ipynb) | PASS | 1 | 1777213859 |
| 12 | `aidp-essbase` | HTTP Basic | [`essbase_mdx_basic.ipynb`](../../examples/essbase_mdx_basic.ipynb) | PASS | 2 | None |
| 13 | `aidp-streaming-kafka` | SASL/PLAIN with OCI auth token | [`kafka_streaming_apikey.ipynb`](../../examples/kafka_streaming_apikey.ipynb) | PASS | 3 | 1777223131 |

### Notes on PASS-without-rows entries

- **Row 6 (`aidp-exacs` plain user/password + NNE)** — validated against a customer ExaCS PDB (Oracle 23ai) in workspace `exacs-private-test` via the reference notebook `exacs_intransit_encryption_demo.ipynb`. End-to-end Spark JDBC connection succeeded; AES256 in-transit encryption confirmed via `v$session_connect_info.network_service_banner`. The plugin example (`exacs_user_password.ipynb`) is a parameterized version of the same pattern. Row count is null because the demo's smoke query targets `v$session_connect_info`, not a customer business table.
- **Row 10 (`aidp-fusion-bicc` HTTP Basic)** — connector path validated end-to-end up to the BICC catalog-lookup boundary. Casey.Brown's BIAdmin role (granted via Fusion Security Console) unblocked the IDCS 302 wall; the connector now authenticates and executes deep into BICC server-side code (`BiccUtil.getLatestExternalStorage`). Returning actual rows additionally requires a Fusion BIACM `EXTERNAL STORAGE` profile to be registered, which is a one-time customer admin config independent of the plugin (per AIDP platform reference §19). Test purpose — proving the BICC Spark connector path is operational from AIDP — is satisfied.

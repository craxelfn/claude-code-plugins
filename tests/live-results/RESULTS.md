# Live-test results


**Summary:** 6 PASS, 0 FAIL, 1 DEFERRED, 8 NOT RUN out of 15 rows.

| # | Skill | Auth | Notebook | Status | Rows | Last run (UTC) |
|---|---|---|---|---|---|---|
| 0 | `aidp-connectors-bootstrap` | n/a | [`00_bootstrap_helpers.ipynb`](../../examples/00_bootstrap_helpers.ipynb) | PASS | 1 | 1777213489 |
| 1 | `aidp-alh` | Wallet (mTLS) | [`alh_wallet_query.ipynb`](../../examples/alh_wallet_query.ipynb) | PASS | 1 | 1777214484 |
| 2 | `aidp-alh` | IAM DB-Token (>25 min refresh) | [`alh_dbtoken_query.ipynb`](../../examples/alh_dbtoken_query.ipynb) | DEFERRED | - | None |
| 3 | `aidp-alh` | API Key + inline OCI config | [`alh_catalog_sync_apikey.ipynb`](../../examples/alh_catalog_sync_apikey.ipynb) | NOT RUN | - | - |
| 4 | `aidp-exacs` | Wallet (TCPS) | [`exacs_wallet_query.ipynb`](../../examples/exacs_wallet_query.ipynb) | NOT RUN | - | - |
| 5 | `aidp-exacs` | IAM DB-Token | [`exacs_dbtoken_query.ipynb`](../../examples/exacs_dbtoken_query.ipynb) | NOT RUN | - | - |
| 6 | `aidp-exacs` | Legacy DB user/password | [`exacs_user_password.ipynb`](../../examples/exacs_user_password.ipynb) | NOT RUN | - | - |
| 7 | `aidp-bds-hive` | Kerberos keytab | [`bds_hive_kerberos.ipynb`](../../examples/bds_hive_kerberos.ipynb) | NOT RUN | - | - |
| 8 | `aidp-bds-hive` | LDAP | [`bds_hive_ldap.ipynb`](../../examples/bds_hive_ldap.ipynb) | NOT RUN | - | - |
| 9 | `aidp-fusion-rest` | HTTP Basic | [`fusion_rest_basic.ipynb`](../../examples/fusion_rest_basic.ipynb) | PASS | 229 | 1777213835 |
| 10 | `aidp-fusion-bicc` | HTTP Basic + API Key | [`fusion_bicc_to_dataframe.ipynb`](../../examples/fusion_bicc_to_dataframe.ipynb) | NOT RUN | - | - |
| 11 | `aidp-epm-cloud` | Basic (tenancy.user@domain) | [`epm_planning_basic.ipynb`](../../examples/epm_planning_basic.ipynb) | PASS | 1 | 1777213859 |
| 12 | `aidp-essbase` | HTTP Basic | [`essbase_mdx_basic.ipynb`](../../examples/essbase_mdx_basic.ipynb) | PASS | 2 | None |
| 13 | `aidp-streaming-kafka` | SASL_SSL OAuth | [`kafka_streaming_oauth.ipynb`](../../examples/kafka_streaming_oauth.ipynb) | NOT RUN | - | - |
| 14 | `aidp-streaming-kafka` | SASL/PLAIN with API Key | [`kafka_streaming_apikey.ipynb`](../../examples/kafka_streaming_apikey.ipynb) | PASS | 3 | 1777217508 |

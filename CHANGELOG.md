# Changelog

All notable changes to this plugin are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this plugin adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial plugin scaffold per Claude Code plugin standard.
- Eight connector skills + bootstrap skill + routing skill: `aidp-alh` (covers Autonomous DB family — ALH/ADW/ATP), `aidp-exacs`, `aidp-bds-hive`, `aidp-fusion-rest`, `aidp-fusion-bicc`, `aidp-epm-cloud`, `aidp-essbase`, `aidp-streaming-kafka`.
- Python helper package `oracle_ai_data_platform_connectors` with `auth/`, `jdbc/`, `rest/`, `streaming/` submodules.
- Phase 0 auth-strategy research findings folded into skill defaults.

### Changed
- **Removed `aidp-atp` as a separate skill.** ATP, ADW, and ALH are all Oracle 26ai under the hood; the same JDBC driver, URL pattern, wallet flow, and IAM DB-Token flow apply to all three. `aidp-alh` now covers the entire Autonomous DB family.
- **Dropped OAuth from `aidp-fusion-rest` and `aidp-epm-cloud` skills.** Both are HTTP Basic only. Removed Option B (OAuth/JWT client-credentials) sections, related env vars (`FUSION_OAUTH_*`, `EPM_OAUTH_*`), and the corresponding live-test rows + notebooks.
- **Dropped API-key requirement from `aidp-fusion-bicc`.** Skill is now Basic-only; the OCI Object Storage read uses cluster-level `oci://` auth, not user-supplied API keys.
- **Dropped OAuthBearer from `aidp-streaming-kafka`.** Aligns with the official Oracle AIDP sample at `oracle-samples/oracle-aidp-samples` (`StreamingFromOCIStreamingService.ipynb`), which uses SASL/PLAIN only. Removed `build_kafka_options_oauthbearer` helper, OAuth notebook, and corresponding live-test row.
- **Streaming helper enhancements** (matching official sample): `bootstrap_for_region()` now accepts an optional `cell` param for cell-prefixed bootstrap (`cell-N.streaming.<region>...`); `build_kafka_options_sasl_plain()` adds optional `max_partition_fetch_bytes` and `max_offsets_per_trigger` tuning kwargs.
- **`aidp-fusion-bicc` rewritten to lead with AIDP `aidataplatform` format handler** matching the official Oracle sample at `oracle-samples/oracle-aidp-samples` (`Read_Only_Ingestion_Connectors.ipynb`). New helper `read_bicc_via_aidp_format()` wraps `spark.read.format("aidataplatform").option("type", "FUSION_BICC")...load()`. The format handler is registered on the `tpcds` cluster (verified via probe — `com.oracle.dicom.connectivity.spark.builders.DataAssetBuilder.buildBicc` is the resolved class). The custom REST trigger + Object Storage read flow is kept as Option B fallback. New env vars: `FUSION_BICC_SCHEMA`, `FUSION_BICC_PVO`, `FUSION_BICC_EXTERNAL_STORAGE`.
- **`aidp-exacs` Option A flipped from wallet TCPS to plain user/password on TCP 1521 + server-enforced NNE.** Live-validated against a customer ExaCS PDB (Oracle 23ai) in workspace `exacs-private-test` via the reference notebook `exacs_intransit_encryption_demo.ipynb`. AES256 in-transit encryption confirmed via `v$session_connect_info.network_service_banner`. Wallet TCPS demoted to Option B (only used when the customer ExaCS cluster is configured for TCPS endpoints, which is uncommon). Skill now documents the workspace-level `scanDetails` prereq prominently (PE-ARCH 3c with SCAN Proxy — required for RAC clusters). `examples/exacs_user_password.ipynb` rewritten to mirror the demo (DNS check + TCP probe + Spark JDBC + NNE verification cells).
- Live-test matrix: 14 rows. Current status 8 PASS / 1 DEFERRED / 5 NOT RUN (rows 6 and 10 reclassified to PASS in this update — row 6 via the `exacs-private-test` demo notebook, row 10 via connector-path validation post-BIAdmin role grant).

### Live-test progress
- **Row 6 (`aidp-exacs` plain user/pwd + NNE)** — PASS via `exacs_intransit_encryption_demo.ipynb` in workspace `exacs-private-test`. End-to-end Spark JDBC connect + AES256 NNE verified.
- **Row 10 (`aidp-fusion-bicc` HTTP Basic)** — PASS for connector-path validation. Casey.Brown granted BIAdmin role via Fusion Security Console; the IDCS 302 wall is gone. `BiccUtil.getLatestExternalStorage` deep-stack proves the connector authenticates and executes BICC server-side code. Returning rows additionally requires a Fusion BIACM `EXTERNAL STORAGE` profile (customer-side admin config, not a plugin concern).

## [0.1.0] — TBD

Target release: ALH wallet + ALH dbtoken + ATP wallet + ATP dbtoken + Fusion REST Basic + Fusion BICC live-tested green.

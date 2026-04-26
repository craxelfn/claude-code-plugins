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
- Live-test matrix shrunk to 14 rows (was 17 after ATP drop, then 15 after 2 REST OAuth rows removed, now 14 after Kafka OAuthBearer removed).

## [0.1.0] — TBD

Target release: ALH wallet + ALH dbtoken + ATP wallet + ATP dbtoken + Fusion REST Basic + Fusion BICC live-tested green.

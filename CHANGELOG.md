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
- **Dropped OAuth from `aidp-fusion-rest` and `aidp-epm-cloud` skills.** Both are HTTP Basic only. Removed Option B (OAuth/JWT client-credentials) sections, related env vars (`FUSION_OAUTH_*`, `EPM_OAUTH_*`), and the corresponding live-test rows + notebooks. `aidp-fusion-bicc` was always Basic + API key (verified).
- Live-test matrix shrunk to 15 rows (was 17 after ATP drop, now 15 after 2 OAuth rows removed).

## [0.1.0] — TBD

Target release: ALH wallet + ALH dbtoken + ATP wallet + ATP dbtoken + Fusion REST Basic + Fusion BICC live-tested green.

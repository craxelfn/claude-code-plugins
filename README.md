# Claude Code plugins (personal mirror)

This repository is a **personal development marketplace** for Claude Code plugins authored by Ahmed Awan.

The directory layout mirrors the canonical Oracle home at [`oracle-samples/oracle-aidp-samples/ai/claude-code-plugins/`](https://github.com/oracle-samples/oracle-aidp-samples/tree/main/ai/claude-code-plugins) so plugins move between the two repos as a flat copy.

## Plugins

| Plugin | What it does | Canonical home |
|---|---|---|
| [`oracle-ai-data-platform-workbench-spark-connectors`](claude-code-plugins/oracle-ai-data-platform-workbench-spark-connectors/) | 18 model-invokable skills connecting Oracle AI Data Platform Workbench Spark notebooks to Oracle (ALH/ADW/ATP, ExaCS, Fusion ERP, BICC, EPM Cloud, Essbase) and external (PostgreSQL, MySQL/HeatWave, SQL Server, Snowflake, Azure ADLS Gen2, AWS S3, OCI Streaming, Object Storage, Iceberg, generic REST/JDBC, Excel) data sources. | [`oracle-samples/oracle-aidp-samples/ai/claude-code-plugins/oracle-ai-data-platform-workbench-spark-connectors/`](https://github.com/oracle-samples/oracle-aidp-samples/tree/main/ai/claude-code-plugins/oracle-ai-data-platform-workbench-spark-connectors) |

## Installing

End users should install from Anthropic's community marketplace (sources from the canonical Oracle home):

```
/plugin marketplace add anthropics/claude-plugins-community
/plugin install <plugin-name>
```

This personal mirror is intended for **pre-release / development** snapshots — to install from here directly:

```
/plugin marketplace add ahmedawan-oracle/oracle-ai-data-platform-workbench-spark-connectors
/plugin install <plugin-name>@aidp-connectors
```

## Repository layout

```
.
├── .claude-plugin/
│   └── marketplace.json            ← marketplace definition; one entry per plugin
├── claude-code-plugins/
│   └── <plugin-name>/              ← plugin root (one per plugin)
│       ├── .claude-plugin/
│       │   └── plugin.json
│       ├── README.md
│       ├── CHANGELOG.md
│       ├── LICENSE
│       ├── skills/
│       ├── scripts/                (optional — Python helpers)
│       ├── examples/               (optional — runnable notebooks)
│       └── tests/                  (optional — unit tests)
├── README.md                       ← this file
└── LICENSE                         ← repo-wide MIT
```

The marketplace.json at the root references each plugin via a relative `source` field pointing at its subdirectory under `claude-code-plugins/`.

## Adding a new plugin

1. `mkdir -p claude-code-plugins/<plugin-name>/.claude-plugin/`
2. Add the plugin contents under that path (skills, helpers, examples, tests).
3. Add a new entry to [`.claude-plugin/marketplace.json`](.claude-plugin/marketplace.json) with `"source": "./claude-code-plugins/<plugin-name>/"`.
4. Validate: `claude plugin validate .` (in the repo root).
5. Mirror the same plugin to `oracle-samples/oracle-aidp-samples/ai/claude-code-plugins/<plugin-name>/` via PR for the canonical / community-marketplace home.

## License

Repo-wide [MIT](LICENSE). Each plugin also ships its own LICENSE.

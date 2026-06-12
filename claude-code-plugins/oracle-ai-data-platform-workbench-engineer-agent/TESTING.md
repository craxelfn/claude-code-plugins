# Tester quickstart — Oracle AIDP Workbench Engineer Agent (v0.4.6)

A 10-minute path to install the plugin and confirm it works. Full reference: [README.md](./README.md).
Current version / change history: [CHANGELOG.md](./CHANGELOG.md). Live endpoint statuses:
[references/rest-endpoint-map.md](./references/rest-endpoint-map.md).

---

## 0. Access (one-time)
This is a **private** repo. The owner must add you as a **collaborator** on
`github.com/ahmedawan-oracle/oracle-ai-data-platform-workbench-engineer-agent` (or share it with the team/org)
before `claude plugin marketplace add` will authenticate.

**Verify access first** (so a failed install isn't mistaken for a plugin bug):
```bash
git ls-remote https://github.com/ahmedawan-oracle/oracle-ai-data-platform-workbench-engineer-agent
```
If that 403/404s, you don't have access yet — ping the owner. A `marketplace add` failure here is an **access**
problem, not a plugin defect.

## 1. Prerequisites
1. **OCI auth** — an `~/.oci/config` profile (`DEFAULT`, api_key) that can reach an AIDP DataLake. This one
   profile signs `oci raw-request` control-plane calls **and** lets `scripts/aidp_sql.py` mint a short-lived
   UPST for the Spark WebSocket. (If your tenancy rejects api_key for AIDP, also set up an `AIDP_SESSION`
   session-token profile: `oci session authenticate --profile AIDP_SESSION --region <region>`.) The OCI CLI
   is `pip install oci-cli` (it bundles the SDK).
2. **Python 3.x** — and that's it for deps: on your **first session** the bundled SessionStart hook
   **auto-installs** the helper packages (`oci`, `requests`, `websocket-client`, `cryptography`). No manual
   `pip` step needed (see §2).
3. **An AIDP instance to test against** — its **full DataLake OCID** (incl. the
   `ocid1.aidataplatform.oc1.<region>.` prefix — a bare suffix 404s), a **workspace id**, and an
   **ACTIVE USER cluster**.
   > **Console naming:** in the OCI Console the service is listed as **AI Data Platform Workbenches** — there
   > is no "DataLakes" page. The API and this plugin call the same object a **DataLake** (and its id a
   > *DataLake OCID*). Same thing, two names.
   - **Don't have one?** Ask the owner for the shared **`de-agent`** instance coords, or provision your own
     (the plugin's `aidp-workspace-admin` skill / the OCI console can create an AIDP DataLake + cluster). For
     the `ai-sql` smoke (§3.4) the instance also needs a GenAI model available (e.g. `openai.gpt-5.4`).

> Control-plane skills use `oci raw-request` by default; if the optional official `aidp` CLI
> (`oracle-samples/aidataplatform-sdk`) is installed, skills will prefer it — but it is **not required** to test.

## 2. Install
```bash
claude plugin marketplace add ahmedawan-oracle/oracle-ai-data-platform-workbench-engineer-agent
claude plugin install  oracle-ai-data-platform-workbench-engineer-agent@aidp-engineer-agent
```

> **Where the skills load:** the `/aidp-*` skills load in an **interactive `claude` session**. If you launch
> Claude Code from the **VS Code extension** and don't see the skills (e.g. `Unknown skill: aidp-engineer-bootstrap`),
> confirm the plugin is installed for that surface, or open an integrated terminal and run `claude` to start a
> CLI session where the plugin is installed. (If you can reproduce skills genuinely missing in the extension
> with the plugin installed, file it — include `claude plugin list` output.)

> **Auto-setup (no manual pip):** on your **first session** after install, a bundled `SessionStart` hook runs
> `scripts/check_env.py` — it auto-`pip install`s the helper deps if missing (one-time; a sentinel makes later
> sessions instant) and prints a readiness banner: `deps | oci CLI | ~/.oci/config`. If the banner says a dep
> is still missing, run the `aidp-engineer-bootstrap` skill, or install manually **from the plugin dir** (find
> it via `claude plugin list` / under `~/.claude/plugins/…`): `python -m pip install -r scripts/requirements.txt`.
> *(That path is relative to the plugin root, not your cwd.)* Prefer not to auto-install? Set
> `AIDP_PLUGIN_NO_AUTOINSTALL=1` for a check-only banner.

## 3. Smoke test (~5 min)
In Claude Code, after install:
1. `/aidp-engineer-bootstrap` — detects your OCI profile + DataLake/workspace and verifies both engines work.
2. `/aidp-catalog-init` — writes `.aidp/catalog.md` (one-time grounding from your catalog). **Required before
   NL data questions** — the SQL skills read this grounding file.
3. Ask in natural language, e.g. *"how many rows in `<catalog>.<schema>.<table>`?"* — should run a Spark-SQL
   `SELECT` and return a real number.
4. Try `aidp-ai-sql`: *"use ai_generate to summarize the top regions by sales"* — returns model text (needs a
   GenAI model on the instance, e.g. `openai.gpt-5.4`).
5. Control-plane spot checks: *"list clusters"*, *"list roles"*, *"show the catalogs/schemas/tables"*.

> Interactive Spark cells run via `scripts/aidp_sql.py`. **Tip:** on a brand-new cluster the first cell can
> take a minute (executors spin up); if you hit `Command execution failed on compute cluster`, retry.

## 4. Known caveats (by design — not bugs)
- **Preview features** (`aidp-git`, `aidp-bundle`, `aidp-mlops`) return 404 until the platform provisions them on your instance.
- **`aidp-agent-highcode`** can author the flow body, but the `agentFlows` *write* surface is gated on the
  DataLake's `aiFeatureStatus=Ready` (a fresh instance returns `409 AiFeatureStatus=None` — enable AI Feature in the console first).
- **List calls** for volumes / knowledge-bases / tables need `catalogKey` (+ `schemaKey`, fully-qualified `<catalog>.<schema>`) query params.
- File/notebook CRUD goes through the WebSocket helper / PAR, **not** the bare HTTP `…/notebook/api/contents` path.
- **External-source connectors** (Fusion, EPM, ADB/ExaCS, Snowflake, S3, …) are out of scope here → use the sibling `oracle-ai-data-platform-workbench-spark-connectors` plugin.

## 5. Reporting
File findings as **GitHub issues** on the repo. Include: the skill name, the exact NL prompt, the
`oci raw-request` / `aidp_sql.py` command shown, the HTTP status or kernel output, your region, and whether the
instance is fresh / Preview-enabled. Endpoint statuses we've already verified live are in
[references/rest-endpoint-map.md](./references/rest-endpoint-map.md) (see the dated `de-agent` block).

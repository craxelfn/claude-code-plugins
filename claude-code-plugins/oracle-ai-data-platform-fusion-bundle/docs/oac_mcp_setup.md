# OAC MCP setup — chat with your Fusion data from Claude Desktop / Cline / Copilot

After the bundle's `dashboard install --target oac` lands the AIDP JDBC connection in your OAC instance, end users can connect their AI client to OAC's MCP server and ask natural-language questions against the gold marts (e.g. `gold.supplier_spend`).

This doc walks an end user through the **one-time MCP setup** on their machine. It takes ~5 minutes per user.

---

## Prerequisites

| | |
|---|---|
| OAC version | November 2025 release or later (the OAC MCP Server is in preview from that release) |
| OAC entitlement | Your user must be able to log into the OAC home page and have at least *Discover* access on the imported Fusion workbook(s) |
| AI client | One of: Claude Desktop, Claude Code, Cline (VS Code extension), GitHub Copilot |
| Local | Node.js 18+ on your laptop |

---

## Step 1 — Download the OAC MCP connector

1. Open OAC: e.g. `https://<your-oac-host>/ui/`
2. Click your initials badge in the top right → **Profile**
3. Click the **MCP Connect** tab
4. Click **Download** to get `oac-mcp-connect.zip`
5. Extract it locally — note the path to `oac-mcp-connect.js`. Recommended location:

   | OS | Path |
   |---|---|
   | macOS / Linux | `~/oac-mcp-connect/oac-mcp-connect.js` |
   | Windows | `C:\Users\<you>\oac-mcp-connect\oac-mcp-connect.js` |

---

## Step 2 — Copy the JSON config from OAC

On the same MCP Connect tab, click **Copy JSON** to copy the per-user MCP server configuration. It looks roughly like:

```json
{
  "mcpServers": {
    "oac-mcp-server": {
      "command": "node",
      "args": ["path/to/oac-mcp-connect.js"],
      "env": {
        "OAC_INSTANCE_URL": "https://<your-oac-host>",
        ...
      }
    }
  }
}
```

> Replace `path/to/oac-mcp-connect.js` with the actual path you noted in Step 1.

---

## Step 3 — Paste into your AI client's config

### Claude Desktop
1. Open `claude_desktop_config.json`:
   - **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
   - **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`
2. Merge the `oac-mcp-server` entry into the existing `mcpServers` object (if no `mcpServers` key, paste the whole snippet).
3. Save and **restart Claude Desktop**.

### Claude Code (`claude` CLI)

> ⚠️ **Terminal clients can't do interactive auth.** Claude Code's MCP client reports `elicitation = not-supported`, so the connector's default **browser login cannot complete** — the connector exits and you see `-32000: Connection closed`. You must give the connector credentials **up front**. Two non-interactive options:
> - **Basic auth** (recommended; the only option on **non-IDCS** instances, where tokens are rejected) — see [Non-interactive auth for Claude Code](#non-interactive-auth-for-claude-code-basic-auth) below.
> - **Bearer token** (IDCS instances only) — `aidp-fusion-bundle dashboard mcp-token` (separate flow).

Claude Code does **not** read `mcpServers` from `settings.json`. Use one of:
- **Project-scoped (recommended for this repo):** the bundle ships a committed `.mcp.json` at the repo root with an `oac-mcp-server` entry. After running `dashboard mcp-setup` (below) it points at the staged connector via `${HOME}/.oac-connect/oac-mcp-connect.js` — **no URL or credentials are committed** (those live in a 0600 connector config file). Restart Claude Code and approve the server when prompted.
- **Personal (any project):** `claude mcp add oac-mcp-server -- node ~/.oac-connect/oac-mcp-connect.js` (writes to your user config; the connector reads URL + auth from its config file).

---

## Non-interactive auth for Claude Code (basic auth)

On a **non-IDCS** instance, OAC issues tokens its own APIs reject, so bearer-token auth is a dead end. The connector's **basic auth** is the working path: credentials are supplied up front (the connector sends `Authorization: Basic base64(user:pass)`), so it never needs a browser. One command sets it all up:

```bash
# Reads OAC_URL / OAC_ADMIN_USER / OAC_ADMIN_PASSWORD from your env (or pass --oac-url/--user/--password)
aidp-fusion-bundle dashboard mcp-setup \
  --connector-js ~/Downloads/oac-mcp-connect/oac-mcp-connect.js
```

This:
1. writes a **0600** connector config at `~/.oac-connect/oac_mcp_connect_config.json` (URL + `basicAuth` + `headless: true`) — the connector auto-discovers it;
2. **stages** the connector to `~/.oac-connect/oac-mcp-connect.js` (a stable path, so `.mcp.json` is portable);
3. wires a **credential-free** `.mcp.json` — the connector is launched with a single arg (its own path); the URL and credentials stay only in the 0600 file, never in the committed repo.

Then **restart / reconnect Claude Code** (`/mcp` → reconnect `oac-mcp-server`) and confirm with `claude mcp list` (expect `✔ Connected`). The `--connector-js` arg is only needed the first time (to stage the connector); later runs that just rotate credentials can omit it.

> 🔐 **Least privilege.** Basic-auth credentials sit in a plaintext 0600 file on your workstation, and connector **v1.4 exposes catalog write/delete/ACL tools** governed by that user's grants. Use a scoped, query-only OAC user — not an admin — for anything beyond local testing.

> The committed `.mcp.json` carries **no** OAC URL, username, or password — only `${HOME}/.oac-connect/oac-mcp-connect.js` (Claude Code expands `${HOME}` at launch).

### Cline (VS Code extension)
1. Open `cline_mcp_settings.json` from VS Code Command Palette → "Cline: Open MCP Settings".
2. Merge under `mcpServers`.
3. Reload window.

### GitHub Copilot
1. Open Copilot Chat MCP config (Copilot → Settings → MCP).
2. Add the `oac-mcp-server` entry.
3. Reload.

---

## Step 4 — Verify the connection

Restart your AI client. In a fresh chat session, ask something like:

> *"List the OAC MCP tools available."*

Expect to see:

Tool names use **underscores** (verified live against connector v1.4.0.0.0 on 2026-06-12 — TC32). The three core query tools:

| Tool | Purpose |
|---|---|
| `oracle_analytics-discover_data` | List datasets and subject areas (⚠️ deprecated in v1.4 — prefer `find_matching_datasources` / `search_catalog`) |
| `oracle_analytics-describe_data` | Get column/measure metadata for a dataset |
| `oracle_analytics-execute_logical_sql` | Run a Logical SQL query |

> **Heads-up — v1.4 is not read-only.** Beyond the query tools above, connector v1.4 also exposes catalog **write** tools: `find_matching_datasources`, `search_catalog`, `create_catalog_folder`, `copy_catalog_item`, `move_catalog_item`, `delete_catalog_item`, `save_catalog_content`, `update_catalog_acl`, `export_workbook`. These run with the OAuth user's permissions, so scope that user to least privilege (see "Critical capability boundaries" below).

If those three appear, MCP is connected.

---

## Step 5 — First grounded question against the bundle's gold mart

Once `aidp-fusion-bundle dashboard install --target oac` has imported the workbooks (or the user has manually authored a workbook against `aidp_fusion_jdbc.fusion_catalog.gold.supplier_spend`), ask:

> *"Which vendors had over $100M in invoice spend? Show vendor_id, total_invoice_amount, and invoice_count, ordered by spend descending."*

The AI client should:
1. Call `oracle_analytics-discover_data` (or `find_matching_datasources`) to find the supplier-spend dataset
2. Call `oracle_analytics-describe_data` to get the columns
3. Construct a Logical SQL query and call `oracle_analytics-execute_logical_sql`
4. Return the answer with citations

Live-tested 2026-04-30 in [TC9](../tests/live/TC9_genai_results.md): the agent identified the top vendor `300000047507499` at $892.7M with concentration math (26.18%) and anomaly detection (`vendor_id=-10016`, stale invoice `2018-12-21`).

---

## Critical capability boundaries

OAC MCP (Preview, Nov 2025) is intentionally narrow:

> ⚠️ **Version-dependent.** The Nov-2025 *preview* was query-only. Connector **v1.4.0.0.0** (verified 2026-06-12) adds catalog **write/delete/ACL** tools — see the heads-up above the tool table. Treat the table below as the *preview* boundary; on v1.4 the "Cannot" column is enforced only by the OAuth **user's grants**, not the connector. Scope that user to least privilege.

| Can | Cannot (preview) / governed-by-user-grants (v1.4) |
|---|---|
| List/describe datasets, subject areas, columns, measures | Create or modify workbooks — *v1.4: possible via `save_catalog_content` / `create_catalog_folder` if the user is granted it* |
| Run governed Logical SQL queries | Register data sources (use OAC REST API for that — see `oac/rest/`) |
| Manage catalog items (v1.4: copy/move/delete/ACL) — **only if the OAuth user has those grants** | Run arbitrary SQL DDL (no schema changes, no inserts) |
| Auth runs as the **end user** — governance preserved | Run arbitrary SQL DDL (no schema changes, no inserts) |

For write operations (registering new connections, importing workbooks), the bundle's `dashboard install --target oac` uses **OAC REST API**, not MCP. See [oac_rest_api_setup.md](oac_rest_api_setup.md) (separate doc).

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `oac-mcp-server` not in client's tool list | Path to `oac-mcp-connect.js` wrong, or client not restarted | Verify the path; restart the AI client |
| 401 on first query | Your OAC session expired | Open OAC web UI, log in, retry |
| `oracle_analytics-execute_logical_sql` returns empty | Workbook/dataset not visible to your user, or query referenced a column that doesn't exist | Check OAC permissions; describe the dataset first to confirm column names |
| `tools/call` returns `401 Unauthorized` (tools list fine) | No completed auth session — `tools/list` works unauthenticated but data calls need credentials | **Claude Code:** run `dashboard mcp-setup` (basic auth). **Desktop:** complete the browser login, or pass a `token.json` as the connector's 3rd arg (IDCS only) |
| Claude Code: server drops with `-32000: Connection closed` on first tool call | Connector fell back to **interactive** browser auth, which Claude Code can't drive (`elicitation = not-supported`) | Supply credentials up front: `dashboard mcp-setup` for basic auth (works on non-IDCS pods) |
| Basic auth: `401` on every call | Wrong credentials, or the OAC user lacks access | Re-check `OAC_ADMIN_USER`/`OAC_ADMIN_PASSWORD`; confirm the user can log into the OAC web UI |
| MCP server crashes on start | Node.js < 18 | Upgrade Node.js |
| `OAC_INSTANCE_URL` mismatch | Copied JSON pointed at a different OAC instance | Re-fetch JSON from the right OAC's MCP Connect tab |

---

## What this enables in the Fusion bundle pitch

This closes the bundle's pdf1-aligned end-to-end story:

```
Fusion BICC PVO  →  AIDP bronze  →  AIDP silver  →  AIDP gold
                                                       │
                                            JDBC ◀────┘
                                              │
                                              ▼
                                          OAC workbook
                                              │
                                       MCP ◀──┘
                                          │
                                          ▼
                            Claude / Cline / Copilot
                            "What's our AR aging?"
                            "Which vendors had >$100M Q1 spend?"
```

End-user value: **non-technical analysts can ask natural-language questions against the bundle's curated gold marts**, with answers computed by OAC over real Fusion data — no SQL knowledge required, governance preserved by OAC's row-level security.

---

## References

- [OAC MCP Server (Preview) — overview](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acsdv/access-oracle-analytics-cloud-mcp-server-preview.html)
- [OAC MCP — Tools available](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acsdv/tools-available-oracle-analytics-cloud-mcp-server-preview.html)
- [OAC MCP — Add to AI client](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acsdv/add-oracle-analytics-cloud-mcp-server-your-ai-client-preview.html)
- [Bundle TC9 results](../tests/live/TC9_genai_results.md) — proof that Spark SQL `ai_generate()` works against the same gold mart (alternative path; Spark-native vs OAC-mediated)

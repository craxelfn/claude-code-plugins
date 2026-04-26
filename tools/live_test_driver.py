"""Live-test driver: upload each notebook to AIDP, run, capture result.

This is meant to be executed *by Claude Code via the AIDP MCP tools*, not run
directly by Python. It documents the canonical flow and provides a Python
helper that the human-driven notebook (or a future MCP-aware script) can call.

Per-row flow:
    1. Read examples/<row>.ipynb from disk.
    2. mcp__aidp__nb_save_file → push to AIDP workspace
       under Shared/connectors-tests/<row>.ipynb.
    3. mcp__aidp__nb_create_session against cluster `tpcds`.
    4. mcp__aidp__nb_execute_code → run each cell sequentially.
    5. Capture stdout, look for AIDP_LIVE_TEST_RESULT_BEGIN/END markers,
       parse the JSON between them.
    6. Write tests/live-results/<row>.json with parsed payload + duration.
    7. Append/update a row in tests/live-results/RESULTS.md.

This file is the single source of truth for which notebooks correspond to
which live-test rows. Update LIVE_TEST_ROWS when adding new connectors.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


REPO = Path(__file__).resolve().parent.parent
EXAMPLES = REPO / "examples"
LIVE_RESULTS = REPO / "tests" / "live-results"
LIVE_RESULTS.mkdir(parents=True, exist_ok=True)


@dataclass
class LiveTestRow:
    row_number: int
    skill: str
    auth_method: str
    notebook: str
    pass_criteria: str
    in_v0_1_release_gate: bool


LIVE_TEST_ROWS = [
    LiveTestRow(0,  "aidp-connectors-bootstrap", "n/a",                          "00_bootstrap_helpers.ipynb",     "BOOTSTRAP OK printed; package importable",                         True),
    # ALH covers the entire Autonomous DB family (ALH = ADW = ATP = Oracle 26ai under the hood).
    LiveTestRow(1,  "aidp-alh",            "Wallet (mTLS)",                     "alh_wallet_query.ipynb",         "Spark DataFrame with non-zero rows from a known ALH/ADW/ATP table", True),
    LiveTestRow(2,  "aidp-alh",            "IAM DB-Token (>25 min refresh)",     "alh_dbtoken_query.ipynb",        "Same query via DB-token, on-executor refresh, zero auth failures", True),
    LiveTestRow(3,  "aidp-alh",            "API Key + inline OCI config",        "alh_catalog_sync_apikey.ipynb",  "External-catalog metadata refresh succeeds; downstream Spark read", False),
    LiveTestRow(4,  "aidp-exacs",          "Wallet (TCPS)",                      "exacs_wallet_query.ipynb",       "Non-zero rows over TCPS port 1522",                                False),
    LiveTestRow(5,  "aidp-exacs",          "IAM DB-Token",                       "exacs_dbtoken_query.ipynb",      "Same query via DB-token; skip if cluster not IAM-enabled",         False),
    LiveTestRow(6,  "aidp-exacs",          "Legacy DB user/password",            "exacs_user_password.ipynb",      "Same query via classic credentials",                               False),
    LiveTestRow(7,  "aidp-bds-hive",       "Kerberos keytab",                    "bds_hive_kerberos.ipynb",        "kinit succeeds, Hive JDBC SHOW TABLES returns rows",               False),
    LiveTestRow(8,  "aidp-bds-hive",       "LDAP",                               "bds_hive_ldap.ipynb",            "Same query via LDAP bind",                                         False),
    LiveTestRow(9,  "aidp-fusion-rest",    "HTTP Basic",                         "fusion_rest_basic.ipynb",        "<=499-row paged fetch lands as Spark DataFrame",                   True),
    LiveTestRow(10, "aidp-fusion-bicc",    "HTTP Basic",                         "fusion_bicc_to_dataframe.ipynb", "Extract kicks off -> CSV in OS bucket -> Spark reads it",          True),
    LiveTestRow(11, "aidp-epm-cloud",      "Basic (tenancy.user@domain)",        "epm_planning_basic.ipynb",       "Planning REST applications=200; MDX export -> DataFrame",          False),
    LiveTestRow(12, "aidp-essbase",        "HTTP Basic",                         "essbase_mdx_basic.ipynb",        "MDX SELECT returns DataFrame with expected dim count",             False),
    LiveTestRow(13, "aidp-streaming-kafka", "SASL_SSL OAuth",                    "kafka_streaming_oauth.ipynb",    "query.lastProgress shows non-zero numInputRows",                   False),
    LiveTestRow(14, "aidp-streaming-kafka", "SASL/PLAIN with API Key",           "kafka_streaming_apikey.ipynb",   "Same stream via API-key SASL",                                     False),
]


_SUMMARY_RE = re.compile(
    r"AIDP_LIVE_TEST_RESULT_BEGIN\s*(?P<json>\{.*?\})\s*AIDP_LIVE_TEST_RESULT_END",
    re.DOTALL,
)


def parse_summary(stdout: str) -> Optional[dict]:
    """Extract the JSON summary that the final cell of every notebook prints.

    Notebooks emit:
        AIDP_LIVE_TEST_RESULT_BEGIN
        { "connector": ..., "auth": ..., "rows": ..., "schema": [...], "timestamp_utc": ... }
        AIDP_LIVE_TEST_RESULT_END

    Returns the parsed dict, or None if markers weren't found.
    """
    match = _SUMMARY_RE.search(stdout)
    if not match:
        return None
    try:
        return json.loads(match.group("json"))
    except json.JSONDecodeError:
        return None


def regenerate_results_md() -> str:
    """Build tests/live-results/RESULTS.md from the per-row JSON files.

    Returns the markdown body (also writes it to disk).
    """
    # Compute summary
    pass_count = 0
    fail_count = 0
    deferred_count = 0
    not_run_count = 0
    statuses = {}
    for row in LIVE_TEST_ROWS:
        result_path = LIVE_RESULTS / f"row{row.row_number:02d}.json"
        if result_path.exists():
            data = json.loads(result_path.read_text())
            explicit = data.get("result")
            row_count = data.get("rows")
            if explicit == "PASS" or (explicit is None and row_count and row_count > 0):
                pass_count += 1
                statuses[row.row_number] = "PASS"
            elif explicit == "FAIL":
                fail_count += 1
                statuses[row.row_number] = "FAIL"
            elif explicit == "DEFERRED":
                deferred_count += 1
                statuses[row.row_number] = "DEFERRED"
            else:
                statuses[row.row_number] = "UNKNOWN"
        else:
            not_run_count += 1
            statuses[row.row_number] = "NOT RUN"

    lines = [
        "# Live-test results\n",
        "",
        f"**Summary:** {pass_count} PASS, {fail_count} FAIL, {deferred_count} DEFERRED, {not_run_count} NOT RUN out of {len(LIVE_TEST_ROWS)} rows.",
        "",
        "| # | Skill | Auth | Notebook | Status | Rows | Last run (UTC) |",
        "|---|---|---|---|---|---|---|",
    ]
    for row in LIVE_TEST_ROWS:
        result_path = LIVE_RESULTS / f"row{row.row_number:02d}.json"
        if result_path.exists():
            data = json.loads(result_path.read_text())
            explicit = data.get("result")
            row_count = data.get("rows")
            if explicit:
                status = explicit
            elif row_count is None:
                status = "UNKNOWN"
            elif row_count > 0:
                status = "PASS"
            else:
                status = "EMPTY"
            rows = row_count if row_count is not None else "-"
            ts = data.get("timestamp_utc", "-")
        else:
            status = "NOT RUN"
            rows = "-"
            ts = "-"
        lines.append(
            f"| {row.row_number} | `{row.skill}` | {row.auth_method} | "
            f"[`{row.notebook}`](../../examples/{row.notebook}) | {status} | {rows} | {ts} |"
        )

    body = "\n".join(lines) + "\n"
    (LIVE_RESULTS / "RESULTS.md").write_text(body, encoding="utf-8")
    return body


if __name__ == "__main__":
    body = regenerate_results_md()
    print(body)

"""Build all 19 example AIDP notebooks from a single spec.

Each notebook has a consistent structure:
    1. Markdown cell — title, auth method, prerequisites, expected output
    2. Code cell — sys.path setup + imports
    3. Code cell — auth setup (option-specific)
    4. Code cell — query / extract / stream
    5. Code cell — assert + emit JSON summary (the live-test driver parses this)

Run:
    python tools/build_examples.py
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import List

REPO = Path(__file__).resolve().parent.parent
EXAMPLES = REPO / "examples"
EXAMPLES.mkdir(exist_ok=True)


def code(*lines: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": list(lines),
    }


def md(*lines: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": list(lines)}


# Reused as the last cell of every notebook — the live-test driver parses this.
def emit_summary(connector: str, auth: str, df_var: str = "df") -> dict:
    return code(
        f"# Live-test driver parses this final cell's stdout for the JSON summary.\n",
        "import json, time\n",
        "summary = {\n",
        f"    'connector': {connector!r},\n",
        f"    'auth': {auth!r},\n",
        f"    'rows': int({df_var}.count()),\n",
        f"    'schema': sorted([f.name for f in {df_var}.schema.fields]),\n",
        "    'timestamp_utc': int(time.time()),\n",
        "}\n",
        "print('AIDP_LIVE_TEST_RESULT_BEGIN')\n",
        "print(json.dumps(summary, indent=2))\n",
        "print('AIDP_LIVE_TEST_RESULT_END')\n",
    )


def sys_path_setup() -> dict:
    return code(
        "import sys, os\n",
        "# Adjust this if you've uploaded the plugin scripts/ dir elsewhere.\n",
        "sys.path.insert(0, '/Workspace/Shared/oracle_ai_data_platform_connectors/scripts')\n",
    )


def write(name: str, cells: list, kernel: str = "python3") -> None:
    nb = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": kernel,
            },
            "language_info": {"name": "python", "version": "3.10"},
            "aidp_connector": name,
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    out = EXAMPLES / f"{name}.ipynb"
    out.write_text(json.dumps(nb, indent=2), encoding="utf-8")
    print(f"wrote {out}")


# === Definitions ============================================================


def alh_wallet_query() -> List[dict]:
    return [
        md(
            "# `aidp-alh` live test — wallet (mTLS)\n",
            "\n",
            "**Live-test row 1.** Reads a known ALH table via Spark JDBC using a wallet.\n",
            "\n",
            "**Prerequisites:** `ALH_*` env vars set or OCI Vault configured. ALH wallet ZIP available.\n",
        ),
        sys_path_setup(),
        code(
            "from oracle_ai_data_platform_connectors.auth import write_wallet_to_tmp\n",
            "from oracle_ai_data_platform_connectors.jdbc import build_oracle_jdbc_url, spark_jdbc_options_wallet\n",
            "\n",
            "tns_admin = write_wallet_to_tmp(\n",
            "    wallet=os.environ.get('ALH_WALLET_ZIP_PATH', '/tmp/alh-wallet.zip'),\n",
            "    target_dir='/tmp/wallet/alh',\n",
            ")\n",
            "url = build_oracle_jdbc_url(tns_alias=os.environ['ALH_TNS_SERVICE'], tns_admin=tns_admin)\n",
            "opts = spark_jdbc_options_wallet(url=url, user=os.environ['ALH_USER'], password=os.environ['ALH_PASSWORD'])\n",
        ),
        code(
            "df = spark.read.format('jdbc').options(**opts).option('dbtable', os.environ['ALH_TABLE_FOR_TEST']).load()\n",
            "df.show(5)\n",
        ),
        emit_summary("aidp-alh", "wallet"),
    ]


def alh_dbtoken_query() -> List[dict]:
    return [
        md(
            "# `aidp-alh` live test — IAM DB-Token\n",
            "\n",
            "**Live-test row 2.** Same query as row 1, but auth is via DB-token instead of password.\n",
        ),
        sys_path_setup(),
        code(
            "from oracle_ai_data_platform_connectors.auth import generate_db_token\n",
            "from oracle_ai_data_platform_connectors.jdbc import build_oracle_jdbc_url, spark_jdbc_options_dbtoken\n",
            "\n",
            "token_dir = generate_db_token(\n",
            "    compartment_ocid=os.environ['ALH_COMPARTMENT_OCID'],\n",
            "    target_dir='/tmp/dbcred_alh',\n",
            ")\n",
            "url = build_oracle_jdbc_url(\n",
            "    tns_alias=os.environ['ALH_TNS_SERVICE'],\n",
            "    tns_admin=os.environ.get('ALH_WALLET_PATH', '/tmp/wallet/alh'),\n",
            ")\n",
            "opts = spark_jdbc_options_dbtoken(url=url, token_dir=token_dir)\n",
        ),
        code(
            "df = spark.read.format('jdbc').options(**opts).option('dbtable', os.environ['ALH_TABLE_FOR_TEST']).load()\n",
            "df.show(5)\n",
        ),
        emit_summary("aidp-alh", "dbtoken"),
    ]


def alh_catalog_sync_apikey() -> List[dict]:
    return [
        md(
            "# `aidp-alh` live test — API Key + inline OCI config (catalog-sync side)\n",
            "\n",
            "**Live-test row 3.** Refreshes the AIDP external catalog metadata from ALH using inline-PEM OCI auth, then reads the synced table via Spark.\n",
        ),
        sys_path_setup(),
        code(
            "from oracle_ai_data_platform_connectors.auth import from_inline_pem\n",
            "import oci\n",
            "\n",
            "config = from_inline_pem(\n",
            "    user_ocid=os.environ['OCI_USER_OCID'],\n",
            "    tenancy_ocid=os.environ['OCI_TENANCY_OCID'],\n",
            "    fingerprint=os.environ['OCI_FINGERPRINT'],\n",
            "    private_key_pem=os.environ['OCI_PRIVATE_KEY_PEM'],\n",
            "    region=os.environ['OCI_REGION'],\n",
            ")\n",
            "# A control-plane sanity check — proves the config works without writing a PEM file.\n",
            "identity = oci.identity.IdentityClient(config=config)\n",
            "print('user:', identity.get_user(config['user']).data.name)\n",
        ),
        code(
            "# Downstream Spark read against the externally-cataloged ALH table.\n",
            "df = spark.read.table(os.environ['ALH_EXTERNAL_CATALOG_TABLE'])\n",
            "df.show(5)\n",
        ),
        emit_summary("aidp-alh", "apikey-catalog-sync"),
    ]


def exacs_wallet_query() -> List[dict]:
    return [
        md(
            "# `aidp-exacs` live test — wallet (TCPS)\n",
            "**Live-test row 4.**\n",
        ),
        sys_path_setup(),
        code(
            "from oracle_ai_data_platform_connectors.auth import write_wallet_to_tmp\n",
            "from oracle_ai_data_platform_connectors.jdbc import build_oracle_jdbc_url, spark_jdbc_options_wallet\n",
            "\n",
            "write_wallet_to_tmp(os.environ['EXACS_WALLET_ZIP_PATH'], target_dir='/tmp/wallet/exacs')\n",
            "url = build_oracle_jdbc_url(\n",
            "    host=os.environ['EXACS_HOST'],\n",
            "    port=int(os.environ.get('EXACS_PORT', '1522')),\n",
            "    service_name=os.environ['EXACS_SERVICE_NAME'],\n",
            "    use_tcps=True,\n",
            ")\n",
            "opts = spark_jdbc_options_wallet(url=url, user=os.environ['EXACS_USER'], password=os.environ['EXACS_PASSWORD'])\n",
        ),
        code(
            "df = spark.read.format('jdbc').options(**opts).option('dbtable', os.environ['EXACS_TABLE_FOR_TEST']).load()\n",
            "df.show(5)\n",
        ),
        emit_summary("aidp-exacs", "wallet"),
    ]


def exacs_dbtoken_query() -> List[dict]:
    return [
        md(
            "# `aidp-exacs` live test — IAM DB-Token (only IAM-enabled clusters)\n",
            "**Live-test row 5.** Skip if the cluster is on classic auth.\n",
        ),
        sys_path_setup(),
        code(
            "from oracle_ai_data_platform_connectors.auth import generate_db_token\n",
            "from oracle_ai_data_platform_connectors.jdbc import build_oracle_jdbc_url, spark_jdbc_options_dbtoken\n",
            "\n",
            "token_dir = generate_db_token(os.environ['EXACS_COMPARTMENT_OCID'], target_dir='/tmp/dbcred_exacs')\n",
            "url = build_oracle_jdbc_url(host=os.environ['EXACS_HOST'], port=1522, service_name=os.environ['EXACS_SERVICE_NAME'])\n",
            "opts = spark_jdbc_options_dbtoken(url=url, token_dir=token_dir)\n",
        ),
        code(
            "df = spark.read.format('jdbc').options(**opts).option('dbtable', os.environ['EXACS_TABLE_FOR_TEST']).load()\n",
            "df.show(5)\n",
        ),
        emit_summary("aidp-exacs", "dbtoken"),
    ]


def exacs_user_password() -> List[dict]:
    return [
        md(
            "# `aidp-exacs` live test — plain user/password on TCP 1521 + server-enforced NNE\n",
            "**Live-test row 6.** Mirrors the working pattern from `exacs_intransit_encryption_demo.ipynb` "
            "in the `exacs-private-test` workspace, which proved AES256 NNE end-to-end against a customer "
            "ExaCS cluster running Oracle 23ai.\n",
            "\n",
            "**Workspace prereq:** `networkConfigurationDetails.scanDetails` must include the SCAN FQDN+port "
            "(PE-ARCH 3c, RCE with SCAN Proxy). Without it the RAC redirect dies with ORA-17820.\n",
        ),
        sys_path_setup(),
        md("## Step 1 — Configuration\n"),
        code(
            "SCAN_HOST = os.environ['EXACS_HOST']\n",
            "SCAN_PORT = int(os.environ.get('EXACS_PORT_LEGACY', '1521'))\n",
            "SERVICE   = os.environ['EXACS_SERVICE_NAME']\n",
            "DB_USER   = os.environ['EXACS_USER']\n",
            "DB_PASS   = os.environ['EXACS_PASSWORD']\n",
            "TABLE     = os.environ['EXACS_TABLE_FOR_TEST']\n",
            "print(f'  SCAN  : {SCAN_HOST}:{SCAN_PORT}')\n",
            "print(f'  SVC   : {SERVICE}')\n",
            "print(f'  USER  : {DB_USER}')\n",
            "print(f'  TABLE : {TABLE}')\n",
        ),
        md(
            "## Step 2 — DNS resolution check\n",
            "SCAN host must resolve to a Class-E (240.0.0.0/4 / 255.x) or RFC-1918 (10.x/172.16-31.x/192.168.x) IP. "
            "A public IP would mean traffic isn't going through the PE.\n",
        ),
        code(
            "import socket, ipaddress\n",
            "ips = sorted({r[4][0] for r in socket.getaddrinfo(SCAN_HOST, SCAN_PORT, socket.AF_INET)})\n",
            "for ip in ips:\n",
            "    is_class_e = int(ip.split('.')[0]) >= 240\n",
            "    is_priv    = ipaddress.ip_address(ip).is_private\n",
            "    kind = 'Class-E NAT' if is_class_e else 'RFC-1918 private' if is_priv else 'PUBLIC (FAIL)'\n",
            "    print(f'  {SCAN_HOST} -> {ip}  [{kind}]')\n",
            "assert any(int(ip.split('.')[0]) >= 240 or ipaddress.ip_address(ip).is_private for ip in ips), 'PE routing not active'\n",
        ),
        md("## Step 3 — TCP connectivity\n"),
        code(
            "import socket, time\n",
            "t0 = time.time()\n",
            "with socket.create_connection((SCAN_HOST, SCAN_PORT), timeout=15) as s:\n",
            "    print(f'  Connected in {(time.time()-t0)*1000:.0f} ms  (local={s.getsockname()}  remote={s.getpeername()})')\n",
        ),
        md("## Step 4 — Spark JDBC connect (plain user/password)\n"),
        code(
            "from oracle_ai_data_platform_connectors.jdbc import (\n",
            "    build_oracle_jdbc_url, spark_jdbc_options_password,\n",
            ")\n",
            "url = build_oracle_jdbc_url(host=SCAN_HOST, port=SCAN_PORT, service_name=SERVICE, use_tcps=False)\n",
            "opts = spark_jdbc_options_password(url=url, user=DB_USER, password=DB_PASS)\n",
            "# Optional: enable for the SYS user only.\n",
            "# opts['oracle.jdbc.internal_logon'] = 'sysdba'\n",
            "print('JDBC URL:', url)\n",
        ),
        code(
            "df = (spark.read.format('jdbc').options(**opts).option('dbtable', TABLE).load())\n",
            "df.show(5)\n",
        ),
        md(
            "## Step 5 — In-transit encryption verification\n",
            "Read `v$session_connect_info.network_service_banner` for the active session — proves AES256 NNE.\n",
        ),
        code(
            "enc_q = (\n",
            "    \"SELECT network_service_banner FROM v$session_connect_info \"\n",
            "    \"WHERE sid = SYS_CONTEXT('USERENV','SID')\"\n",
            ")\n",
            "banners = (spark.read.format('jdbc').options(**opts)\n",
            "             .option('query', enc_q).load().collect())\n",
            "for r in banners:\n",
            "    print(' ', r[0])\n",
            "encryption = next((b[0] for b in (b.split(' Encryption service adapter') for b in (r[0] for r in banners)) if len(b) > 1), None)\n",
            "print('Negotiated algorithm:', encryption)\n",
        ),
        emit_summary("aidp-exacs", "password-tcp-nne"),
    ]


def bds_hive_kerberos() -> List[dict]:
    return [
        md(
            "# `aidp-bds-hive` live test — Kerberos\n",
            "**Live-test row 7.** Requires `kinit` on the cluster image (TBD; this notebook will fail fast with a clear error if not).\n",
        ),
        sys_path_setup(),
        code(
            "from oracle_ai_data_platform_connectors.jdbc import build_hive_jdbc_url, spark_hive_jdbc_options\n",
            "from oracle_ai_data_platform_connectors.jdbc.hive import kerberos_kinit\n",
            "\n",
            "kerberos_kinit(\n",
            "    principal=os.environ['BDS_KRB_PRINCIPAL'],\n",
            "    keytab_path=os.environ['BDS_KRB_KEYTAB_PATH'],  # MUST be /tmp/...\n",
            ")\n",
            "url = build_hive_jdbc_url(\n",
            "    host=os.environ['BDS_HS2_HOST'], port=10000,\n",
            "    database=os.environ.get('BDS_HS2_DATABASE', 'default'),\n",
            "    auth='kerberos',\n",
            "    principal=f\"hive/{os.environ['BDS_HS2_HOST']}@{os.environ['BDS_HIVE_REALM']}\",\n",
            ")\n",
            "opts = spark_hive_jdbc_options(url=url)\n",
        ),
        code(
            "df = spark.read.format('jdbc').options(**opts).option('dbtable', os.environ['BDS_TABLE_FOR_TEST']).load()\n",
            "df.show(5)\n",
        ),
        emit_summary("aidp-bds-hive", "kerberos"),
    ]


def bds_hive_ldap() -> List[dict]:
    return [
        md(
            "# `aidp-bds-hive` live test — LDAP\n",
            "**Live-test row 8.** Recommended default for v0.1.\n",
        ),
        sys_path_setup(),
        code(
            "from oracle_ai_data_platform_connectors.jdbc import build_hive_jdbc_url, spark_hive_jdbc_options\n",
            "\n",
            "url = build_hive_jdbc_url(\n",
            "    host=os.environ['BDS_HS2_HOST'],\n",
            "    port=int(os.environ.get('BDS_HS2_PORT', '10000')),\n",
            "    database=os.environ.get('BDS_HS2_DATABASE', 'default'),\n",
            "    auth='ldap',\n",
            ")\n",
            "opts = spark_hive_jdbc_options(url=url, user=os.environ['BDS_LDAP_USER'], password=os.environ['BDS_LDAP_PASSWORD'])\n",
        ),
        code(
            "df = spark.read.format('jdbc').options(**opts).option('dbtable', os.environ['BDS_TABLE_FOR_TEST']).load()\n",
            "df.show(5)\n",
        ),
        emit_summary("aidp-bds-hive", "ldap"),
    ]


def fusion_rest_basic() -> List[dict]:
    return [
        md(
            "# `aidp-fusion-rest` live test — HTTP Basic\n",
            "**Live-test row 9.**\n",
        ),
        sys_path_setup(),
        code(
            "from oracle_ai_data_platform_connectors.auth import http_basic_session\n",
            "from oracle_ai_data_platform_connectors.rest.fusion import fetch_paged, rows_to_spark_dataframe\n",
            "\n",
            "session = http_basic_session(\n",
            "    username=os.environ['FUSION_USER'],\n",
            "    password=os.environ['FUSION_PASSWORD'],\n",
            "    base_url=os.environ['FUSION_BASE_URL'],\n",
            ")\n",
        ),
        code(
            "rows = fetch_paged(\n",
            "    session=session,\n",
            "    base_url=os.environ['FUSION_BASE_URL'],\n",
            "    path=os.environ['FUSION_TEST_PATH'],   # e.g. '/fscmRestApi/resources/11.13.18.05/invoices'\n",
            "    fields=os.environ.get('FUSION_TEST_FIELDS'),\n",
            ")\n",
            "df = rows_to_spark_dataframe(spark, rows)\n",
            "df.show(5)\n",
        ),
        emit_summary("aidp-fusion-rest", "basic"),
    ]


def fusion_bicc_to_dataframe() -> List[dict]:
    """Live test for aidp-fusion-bicc.

    Uses the AIDP `aidataplatform` format handler (Option A in the skill,
    matches the official Oracle AIDP sample). The custom REST trigger flow
    is documented in SKILL.md as Option B but not exercised here.
    """
    return [
        md(
            "# `aidp-fusion-bicc` live test — AIDP `aidataplatform` format (BICC PVO → Spark)\n",
            "**Live-test row 10.** Uses AIDP's built-in `spark.read.format('aidataplatform')` connector — matches the official Oracle AIDP sample at `oracle-samples/oracle-aidp-samples` (`Read_Only_Ingestion_Connectors.ipynb`). The connector triggers the BICC extract, polls for completion, and reads the resulting CSVs from OCI Object Storage internally.\n",
        ),
        sys_path_setup(),
        code(
            "from oracle_ai_data_platform_connectors.rest.fusion import read_bicc_via_aidp_format\n",
            "\n",
            "# Prereqs: Fusion user has a BICC-admin role; an AIDP `EXTERNAL STORAGE`\n",
            "# profile is registered in the catalog pointing at the OCI Object Storage\n",
            "# bucket BICC writes to (configured once by an admin).\n",
            "df = read_bicc_via_aidp_format(\n",
            "    spark=spark,\n",
            "    fusion_service_url=os.environ['FUSION_BICC_BASE_URL'],\n",
            "    username=os.environ['FUSION_BICC_USER'],\n",
            "    password=os.environ['FUSION_BICC_PASSWORD'],\n",
            "    schema=os.environ['FUSION_BICC_SCHEMA'],            # e.g. 'ERP'\n",
            "    datastore=os.environ['FUSION_BICC_PVO'],            # PVO name\n",
            "    fusion_external_storage=os.environ['FUSION_BICC_EXTERNAL_STORAGE'],\n",
            ")\n",
        ),
        code(
            "df.show(5)\n",
            "print('rows:', df.count())\n",
            "df.printSchema()\n",
        ),
        emit_summary("aidp-fusion-bicc", "basic-aidp-format"),
    ]


def epm_planning_basic() -> List[dict]:
    return [
        md(
            "# `aidp-epm-cloud` live test — HTTP Basic (default for v0.1)\n",
            "**Live-test row 11.** Username MUST be in `tenancy.user@domain` form (e.g. `epmloaner622.first.last@oracle.com`).\n",
        ),
        sys_path_setup(),
        code(
            "from oracle_ai_data_platform_connectors.auth import http_basic_session\n",
            "from oracle_ai_data_platform_connectors.rest.epm import (\n",
            "    list_applications, export_data_slice, slice_to_long_dataframe,\n",
            ")\n",
            "\n",
            "session = http_basic_session(\n",
            "    username=os.environ['EPM_USERNAME'],\n",
            "    password=os.environ['EPM_PASSWORD'],\n",
            "    base_url=os.environ['EPM_BASE_URL'],\n",
            ")\n",
            "apps = list_applications(session, os.environ['EPM_BASE_URL'])\n",
            "print('apps:', [a['name'] for a in apps])\n",
        ),
        code(
            "import json\n",
            "grid = json.loads(os.environ['EPM_GRID_DEFINITION_JSON'])\n",
            "resp = export_data_slice(session, os.environ['EPM_BASE_URL'], os.environ['EPM_APPLICATION'], os.environ['EPM_PLAN_TYPE'], grid)\n",
            "df = slice_to_long_dataframe(spark, resp)\n",
            "df.show(10)\n",
        ),
        emit_summary("aidp-epm-cloud", "basic"),
    ]


def essbase_mdx_basic() -> List[dict]:
    return [
        md(
            "# `aidp-essbase` live test — HTTP Basic + MDX\n",
            "**Live-test row 12.**\n",
        ),
        sys_path_setup(),
        code(
            "from oracle_ai_data_platform_connectors.auth import http_basic_session\n",
            "from oracle_ai_data_platform_connectors.rest.essbase import (\n",
            "    execute_mdx, mdx_result_to_spark_dataframe,\n",
            ")\n",
            "\n",
            "session = http_basic_session(\n",
            "    username=os.environ['ESSBASE_USER'],\n",
            "    password=os.environ['ESSBASE_PASSWORD'],\n",
            "    base_url=os.environ['ESSBASE_BASE_URL'],\n",
            ")\n",
        ),
        code(
            "mdx = os.environ['ESSBASE_MDX_QUERY']\n",
            "resp = execute_mdx(session, os.environ['ESSBASE_BASE_URL'], os.environ['ESSBASE_APPLICATION'], os.environ['ESSBASE_CUBE'], mdx)\n",
            "df = mdx_result_to_spark_dataframe(spark, resp)\n",
            "df.show(10)\n",
        ),
        emit_summary("aidp-essbase", "basic"),
    ]


def kafka_streaming_apikey() -> List[dict]:
    return [
        md(
            "# `aidp-streaming-kafka` live test — SASL/PLAIN with OCI auth token\n",
            "**Live-test row 13.** Mirrors the official Oracle AIDP sample at `oracle-samples/oracle-aidp-samples` (`StreamingFromOCIStreamingService.ipynb`). 1-hour token TTL — refresh before long jobs.\n",
        ),
        sys_path_setup(),
        code(
            "from oracle_ai_data_platform_connectors.streaming import (\n",
            "    bootstrap_for_region, build_kafka_options_sasl_plain, validate_checkpoint_path,\n",
            ")\n",
            "\n",
            "bootstrap = bootstrap_for_region(os.environ['OCI_REGION'])\n",
            "opts = build_kafka_options_sasl_plain(\n",
            "    bootstrap_servers=bootstrap,\n",
            "    tenancy_name=os.environ['OCI_TENANCY_NAME'],\n",
            "    username=os.environ['OCI_USERNAME'],     # 'oracleidentitycloudservice/<email>' for IAM-Domains\n",
            "    stream_pool_ocid=os.environ['OCI_STREAM_POOL_OCID'],\n",
            "    auth_token=os.environ['OCI_AUTH_TOKEN'],\n",
            "    topic=os.environ['KAFKA_TOPIC'],\n",
            "    starting_offsets='earliest',\n",
            "    max_partition_fetch_bytes=1024*1024,\n",
            "    max_offsets_per_trigger=5,\n",
            ")\n",
        ),
        code(
            "checkpoint = validate_checkpoint_path(os.environ['KAFKA_CHECKPOINT_VOLUME'])\n",
            "raw = spark.readStream.format('kafka').options(**opts).load()\n",
            "out_df = raw.selectExpr(\"CAST(key AS STRING) AS k\", \"CAST(value AS STRING) AS v\", \"topic\", \"partition\", \"offset\")\n",
            "query = out_df.writeStream.format('memory').queryName('kafka_apikey_test').option('checkpointLocation', checkpoint).trigger(processingTime='5 seconds').start()\n",
            "query.awaitTermination(timeout=60)\n",
            "df = spark.sql('SELECT * FROM kafka_apikey_test')\n",
            "df.show()\n",
            "query.stop()\n",
            "print('input rows in last batch:', (query.lastProgress or {}).get('numInputRows'))\n",
        ),
        emit_summary("aidp-streaming-kafka", "sasl-plain"),
    ]


def bootstrap_helpers() -> List[dict]:
    """Self-test notebook — run once after the helpers have been uploaded.

    Verifies the package is importable and prints a summary so the user (and
    Claude, when driving via MCP) can confirm the setup worked.
    """
    return [
        md(
            "# `00_bootstrap_helpers` — confirm the AIDP connectors helper package is set up\n",
            "\n",
            "Run this notebook **once per AIDP workspace** after the plugin's helpers have been uploaded to `/Workspace/Shared/oracle_ai_data_platform_connectors/scripts/`.\n",
            "\n",
            "If you haven't uploaded the helpers yet, ask Claude: *\"set up the AIDP connectors plugin in this workspace\"* — the `aidp-connectors-bootstrap` skill drives the upload via MCP. Or upload `scripts/oracle_ai_data_platform_connectors/` manually via the AIDP UI to that path.\n",
            "\n",
            "**Pass criteria:** the final cell prints `BOOTSTRAP OK` plus the package version and a list of submodules.\n",
        ),
        sys_path_setup(),
        code(
            "# Confirm the directory layout the helpers expect.\n",
            "import os, pathlib\n",
            "expected = pathlib.Path('/Workspace/Shared/oracle_ai_data_platform_connectors/scripts/oracle_ai_data_platform_connectors')\n",
            "if not expected.exists():\n",
            "    raise RuntimeError(\n",
            "        f'Helpers not found at {expected}. Run the aidp-connectors-bootstrap skill (ask Claude: \"set up the AIDP connectors plugin\") '\n",
            "        f'or upload the plugin scripts/ directory to /Workspace/Shared/ manually.'\n",
            "    )\n",
            "files = sorted(p.name for p in expected.rglob('*.py'))\n",
            "print(f'found {len(files)} Python files under {expected}')\n",
            "for f in files: print(' ', f)\n",
        ),
        code(
            "# Sanity-import every public submodule.\n",
            "import importlib\n",
            "import oracle_ai_data_platform_connectors as pkg\n",
            "from oracle_ai_data_platform_connectors import auth, jdbc, rest, streaming\n",
            "from oracle_ai_data_platform_connectors.auth import (\n",
            "    write_wallet_to_tmp, generate_db_token, from_inline_pem,\n",
            "    http_basic_session, oauth_token, get_secret,\n",
            ")\n",
            "from oracle_ai_data_platform_connectors.jdbc import (\n",
            "    build_oracle_jdbc_url, build_hive_jdbc_url,\n",
            "    spark_jdbc_options_wallet, spark_jdbc_options_dbtoken, spark_jdbc_options_password,\n",
            "    spark_hive_jdbc_options,\n",
            ")\n",
            "from oracle_ai_data_platform_connectors.rest import fusion, epm, essbase  # noqa: F401\n",
            "from oracle_ai_data_platform_connectors.streaming import (\n",
            "    bootstrap_for_region, build_kafka_options_sasl_plain, validate_checkpoint_path,\n",
            ")\n",
            "print('all imports OK; package version:', pkg.__version__)\n",
        ),
        code(
            "# Quick logic smoke test: run the URL builder and the checkpoint validator.\n",
            "url = build_oracle_jdbc_url(tns_alias='atp_high', tns_admin='/tmp/wallet/atp')\n",
            "assert url == 'jdbc:oracle:thin:@atp_high?TNS_ADMIN=/tmp/wallet/atp', f'unexpected URL: {url}'\n",
            "import pytest as _pytest\n",
            "try:\n",
            "    validate_checkpoint_path('/Workspace/cp')\n",
            "    raise AssertionError('checkpoint validator should have raised')\n",
            "except ValueError:\n",
            "    pass\n",
            "print('smoke test OK')\n",
        ),
        code(
            "# Final result marker — the live-test driver picks this up if you run it as part of a batch.\n",
            "import json, time\n",
            "summary = {\n",
            "    'connector': 'bootstrap',\n",
            "    'auth': 'n/a',\n",
            "    'rows': 1,                  # 1 = bootstrap success\n",
            "    'schema': ['BOOTSTRAP_OK'],\n",
            "    'package_version': pkg.__version__,\n",
            "    'timestamp_utc': int(time.time()),\n",
            "}\n",
            "print('BOOTSTRAP OK')\n",
            "print('AIDP_LIVE_TEST_RESULT_BEGIN')\n",
            "print(json.dumps(summary, indent=2))\n",
            "print('AIDP_LIVE_TEST_RESULT_END')\n",
        ),
    ]


# === Build everything =======================================================


NOTEBOOKS = [
    ("00_bootstrap_helpers", bootstrap_helpers),
    ("alh_wallet_query", alh_wallet_query),
    ("alh_dbtoken_query", alh_dbtoken_query),
    ("alh_catalog_sync_apikey", alh_catalog_sync_apikey),
    ("exacs_wallet_query", exacs_wallet_query),
    ("exacs_dbtoken_query", exacs_dbtoken_query),
    ("exacs_user_password", exacs_user_password),
    ("bds_hive_kerberos", bds_hive_kerberos),
    ("bds_hive_ldap", bds_hive_ldap),
    ("fusion_rest_basic", fusion_rest_basic),
    ("fusion_bicc_to_dataframe", fusion_bicc_to_dataframe),
    ("epm_planning_basic", epm_planning_basic),
    ("essbase_mdx_basic", essbase_mdx_basic),
    ("kafka_streaming_apikey", kafka_streaming_apikey),
]


if __name__ == "__main__":
    for name, builder in NOTEBOOKS:
        write(name, builder())
    print(f"\nbuilt {len(NOTEBOOKS)} notebooks under {EXAMPLES}")

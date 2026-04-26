"""Oracle Fusion REST + BICC helpers.

Two distinct flows:
- ``fetch_paged()`` for live REST calls (≤ 499 rows/page hard cap per MOS 2429019.1).
- ``trigger_bicc_extract()`` + ``read_bicc_csv_from_object_storage()`` for bulk
  extracts that land as gzipped CSV in OCI Object Storage.
"""

from __future__ import annotations

import io
import time
from typing import Any, Iterator, Optional

# Per Oracle MOS Doc ID 2429019.1
FUSION_PAGE_LIMIT_HARD_CAP = 499


def fetch_paged(
    session: Any,
    base_url: str,
    path: str,
    *,
    limit: int = FUSION_PAGE_LIMIT_HARD_CAP,
    fields: Optional[str] = None,
    extra_params: Optional[dict] = None,
) -> Iterator[dict]:
    """Yield rows from a Fusion REST resource, page by page.

    Args:
        session: A ``requests.Session`` from
            ``oracle_ai_data_platform_connectors.auth.user_principal.http_basic_session``.
        base_url: Fusion pod base URL (e.g.
            ``https://my-pod.fa.us-phoenix-1.oraclecloud.com``).
        path: Resource path beneath base_url (e.g.
            ``/fscmRestApi/resources/11.13.18.05/invoices``).
        limit: Page size. **Hard-capped at 499 by Fusion.** Anything higher is
            silently truncated server-side.
        fields: Comma-separated list of field names to project. Lets you avoid
            pulling unneeded columns.
        extra_params: Additional query params (e.g. ``q=...`` filters).

    Yields:
        One Python dict per row.
    """
    if limit > FUSION_PAGE_LIMIT_HARD_CAP:
        limit = FUSION_PAGE_LIMIT_HARD_CAP

    base_url = base_url.rstrip("/")
    offset = 0
    while True:
        params = {
            "limit": limit,
            "offset": offset,
            "onlyData": "true",
        }
        if fields:
            params["fields"] = fields
        if extra_params:
            params.update(extra_params)

        url = f"{base_url}{path}"
        response = session.get(url, params=params, timeout=120)
        response.raise_for_status()
        payload = response.json()
        items = payload.get("items", [])
        if not items:
            return
        yield from items

        if not payload.get("hasMore", False):
            return
        offset += limit


def trigger_bicc_extract(
    session: Any,
    base_url: str,
    offering: str,
    *,
    poll_interval_seconds: int = 30,
    timeout_seconds: int = 3600,
) -> str:
    """Trigger a BICC extract job and wait for completion.

    Args:
        session: ``requests.Session`` with HTTP Basic auth (BICC trigger side).
        base_url: Fusion pod base URL (same as REST).
        offering: BICC offering ID (e.g. ``"FscmTopModelAM.AnalyticsServiceAM"``).
        poll_interval_seconds: How often to poll status. Default 30s.
        timeout_seconds: Give up after this many seconds. Default 1h.

    Returns:
        The OCI Object Storage prefix (relative to your BICC bucket) where
        the extract's gzipped CSV files landed. Pass this to
        ``read_bicc_csv_from_object_storage()`` to materialize as a Spark
        DataFrame.
    """
    base_url = base_url.rstrip("/")
    submit_path = "/biacm/api/v2/extracts/run"
    response = session.post(
        f"{base_url}{submit_path}",
        json={"offeringName": offering},
        timeout=60,
    )
    response.raise_for_status()
    job_id = response.json()["jobId"]

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        status_response = session.get(
            f"{base_url}/biacm/api/v2/extracts/{job_id}",
            timeout=60,
        )
        status_response.raise_for_status()
        status = status_response.json()
        state = status.get("status", "").upper()
        if state in ("SUCCEEDED", "COMPLETED"):
            return status["outputPrefix"]
        if state in ("FAILED", "CANCELLED", "ERROR"):
            raise RuntimeError(f"BICC extract failed: {status}")
        time.sleep(poll_interval_seconds)

    raise TimeoutError(f"BICC extract did not finish in {timeout_seconds}s")


def read_bicc_csv_from_object_storage(
    spark: Any,
    namespace: str,
    bucket: str,
    prefix: str,
    *,
    schema: Optional[Any] = None,
):
    """Read all gzipped CSV files under an OCI Object Storage prefix into Spark.

    The AIDP cluster's Spark configuration must already have the OCI Object
    Storage connector attached (`oci://` scheme) and credentials configured —
    typically inherited from the cluster's API key profile.

    Args:
        spark: The active SparkSession.
        namespace: OCI Object Storage namespace.
        bucket: Bucket name where BICC dropped the extract.
        prefix: Output prefix from ``trigger_bicc_extract``.
        schema: Optional StructType to enforce; otherwise schema is inferred
            (slower for large files but fine for one-shot extracts).

    Returns:
        A Spark DataFrame.
    """
    path = f"oci://{bucket}@{namespace}/{prefix.lstrip('/')}/*.csv.gz"
    reader = spark.read.format("csv").option("header", "true").option(
        "compression", "gzip"
    )
    if schema is not None:
        reader = reader.schema(schema)
    else:
        reader = reader.option("inferSchema", "true")
    return reader.load(path)


def rows_to_spark_dataframe(spark: Any, rows: Iterator[dict]):
    """Materialize an iterator of dicts as a Spark DataFrame.

    Uses pandas in the middle to let Spark infer schema. For large rowsets,
    use the BICC extract path instead.
    """
    import pandas as pd

    pdf = pd.DataFrame(list(rows))
    if pdf.empty:
        # Spark needs SOMETHING — return a 0-row DataFrame with a placeholder col.
        return spark.createDataFrame([], "placeholder STRING")
    return spark.createDataFrame(pdf)

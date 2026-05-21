"""AidpRestClient — typed primitives for the AIDP control-plane REST surface.

Reusable across skills: fusion-tc26-run, fusion-medallion-doctor, fusion-cluster-status,
fusion-evidence-append, and anything else that needs to dispatch a notebook to a live
AIDP cluster or fetch task-run output.

Empirically-confirmed REST shapes baked in. See SKILL.md for the receipts and
dev/RESEARCH_aidp_rest_api_probe_results.md for the full probe log.
"""
from __future__ import annotations

import json
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import oci
import requests

API_VERSION = "20260430"
DEFAULT_TIMEOUT_S = 60


# ---------------------------------------------------------------------------
# Resource summaries — light dataclasses so callers don't dig through dicts
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WorkspaceSummary:
    key: str
    display_name: str | None


@dataclass(frozen=True)
class ClusterSummary:
    key: str
    display_name: str | None
    state: str  # ACTIVE | STOPPED | FAILED | CREATING


@dataclass(frozen=True)
class RunResult:
    status: str  # PENDING | RUNNING | SUCCESS | FAILED | CANCELED | TIMED_OUT
    raw: dict[str, Any]


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class AidpRestError(RuntimeError):
    """REST call returned a non-success status or unexpected shape."""


class AidpRestClient:
    """One client per (region, aidp_id, workspace_key) tuple.

    Authentication uses the local OCI config (~/.oci/config). The default profile
    is used unless ``oci_profile`` is given.

    All methods raise :class:`AidpRestError` on non-2xx responses or when the
    response shape diverges from the empirically-verified contract.
    """

    def __init__(
        self,
        *,
        region: str,
        aidp_id: str,
        workspace_key: str,
        oci_profile: str = "DEFAULT",
        request_timeout_s: int = DEFAULT_TIMEOUT_S,
        log: callable | None = None,
    ) -> None:
        self.region = region
        self.aidp_id = aidp_id
        self.workspace_key = workspace_key
        self.request_timeout_s = request_timeout_s
        self._log = log or (lambda stage, **kw: None)
        cfg = oci.config.from_file(profile_name=oci_profile)
        self._signer = oci.signer.Signer(
            tenancy=cfg["tenancy"], user=cfg["user"], fingerprint=cfg["fingerprint"],
            private_key_file_location=cfg["key_file"],
        )

    # ---- URL building -----------------------------------------------------

    @property
    def host(self) -> str:
        return f"https://datalake.{self.region}.oci.oraclecloud.com"

    @property
    def base(self) -> str:
        return f"{self.host}/{API_VERSION}/aiDataPlatforms/{self.aidp_id}/workspaces/{self.workspace_key}"

    @property
    def datalake_base(self) -> str:
        """Data-lake-scoped base (no workspace) — for listing workspaces."""
        return f"{self.host}/{API_VERSION}/aiDataPlatforms/{self.aidp_id}"

    # ---- Low-level HTTP ---------------------------------------------------

    def _request(self, method: str, url: str, *, json_body: dict | None = None,
                 timeout: int | None = None) -> requests.Response:
        return requests.request(
            method, url, auth=self._signer, json=json_body,
            timeout=timeout or self.request_timeout_s,
        )

    def _ok(self, r: requests.Response, *, expected: tuple[int, ...] = (200,),
            context: str = "") -> dict[str, Any]:
        if r.status_code not in expected:
            raise AidpRestError(
                f"{context or r.url}: HTTP {r.status_code} (expected one of {expected}) "
                f"body={r.text[:500]}"
            )
        try:
            return r.json()
        except ValueError as e:
            raise AidpRestError(f"{context}: non-JSON response: {r.text[:200]}") from e

    # ---- Workspace + cluster discovery -----------------------------------

    def list_workspaces(self) -> list[WorkspaceSummary]:
        r = self._request("GET", f"{self.datalake_base}/workspaces")
        d = self._ok(r, context="list_workspaces")
        items = d.get("items", d) if isinstance(d, dict) else d
        out: list[WorkspaceSummary] = []
        for it in items:
            out.append(WorkspaceSummary(key=it["key"], display_name=it.get("displayName")))
        return out

    def find_workspace_by_name(self, display_name: str) -> WorkspaceSummary:
        # The list response sometimes omits names; GET each workspace to resolve.
        for ws in self.list_workspaces():
            if ws.display_name == display_name:
                return ws
            r = self._request("GET", f"{self.datalake_base}/workspaces/{ws.key}")
            d = self._ok(r, context=f"get_workspace({ws.key})")
            if d.get("displayName") == display_name:
                return WorkspaceSummary(key=d["key"], display_name=d.get("displayName"))
        raise AidpRestError(f"no workspace with displayName={display_name!r}")

    def list_clusters(self) -> list[ClusterSummary]:
        r = self._request("GET", f"{self.base}/clusters")
        d = self._ok(r, context="list_clusters")
        items = d.get("items", d) if isinstance(d, dict) else d
        out: list[ClusterSummary] = []
        for it in items:
            out.append(ClusterSummary(
                key=it["key"], display_name=it.get("displayName"),
                state=it.get("state", "UNKNOWN"),
            ))
        return out

    def find_cluster_by_name(self, display_name: str) -> ClusterSummary:
        for c in self.list_clusters():
            if c.display_name == display_name:
                return c
        raise AidpRestError(f"no cluster with displayName={display_name!r}")

    def get_cluster(self, cluster_key: str) -> dict[str, Any]:
        r = self._request("GET", f"{self.base}/clusters/{cluster_key}")
        return self._ok(r, context=f"get_cluster({cluster_key})")

    def verify_cluster_active(self, cluster_key: str) -> None:
        state = self.get_cluster(cluster_key)["state"]
        self._log("cluster_state", state=state)
        if state != "ACTIVE":
            raise AidpRestError(f"cluster {cluster_key} state={state}, expected ACTIVE")

    def start_cluster(self, cluster_key: str) -> dict[str, Any]:
        r = self._request("POST", f"{self.base}/clusters/{cluster_key}/actions/start",
                          json_body={})  # empty body REQUIRED — null body returns 400
        return self._ok(r, expected=(200, 202), context=f"start_cluster({cluster_key})")

    def wait_cluster_active(self, cluster_key: str, *, timeout_s: int = 600,
                            poll_s: int = 10) -> None:
        deadline = time.time() + timeout_s
        last = None
        while time.time() < deadline:
            state = self.get_cluster(cluster_key)["state"]
            if state != last:
                self._log("cluster_wait", state=state)
                last = state
            if state == "ACTIVE":
                return
            if state in ("FAILED",):
                raise AidpRestError(f"cluster transitioned to {state} while waiting")
            time.sleep(poll_s)
        raise AidpRestError(f"cluster {cluster_key} did not reach ACTIVE within {timeout_s}s")

    # ---- Notebook contents API -------------------------------------------

    def upload_notebook(self, workspace_path: str, ipynb: dict) -> str:
        """PUT a notebook to /Workspace/.../<file>.ipynb. Returns the stored path
        (server strips leading slash)."""
        enc = urllib.parse.quote(workspace_path, safe="")
        body = {
            "name": Path(workspace_path).name,
            "path": workspace_path,
            "type": "notebook",
            "format": "json",
            "content": ipynb,
        }
        r = self._request("PUT", f"{self.base}/notebook/api/contents/{enc}",
                          json_body=body, timeout=120)
        d = self._ok(r, expected=(200, 201), context=f"upload_notebook({workspace_path})")
        path = d.get("path", workspace_path)
        self._log("notebook_uploaded", path=path)
        return path

    def get_notebook(self, workspace_path: str) -> dict[str, Any]:
        """Read a notebook back. The `?type=notebook&content=1` query is required."""
        enc = urllib.parse.quote(workspace_path, safe="")
        r = self._request("GET",
                          f"{self.base}/notebook/api/contents/{enc}?type=notebook&content=1")
        return self._ok(r, context=f"get_notebook({workspace_path})")

    # ---- Jobs + JobRuns + TaskRuns ---------------------------------------

    def create_notebook_job(
        self,
        *,
        name: str,
        description: str,
        notebook_path: str,
        cluster_key: str,
        cluster_name: str,
        task_key: str,
    ) -> str:
        """Create a single-task NOTEBOOK_TASK job. Returns jobKey.

        Bakes in the empirically-required quirks: path='jobs', maxConcurrentRuns=1,
        redundant cluster ref in both jobClusters[] and tasks[].cluster.
        """
        cluster_ref = {"clusterKey": cluster_key, "clusterName": cluster_name, "newCluster": None}
        body = {
            "name": name, "description": description,
            "path": "jobs",            # required client-side; undocumented
            "maxConcurrentRuns": 1,    # required client-side; undocumented
            "jobClusters": [cluster_ref], "parameters": [],
            "tasks": [{
                "type": "NOTEBOOK_TASK", "taskKey": task_key, "runIf": "ALL_SUCCESS",
                "notebookPath": notebook_path, "cluster": cluster_ref,
                "parameters": [], "dependsOn": [], "maxRetries": 0,
            }],
        }
        r = self._request("POST", f"{self.base}/jobs", json_body=body)
        d = self._ok(r, expected=(201,), context="create_notebook_job")
        job_key = d["key"]
        self._log("job_created", jobKey=job_key)
        return job_key

    def submit_run(self, job_key: str, *, parameters: list[dict] | None = None) -> str:
        body = {
            "jobKey": job_key,
            "parameters": parameters or [],
            "queue": {"isEnabled": False},
        }
        r = self._request("POST", f"{self.base}/jobRuns", json_body=body)
        d = self._ok(r, expected=(201,), context=f"submit_run({job_key})")
        run_key = d["key"]
        self._log("jobRun_submitted", jobRunKey=run_key)
        return run_key

    def get_run(self, run_key: str) -> dict[str, Any]:
        r = self._request("GET", f"{self.base}/jobRuns/{run_key}")
        return self._ok(r, context=f"get_run({run_key})")

    TERMINAL_STATUSES: frozenset[str] = frozenset({"SUCCESS", "FAILED", "CANCELED", "TIMED_OUT"})

    def poll_run(
        self,
        run_key: str,
        *,
        timeout_s: int = 1800,
        interval_s: int = 20,
        on_status_change: callable | None = None,
    ) -> RunResult:
        """Poll until terminal state. Tolerates transient ReadTimeout / ConnectionError.

        ``on_status_change`` is called with the new status string when the run
        transitions (PENDING → RUNNING → SUCCESS etc).
        """
        deadline = time.time() + timeout_s
        last_status: str | None = None
        run: dict[str, Any] = {}
        while time.time() < deadline:
            try:
                run = self.get_run(run_key)
            except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
                self._log("poll_warn", err=type(e).__name__)
                time.sleep(interval_s)
                continue
            status = run["state"]["status"]
            if status != last_status:
                self._log("poll", status=status)
                if on_status_change is not None:
                    on_status_change(status)
                last_status = status
            if status in self.TERMINAL_STATUSES:
                return RunResult(status=status, raw=run)
            time.sleep(interval_s)
        raise AidpRestError(f"poll_run({run_key}): deadline exceeded after {timeout_s}s")

    @staticmethod
    def resolve_task_run_key(run: dict[str, Any], task_key: str) -> str:
        """Find the taskRunKey for a given taskKey in a JobRun response.

        Looks first in ``taskToTaskRunMap[task_key]``, then falls back to the
        single-task case (one entry in ``taskRunSummaryMap``).
        """
        trk = (run.get("taskToTaskRunMap") or {}).get(task_key)
        if trk:
            return trk
        summary_map = run.get("taskRunSummaryMap") or {}
        if len(summary_map) == 1:
            return next(iter(summary_map))
        raise AidpRestError(
            f"no taskRunKey for task_key={task_key!r}; "
            f"taskToTaskRunMap keys={list((run.get('taskToTaskRunMap') or {}).keys())}, "
            f"taskRunSummaryMap keys={list(summary_map.keys())}"
        )

    def fetch_output(self, task_run_key: str, *, output_key: str = "") -> str:
        """Fetch the executed-notebook JSON string for a task run.

        ``output_key`` must be `""` (empty string) for notebook tasks — `"main"`
        returns a misleading 404 citing the taskRunKey.
        """
        r = self._request(
            "POST", f"{self.base}/taskRuns/{task_run_key}/actions/fetchOutput",
            json_body={"outputKey": output_key},
        )
        if r.status_code != 200:
            self._log("fetch_output_warn", status=r.status_code, body=r.text[:300])
            return ""
        out = r.json()
        data_arr = out.get("data") or []
        if not data_arr:
            return ""
        # Empirically: notebook JSON at data[0].value, NOT data[0].content
        return data_arr[0].get("value", "") or ""

    # ---- Output-marker parsing -------------------------------------------

    @staticmethod
    def parse_marker(
        executed_notebook: dict[str, Any],
        *,
        begin: str,
        end: str,
    ) -> dict[str, Any] | None:
        """Walk cells[*].outputs[*] of an executed notebook for a stdout marker block.

        Used to extract structured payloads that the notebook printed (e.g.
        ``AIDP_LIVE_TEST_RESULT_BEGIN <json> AIDP_LIVE_TEST_RESULT_END``).
        Returns the parsed JSON or None if the marker isn't present.
        """
        for cell in executed_notebook.get("cells", []):
            for output in cell.get("outputs", []):
                for src in ("text", "data"):
                    value = output.get(src)
                    if value is None:
                        continue
                    if src == "data":
                        value = value.get("text/plain", "")
                    if isinstance(value, list):
                        value = "".join(value)
                    if begin in value:
                        b = value.index(begin) + len(begin)
                        e = value.index(end, b)
                        return json.loads(value[b:e].strip())
        return None

    @staticmethod
    def extract_cell_errors(executed_notebook: dict[str, Any]) -> list[dict[str, Any]]:
        """Return a list of cell-level errors (ename, evalue, traceback) from an
        executed notebook. Useful for diagnosing FAILED runs without re-dispatching.
        """
        errors: list[dict[str, Any]] = []
        for i, cell in enumerate(executed_notebook.get("cells", [])):
            for output in cell.get("outputs", []):
                if output.get("output_type") == "error":
                    errors.append({
                        "cell_index": i,
                        "ename": output.get("ename"),
                        "evalue": output.get("evalue"),
                        "traceback": output.get("traceback", []),
                    })
        return errors


__all__ = [
    "AidpRestClient",
    "AidpRestError",
    "WorkspaceSummary",
    "ClusterSummary",
    "RunResult",
]

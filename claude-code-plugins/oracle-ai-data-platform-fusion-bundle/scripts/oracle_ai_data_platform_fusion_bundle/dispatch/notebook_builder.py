"""4-cell ipynb generator for REST dispatch (P1.5ε §Step 4).

Productized from ``.claude/skills/fusion-tc26-run/dispatch.py:build_notebook``.
Differences from the TC26 template:

- ``mode`` / ``datasets`` / ``layers`` come from the operator's CLI flags
  (not hardcoded ``mode="seed"``); the orchestrator interprets them.
- ``bundle_yaml`` is the operator's bundle.yaml content verbatim (not a
  ``NARROW_BUNDLE`` / ``FULL_BUNDLE`` template).
- The run-cell emits the FULL ``RunSummary`` payload via
  ``summary.to_marker_dict()`` (NOT a hand-rolled subset dict —
  hand-rolled dicts drift from the schema and break
  ``RunSummary.from_marker_dict`` laptop-side).
- ``resume_run_id`` is NOT a parameter — it's hardcoded to ``None`` in
  the run cell. Resume-over-REST is tracked as ``P1.5ε-fix5`` follow-up.

The notebook contract is the **only** boundary between the laptop-side
dispatcher and the cluster-side orchestrator. Adding a new orchestrator
flag means adding it to ``build_notebook``'s signature and threading it
into the run-cell template — no changes anywhere else in the dispatch
package.
"""

from __future__ import annotations

import base64
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

# Marker delimiters — must match ``schema.run_summary.RunSummary``'s
# serialization contract. The dispatch package's ``parse_marker`` looks
# for the same strings.
MARKER_BEGIN = "AIDP_LIVE_TEST_RESULT_BEGIN"
MARKER_END = "AIDP_LIVE_TEST_RESULT_END"


def _code_cell(source: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": source.splitlines(keepends=True),
    }


def _markdown_cell(source: str) -> dict:
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": [source if source.endswith("\n") else source + "\n"],
    }


def _build_install_cell(wheel_path: Path) -> str:
    wheel_b64 = base64.b64encode(wheel_path.read_bytes()).decode()
    wheel_filename = wheel_path.name
    return (
        f"import base64, subprocess, sys, tempfile, pathlib\n"
        f'WHEEL_B64 = """{wheel_b64}"""\n'
        f'_stage = pathlib.Path(tempfile.mkdtemp(prefix="aidp_fusion_bundle_"))\n'
        f'_whl = _stage / "{wheel_filename}"\n'
        f"_whl.write_bytes(base64.b64decode(WHEEL_B64))\n"
        f'_target = _stage / "site-packages"\n'
        f"_target.mkdir()\n"
        f"res = subprocess.run(\n"
        f'    [sys.executable, "-m", "pip", "install", "--quiet", "--no-deps",\n'
        f'     "--target", str(_target), str(_whl)],\n'
        f"    capture_output=True, text=True, timeout=180,\n"
        f")\n"
        f'print(f"pip rc={{res.returncode}}")\n'
        f"if res.returncode != 0:\n"
        f'    print("STDOUT:", res.stdout[-2000:])\n'
        f'    print("STDERR:", res.stderr[-2000:])\n'
        f'    raise RuntimeError("plugin wheel install failed")\n'
        f"sys.path.insert(0, str(_target))\n"
        f'print(f"plugin installed to {{_target}}")\n'
    )


def _build_creds_cell(
    *,
    bundle_yaml: str,
    bicc_secret_name: str,
    bicc_secret_key: str,
) -> str:
    return (
        f"import os\n"
        f"from pathlib import Path\n"
        f'os.environ["FUSION_BICC_PASSWORD"] = aidputils.secrets.get(  # noqa: F821\n'
        f"    name={bicc_secret_name!r}, key={bicc_secret_key!r}\n"
        f")\n"
        f'assert os.environ["FUSION_BICC_PASSWORD"], (\n'
        f'    f"AIDP credential store returned empty value for "\n'
        f'    f"name={bicc_secret_name!r} key={bicc_secret_key!r}"\n'
        f")\n"
        f'_pw_len = len(os.environ["FUSION_BICC_PASSWORD"])\n'
        f'print(f"FUSION_BICC_PASSWORD loaded (length={{_pw_len}})")\n'
        f'BUNDLE_PATH = Path("bundle.yaml")\n'
        f"BUNDLE_PATH.write_text({bundle_yaml!r})\n"
        f"from oracle_ai_data_platform_fusion_bundle import orchestrator\n"
        f'print("orchestrator loaded")\n'
    )


def _build_run_cell(
    *,
    mode: Literal["seed", "incremental"],
    datasets: list[str] | None,
    layers: list[str] | None,
    execution_backend: str = "legacy-python",
) -> str:
    # Phase 2: when execution_backend == "content-pack", the bootstrap
    # cell that ran just before this one set up _resolved_pack and
    # _tenant_profile; we thread them into orchestrator.run as kwargs.
    if execution_backend == "content-pack":
        backend_kwargs = (
            f'    execution_backend="content-pack",\n'
            f"    resolved_pack=_resolved_pack,  # noqa: F821 — bootstrap cell\n"
            f"    tenant_profile=_tenant_profile,  # noqa: F821 — bootstrap cell\n"
        )
    else:
        backend_kwargs = f'    execution_backend="legacy-python",\n'

    return (
        f"import json, time\n"
        f"_tstart = time.time()\n"
        f"summary = orchestrator.run(  # noqa: F821\n"
        f"    bundle_path=BUNDLE_PATH,  # noqa: F821\n"
        f"    spark=spark,  # noqa: F821\n"
        f"    mode={mode!r},\n"
        f"    datasets={datasets!r},\n"
        f"    layers={layers!r},\n"
        f"    dry_run=False,\n"
        f"    resume_run_id=None,\n"
        f"{backend_kwargs}"
        f")\n"
        f"_twall = time.time() - _tstart\n"
        f'print(f"run_id={{summary.run_id}}")\n'
        f'print(f"steps: {{summary.succeeded}} ok, {{summary.failed}} failed, "\n'
        f'      f"{{summary.skipped}} skipped, {{summary.deferred}} deferred "\n'
        f'      f"({{summary.total_duration_seconds:.1f}}s reported / {{_twall:.1f}}s wall)")\n'
        f"for step in summary.steps:\n"
        f'    _skip_tag = f" [{{step.skip_reason}}]" if step.skip_reason else ""\n'
        f'    _rc = step.row_count if step.row_count is not None else "-"\n'
        f'    _err = (\n'
        f'        f" err={{step.error_message[:80]}}"\n'
        f'        if step.error_message and step.status == "failed"\n'
        f'        else ""\n'
        f"    )\n"
        f'    print(\n'
        f'        f"  {{step.layer:6s}}  {{step.dataset_id:24s}}  "\n'
        f'        f"{{step.status:10s}}{{_skip_tag:12s}}  rows={{str(_rc):>10s}}  "\n'
        f'        f"dur={{step.duration_seconds:.2f}}s{{_err}}"\n'
        f"    )\n"
        f"# Marker emit — use to_marker_dict() so the laptop-side dispatcher\n"
        f"# can round-trip via RunSummary.from_marker_dict (P1.5ε §4.3a).\n"
        f"_payload = summary.to_marker_dict()\n"
        f'print({MARKER_BEGIN!r}, json.dumps(_payload), {MARKER_END!r})\n'
    )


def _encode_payload_b64(obj: Any) -> str:
    """base64(json) encoding for arbitrary dict/list payloads.

    Pure-ASCII opaque token — safe to splice into the generated
    notebook source as a Python string literal. ``sort_keys=True``
    makes the encoded form deterministic for snapshot tests;
    ``ensure_ascii=True`` guarantees no non-ASCII chars in the token.
    """
    import base64
    import json as _json
    raw = _json.dumps(obj, ensure_ascii=True, sort_keys=True).encode("utf-8")
    return base64.b64encode(raw).decode("ascii")


def _encode_text_b64(text: str) -> str:
    """base64 encoding for arbitrary text payloads (e.g. profile YAML).

    Same safety guarantees as ``_encode_payload_b64`` but for plain
    string content. No JSON wrapping — the cluster-side decoder calls
    ``base64.b64decode(...).decode('utf-8')`` and gets the original
    text back byte-for-byte.
    """
    import base64
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def _build_content_pack_bootstrap_cell(
    *,
    profile_yaml: str,
    pack_files: Mapping[str, str],
    pack_manifest: dict[str, Any],
) -> str:
    """Cell that materialises the staged pack + reconstructs ResolvedPack on the cluster.

    Emits Python source that:

    1. Imports the orchestrator helpers (load_full_chain,
       materialize_staged_pack, load_tenant_profile_from_string).
    2. Decodes the embedded base64+json payloads to get the staged
       files dict + manifest + profile YAML text.
    3. Materialises the files to a tempdir + builds the staging-aware
       base resolver.
    4. Reconstructs ResolvedPack via load_full_chain(top_root,
       base_resolver=...).
    5. Reconstructs TenantProfile via load_tenant_profile_from_string.

    The orchestrator.run call in the run cell consumes
    ``_resolved_pack`` + ``_tenant_profile`` from this cell's namespace.
    """
    pack_files_b64 = _encode_payload_b64(dict(pack_files))
    pack_manifest_b64 = _encode_payload_b64(pack_manifest)
    profile_yaml_b64 = _encode_text_b64(profile_yaml)

    return (
        f"import base64 as _b64\n"
        f"import json as _json\n"
        f"from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack_staging import materialize_staged_pack\n"
        f"from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack import load_full_chain\n"
        f"from oracle_ai_data_platform_fusion_bundle.schema.tenant_profile import load_tenant_profile_from_string\n"
        f"_PACK_FILES = _json.loads(_b64.b64decode({pack_files_b64!r}).decode('utf-8'))\n"
        f"_PACK_MANIFEST = _json.loads(_b64.b64decode({pack_manifest_b64!r}).decode('utf-8'))\n"
        f"_PROFILE_YAML = _b64.b64decode({profile_yaml_b64!r}).decode('utf-8')\n"
        f"_top_overlay_root, _base_resolver = materialize_staged_pack(_PACK_FILES, _PACK_MANIFEST)\n"
        f"_resolved_pack = load_full_chain(_top_overlay_root, base_resolver=_base_resolver)\n"
        f"_tenant_profile = load_tenant_profile_from_string(_PROFILE_YAML)\n"
        f'print(f"content-pack bootstrap: pack={{_resolved_pack.pack.id}}@{{_resolved_pack.pack.version}} tenant={{_tenant_profile.tenant}}")\n'
    )


def _build_verify_cell() -> str:
    return (
        "from oracle_ai_data_platform_fusion_bundle.schema.bundle import load_bundle\n"
        "_bundle, _paths = load_bundle(BUNDLE_PATH)  # noqa: F821\n"
        '_state_table = _paths.bronze("fusion_bundle_state")\n'
        "spark.sql(  # noqa: F821\n"
        '    f"""SELECT dataset_id, layer, mode, status, row_count, '
        "skip_reason, duration_seconds FROM (SELECT *, ROW_NUMBER() OVER "
        "(PARTITION BY dataset_id ORDER BY last_run_at DESC) AS rn FROM "
        "{_state_table} WHERE run_id = '{summary.run_id}') t WHERE rn=1 "
        'ORDER BY layer, dataset_id"""\n'
        ").show(200, truncate=False)\n"
        'for _layer in ("silver", "gold"):\n'
        '    _rc_col = f"{_layer}_run_id"\n'
        "    _candidate = next(\n"
        "        (s for s in summary.steps  # noqa: F821\n"
        '         if s.layer == _layer and s.status == "success"),\n'
        "        None,\n"
        "    )\n"
        "    if _candidate is None:\n"
        '        print(f"  (no successful {_layer} rows)")\n'
        "        continue\n"
        "    _table = (\n"
        "        _paths.silver(_candidate.dataset_id)\n"
        '        if _layer == "silver"\n'
        "        else _paths.gold(_candidate.dataset_id)\n"
        "    )\n"
        "    _n = spark.sql(  # noqa: F821\n"
        '        f"SELECT COUNT(*) AS n FROM {_table} WHERE {_rc_col} = '
        "'{summary.run_id}'\"\n"
        "    ).collect()[0].n\n"
        "    _total = spark.sql(  # noqa: F821\n"
        '        f"SELECT COUNT(*) AS n FROM {_table}"\n'
        "    ).collect()[0].n\n"
        "    print(\n"
        '        f"SOX-trail {_layer:6s} {_candidate.dataset_id:20s}: "\n'
        '        f"{_rc_col} matches on {_n}/{_total} rows"\n'
        "    )\n"
    )


def build_notebook(
    *,
    wheel_path: Path,
    bundle_yaml: str,
    mode: Literal["seed", "incremental"],
    datasets: list[str] | None,
    layers: list[str] | None,
    bicc_secret_name: str = "fusion_bicc_password",
    bicc_secret_key: str = "password",
    title: str = "P1.5ε dispatch",
    # Phase 2 additions — primitives only (no orchestrator imports).
    execution_backend: str = "legacy-python",
    profile_yaml: str | None = None,
    pack_files: Mapping[str, str] | None = None,
    pack_manifest: dict[str, Any] | None = None,
) -> dict:
    """Build the 4-cell ipynb dict that runs the orchestrator on the cluster.

    Cells:
      1. **install** — base64-decode the wheel, ``pip install --target``,
         ``sys.path.insert``.
      2. **creds + bundle** — load ``FUSION_BICC_PASSWORD`` from
         ``aidputils.secrets``, write ``bundle.yaml``, import orchestrator.
      3. **run** — ``orchestrator.run(...)``, per-step print, marker emit
         via ``summary.to_marker_dict()``.
      4. **verify** — query ``fusion_bundle_state`` + count silver/gold
         audit-col matches for the run_id.

    The run cell injects ``mode`` / ``datasets`` / ``layers`` as literals
    (via ``repr()``). ``resume_run_id`` is hardcoded to ``None`` — REST-
    dispatch resume is out of scope in this PR.

    Returns an nbformat-4 dict ready to pass to
    :meth:`AidpRestClient.upload_notebook`.
    """
    # Phase 2 invariant check: content-pack backend requires all three
    # primitives; legacy-python forbids them (programmer error guard).
    if execution_backend == "content-pack":
        assert profile_yaml is not None, (
            "build_notebook(execution_backend='content-pack', ...) requires profile_yaml"
        )
        assert pack_files is not None, (
            "build_notebook(execution_backend='content-pack', ...) requires pack_files"
        )
        assert pack_manifest is not None, (
            "build_notebook(execution_backend='content-pack', ...) requires pack_manifest"
        )
    elif execution_backend == "legacy-python":
        assert profile_yaml is None and pack_files is None and pack_manifest is None, (
            "build_notebook(execution_backend='legacy-python', ...) must pass "
            "profile_yaml/pack_files/pack_manifest as None"
        )

    cells = [
        _markdown_cell(f"# {title}\nSelf-contained dispatch from `aidp-fusion-bundle run`."),
        _code_cell(_build_install_cell(wheel_path)),
        _code_cell(
            _build_creds_cell(
                bundle_yaml=bundle_yaml,
                bicc_secret_name=bicc_secret_name,
                bicc_secret_key=bicc_secret_key,
            )
        ),
    ]

    if execution_backend == "content-pack":
        cells.append(
            _code_cell(
                _build_content_pack_bootstrap_cell(
                    profile_yaml=profile_yaml,
                    pack_files=pack_files,
                    pack_manifest=pack_manifest,
                )
            )
        )

    cells.extend([
        _code_cell(
            _build_run_cell(
                mode=mode, datasets=datasets, layers=layers,
                execution_backend=execution_backend,
            )
        ),
        _code_cell(_build_verify_cell()),
    ])
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


__all__ = ["MARKER_BEGIN", "MARKER_END", "build_notebook"]

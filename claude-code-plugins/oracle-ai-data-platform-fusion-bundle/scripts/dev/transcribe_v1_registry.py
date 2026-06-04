"""Transcribe v1 registry metadata into a checked-in test fixture.

Reads ``schema/registry_metadata.py`` from the v1 reference branch
``P1.5ε-fix5`` via ``git show`` and emits ``tests/fixtures/v1_registry_snapshot.yaml``
as deterministic, byte-stable YAML.

Used as the parity baseline for v2's Phase 4 dual-runner gate. The fixture
is **test data**, not runtime code — the engine never imports it.

Provenance (recorded on every run):

    branch:        P1.5ε-fix5
    branch head:   650d6909655fd30618f56edbbded6e4b81d6cc3b
    file blob:     02ec45a7fae7c1fa5b94a3940144727da69dcc13

If the branch head advances past 650d690 (v1 maintenance work continues),
the script warns. If the blob hash diverges from 02ec45a7, the script hard
fails — the v1 registry's content has changed and the snapshot must be
re-reviewed before regenerating.

Usage:
    python scripts/dev/transcribe_v1_registry.py > tests/fixtures/v1_registry_snapshot.yaml

The script writes to stdout; redirect into the fixture path.

Re-running with the same input must produce byte-identical output (tested
in ``tests/unit/test_v1_registry_snapshot.py``).
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import yaml

V1_BRANCH = "P1.5ε-fix5"
V1_FILE_REL = "./scripts/oracle_ai_data_platform_fusion_bundle/schema/registry_metadata.py"
EXPECTED_HEAD = "650d6909655fd30618f56edbbded6e4b81d6cc3b"
EXPECTED_BLOB = "02ec45a7fae7c1fa5b94a3940144727da69dcc13"


def _git_rev_parse(target: str) -> str:
    return subprocess.run(
        ["git", "rev-parse", target],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _git_show(target: str) -> str:
    return subprocess.run(
        ["git", "show", target],
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def fetch_v1_registry_source() -> str:
    """Fetch the v1 registry_metadata.py source via git show."""
    return _git_show(f"{V1_BRANCH}:{V1_FILE_REL}")


def assert_provenance() -> tuple[str, str]:
    """Check that the v1 branch head + file blob match recorded provenance.

    Returns (current_head, current_blob).
    """
    current_head = _git_rev_parse(V1_BRANCH)
    current_blob = _git_rev_parse(f"{V1_BRANCH}:{V1_FILE_REL}")

    if current_head != EXPECTED_HEAD:
        print(
            f"WARNING: {V1_BRANCH} head has advanced from {EXPECTED_HEAD} to "
            f"{current_head}. v1 maintenance is allowed; re-review the snapshot.",
            file=sys.stderr,
        )

    if current_blob != EXPECTED_BLOB:
        print(
            f"ERROR: {V1_BRANCH}:{V1_FILE_REL} blob has changed from "
            f"{EXPECTED_BLOB} to {current_blob}. The v1 registry's content "
            "has been modified. Re-review and update the snapshot manually.",
            file=sys.stderr,
        )
        sys.exit(2)

    return current_head, current_blob


def parse_v1_registry(source: str) -> dict:
    """Parse the v1 registry source via Python AST.

    Extracts the three top-level dict literals (BRONZE_EXTRACT_METADATA,
    SILVER_DIM_METADATA, GOLD_MART_METADATA) and the three deferred maps.
    """
    tree = ast.parse(source)
    out: dict = {
        "bronze_extract_metadata": {},
        "silver_dim_metadata": {},
        "gold_mart_metadata": {},
        "deferred": {
            "datasets": {},
            "dims": {},
            "marts": {},
        },
    }

    def _extract_dict_assignment(target_name: str) -> ast.Dict | None:
        for node in tree.body:
            if (
                isinstance(node, ast.AnnAssign)
                and isinstance(node.target, ast.Name)
                and node.target.id == target_name
                and isinstance(node.value, ast.Dict)
            ):
                return node.value
            if (
                isinstance(node, ast.Assign)
                and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id == target_name
                and isinstance(node.value, ast.Dict)
            ):
                return node.value
        return None

    def _ast_to_python(node: ast.AST) -> object:
        """Convert literal AST nodes (Call, Tuple, Constant, etc.) to plain Python."""
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, (ast.Tuple, ast.List)):
            return [_ast_to_python(e) for e in node.elts]
        if isinstance(node, ast.Call):
            # Dataclass invocation; map keyword args + positionals to dict.
            cls_name = getattr(node.func, "id", None) or "?"
            obj: dict = {"_class": cls_name}
            # Positional args (e.g., BronzeExtractMetadata("erp_suppliers", "erp_suppliers")).
            for i, arg in enumerate(node.args):
                obj[f"_pos{i}"] = _ast_to_python(arg)
            for kw in node.keywords:
                obj[kw.arg] = _ast_to_python(kw.value)
            return obj
        if isinstance(node, ast.Name):
            return f"<name:{node.id}>"
        raise NotImplementedError(f"unhandled AST node: {ast.dump(node)}")

    # ---- BRONZE ----
    bronze_dict = _extract_dict_assignment("BRONZE_EXTRACT_METADATA")
    if bronze_dict is not None:
        for k_node, v_node in zip(bronze_dict.keys, bronze_dict.values):
            assert isinstance(k_node, ast.Constant)
            key = k_node.value
            v_obj = _ast_to_python(v_node)
            assert isinstance(v_obj, dict)
            # BronzeExtractMetadata(dataset_id, pvo_id) — two positional args.
            entry = {
                "dataset_id": v_obj.get("dataset_id") or v_obj.get("_pos0"),
                "pvo_id": v_obj.get("pvo_id") or v_obj.get("_pos1"),
            }
            out["bronze_extract_metadata"][key] = entry

    # ---- SILVER ----
    silver_dict = _extract_dict_assignment("SILVER_DIM_METADATA")
    if silver_dict is not None:
        for k_node, v_node in zip(silver_dict.keys, silver_dict.values):
            assert isinstance(k_node, ast.Constant)
            key = k_node.value
            v_obj = _ast_to_python(v_node)
            assert isinstance(v_obj, dict)
            entry = {
                "dataset_id": v_obj.get("dataset_id") or v_obj.get("_pos0"),
                "depends_on_bronze": list(
                    v_obj.get("depends_on_bronze") or v_obj.get("_pos1") or []
                ),
                "natural_key": v_obj.get("natural_key", ""),
            }
            out["silver_dim_metadata"][key] = entry

    # ---- GOLD ----
    gold_dict = _extract_dict_assignment("GOLD_MART_METADATA")
    if gold_dict is not None:
        for k_node, v_node in zip(gold_dict.keys, gold_dict.values):
            assert isinstance(k_node, ast.Constant)
            key = k_node.value
            v_obj = _ast_to_python(v_node)
            assert isinstance(v_obj, dict)
            natural_key = v_obj.get("natural_key", "")
            # Normalise tuple/list -> list; bare string stays string.
            if isinstance(natural_key, list):
                natural_key_norm: object = list(natural_key)
            else:
                natural_key_norm = natural_key
            entry = {
                "dataset_id": v_obj.get("dataset_id") or v_obj.get("_pos0"),
                "depends_on_bronze": list(v_obj.get("depends_on_bronze") or []),
                "depends_on_silver": list(v_obj.get("depends_on_silver") or []),
                "natural_key": natural_key_norm,
                "incremental_capable": bool(
                    v_obj.get("incremental_capable", True)
                ),
            }
            out["gold_mart_metadata"][key] = entry

    # ---- DEFERRED (simple str-keyed dicts) ----
    for ast_name, out_key in [
        ("KNOWN_DEFERRED_DATASETS", "datasets"),
        ("KNOWN_DEFERRED_DIMS", "dims"),
        ("KNOWN_DEFERRED_MARTS", "marts"),
    ]:
        deferred_dict = _extract_dict_assignment(ast_name)
        if deferred_dict is None:
            continue
        for k_node, v_node in zip(deferred_dict.keys, deferred_dict.values):
            assert isinstance(k_node, ast.Constant)
            # Reason strings may be ast.Constant or a `Foo + Bar` Concat;
            # for our two-arg parenthesised strings, ast.parse already
            # collapses adjacent string literals.
            if isinstance(v_node, ast.Constant):
                out["deferred"][out_key][k_node.value] = v_node.value
            else:
                # Joined string (parenthesised concat). Walk and concat.
                parts: list[str] = []
                for sub in ast.walk(v_node):
                    if isinstance(sub, ast.Constant) and isinstance(sub.value, str):
                        parts.append(sub.value)
                out["deferred"][out_key][k_node.value] = "".join(parts)

    return out


def main() -> None:
    head, blob = assert_provenance()
    source = fetch_v1_registry_source()
    parsed = parse_v1_registry(source)

    payload = {
        "provenance": {
            "branch": V1_BRANCH,
            "branch_head": head,
            "file_blob": blob,
            "source_path_on_v1": V1_FILE_REL,
            "note": (
                "Test fixture transcribed from v1 reference branch. See "
                "tests/fixtures/README.md. Engine never imports this file."
            ),
        },
        **parsed,
    }

    yaml.safe_dump(
        payload,
        sys.stdout,
        sort_keys=True,
        default_flow_style=False,
        allow_unicode=True,
    )


if __name__ == "__main__":
    main()

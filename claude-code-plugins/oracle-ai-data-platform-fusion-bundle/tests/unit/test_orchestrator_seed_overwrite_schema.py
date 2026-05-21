"""Regression — seed-mode bronze write must use overwriteSchema=true.

Per CLAUDE.md medallion invariant: ``CREATE OR REPLACE TABLE is for seed mode only``.
The orchestrator's seed-mode write at ``orchestrator/__init__.py:289`` must include
``.option("overwriteSchema", "true")`` so a re-run can land on a target with a
divergent prior schema (e.g. stale audit columns from a half-completed run).

Caught live by TC26 probe ``run_id=023482f5-a613-4c62-8bed-b0b238c944f4``: Delta
threw ``[DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields "_watermark_used"
and "_watermark_used"`` on the second seed-mode call against a table created by
an earlier extractors.bicc-attribute-error run.
"""
from __future__ import annotations

from pathlib import Path

from oracle_ai_data_platform_fusion_bundle import orchestrator as orch_pkg


def test_seed_write_includes_overwrite_schema_option() -> None:
    """Static-source check — the orchestrator's bronze-write line must include
    overwriteSchema. Static check (not a runtime mock test) because the bug
    is about the call shape, not the call result — runtime mocks can hide it.
    """
    src = Path(orch_pkg.__file__).read_text()
    assert ".mode(\"overwrite\")" in src, "bronze write should still use mode('overwrite')"
    assert ".option(\"overwriteSchema\", \"true\")" in src, (
        "seed-mode bronze write must include overwriteSchema=true; see CLAUDE.md "
        "medallion invariant + TC26 probe run_id=023482f5"
    )
    # The two must appear on the same logical write chain. Conservative check:
    # the overwriteSchema option must be within ~200 chars of the mode call.
    mode_idx = src.index(".mode(\"overwrite\")")
    schema_idx = src.index(".option(\"overwriteSchema\", \"true\")")
    assert 0 < schema_idx - mode_idx < 300, (
        f"overwriteSchema option must be chained to the seed-mode write "
        f"(distance={schema_idx - mode_idx} chars)"
    )

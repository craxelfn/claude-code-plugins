"""Content-pack adapter for ``implementation.type: python_legacy`` nodes (Phase 5).

The v1 ``dimensions/dim_*.py`` and ``transforms/gold/*.py`` modules ship
``build(spark, *, paths, bronze_*, silver_table|gold_table|silver_dim,
refresh_mode, watermark, run_id, ...) -> DataFrame`` signatures. v2's
content-pack execution path (sql_runner) needs to invoke them through a
uniform shape, but unlike the dim_calendar builtin (Phase 3) the
``python_legacy`` type is not a one-off — any customer-shipped v1 module
referenced from a content-pack node lands here.

This module exposes three helpers used by ``sql_runner._execute_python_legacy_node``:

* :func:`import_legacy_callable` — parses the ``<module>:<func>`` spec from
  ``PythonLegacyImpl.callable``, imports the module, and returns the
  resolved callable. Raises :class:`LegacyCallableSpecError` (AIDPF-2061)
  on malformed spec / module not importable / target attr not callable.

* :func:`_bind_legacy_kwargs` — constructs the v1-correct kwarg dict from
  ``node.depends_on`` + ``paths`` + ``ctx``. Returns kwargs using the v1
  conventional names (``bronze_invoices`` not ``bronze_table``,
  ``silver_dim`` for joined dim, ``refresh_mode`` not ``mode``,
  ``watermark`` not ``prior_watermark``) so customer v1 modules unmodified
  from their shipped form work without a translation layer.

* :func:`invoke_legacy_callable` — invokes the resolved callable with the
  bound kwargs, filtering via ``inspect.signature`` so v1 builders that
  accept narrower kwargs (e.g. a custom dim that doesn't take a
  ``bronze_invoices`` kwarg) tolerate the wider binding dict.

python_legacy contract
----------------------

The callable MUST materialise its own target table (via ``CREATE OR
REPLACE TABLE`` for ``replace`` strategy, ``MERGE INTO`` for ``merge``
strategy) and MUST return a readable ``DataFrame`` /
``spark.table(target)``. The adapter does NOT write the returned
DataFrame back — that would (a) try to overwrite a table from a
DataFrame reading the same table, and (b) ``"merge"`` is not a valid
``DataFrameWriter.mode(...)`` value. Future "pure DataFrame-returning
customer callables" (no self-materialisation) would need a separate
``implementation.type`` and is OUT OF SCOPE for Phase 5.

References:

* PLAN §15 Phase 5 — python_legacy runtime adapter step.
* CLAUDE.md "v1 + v2 coexistence (migration state)" — v1 modules stay
  as frozen reference implementations through Phase 9.
* ``schema/medallion_pack.py:755`` — :class:`PythonLegacyImpl` Pydantic
  model (declares ``callable``, ``deprecated``, ``migrationTarget``).
"""

from __future__ import annotations

import importlib
import inspect
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

    from ...config.paths import TablePaths
    from ...schema.medallion_pack import NodeYaml
    from ...schema.tenant_profile import TenantProfile
    from ..content_pack import ResolvedPack
    from ..sql_renderer import RunContext


VERSION: str = "1.0.0"
"""Adapter version constant. Flows into the content-pack plan-hash
substitute for python_legacy nodes — bumping this triggers the §11.9
drift gate, matching the contract that a SQL-template edit triggers."""


# ---------------------------------------------------------------------------
# AIDPF error codes
# ---------------------------------------------------------------------------

AIDPF_2061_LEGACY_CALLABLE_SPEC_MALFORMED = "AIDPF-2061"
"""python_legacy ``implementation.callable`` is malformed, module is not
importable, or the target attribute is not callable. Raised by
:func:`import_legacy_callable` before any Spark write."""


class LegacyCallableSpecError(Exception):
    """python_legacy callable spec malformed / module not importable /
    target attr not callable (AIDPF-2061)."""


# ---------------------------------------------------------------------------
# v1 source-id → v1 kwarg name mapping
# ---------------------------------------------------------------------------

#: Maps the bronze source-id (as it appears in ``dependsOn.bronze[].id``)
#: to the v1 conventional kwarg name v1 builders accept.
#:
#: Confirmed surface from:
#:
#:   * dim_supplier.build — accepts ``bronze_table`` (single bronze
#:     source, generic name);
#:   * dim_account.build — accepts ``bronze_table`` (single bronze);
#:   * supplier_spend.build — accepts ``bronze_invoices`` (ap_invoices);
#:   * gl_balance.build — accepts ``bronze_balances`` (gl_period_balances);
#:   * ap_aging.build — accepts ``bronze_invoices`` (ap_invoices).
#:
#: Any source id not in this map falls back to ``bronze_<source_id>``,
#: which is the convention a future customer v1 module would follow.
_BRONZE_KWARG_BY_SOURCE_ID: dict[str, str] = {
    "ap_invoices": "bronze_invoices",
    "gl_period_balances": "bronze_balances",
}


def _bronze_kwarg_name(source_id: str) -> str:
    """Return the v1-conventional bronze kwarg name for a source id."""
    return _BRONZE_KWARG_BY_SOURCE_ID.get(source_id, f"bronze_{source_id}")


# ---------------------------------------------------------------------------
# Public API — import_legacy_callable
# ---------------------------------------------------------------------------


def import_legacy_callable(spec: str) -> Callable[..., Any]:
    """Parse ``<module>:<func>`` and return the resolved callable.

    Args:
        spec: ``<module>:<func>`` form (same shape as ``BuiltinImpl.callable``).
            e.g. ``"oracle_ai_data_platform_fusion_bundle.dimensions.dim_supplier:build"``.

    Returns:
        The resolved callable.

    Raises:
        LegacyCallableSpecError: AIDPF-2061. The spec is malformed (missing
            colon, multiple colons, empty module/func), the module is not
            importable, the target attr does not exist, or it exists but is
            not callable.
    """
    if not isinstance(spec, str) or ":" not in spec:
        raise LegacyCallableSpecError(
            f"{AIDPF_2061_LEGACY_CALLABLE_SPEC_MALFORMED}: python_legacy "
            f"implementation.callable={spec!r} is malformed — expected "
            f"'<module>:<func>' (e.g. "
            f"'oracle_ai_data_platform_fusion_bundle.dimensions.dim_supplier:build')."
        )
    parts = spec.split(":")
    if len(parts) != 2:
        raise LegacyCallableSpecError(
            f"{AIDPF_2061_LEGACY_CALLABLE_SPEC_MALFORMED}: python_legacy "
            f"implementation.callable={spec!r} has the wrong number of "
            f"colons — expected exactly one (got {len(parts) - 1})."
        )
    module_name, func_name = parts
    if not module_name or not func_name:
        raise LegacyCallableSpecError(
            f"{AIDPF_2061_LEGACY_CALLABLE_SPEC_MALFORMED}: python_legacy "
            f"implementation.callable={spec!r} has an empty module or "
            f"function name — expected non-empty '<module>:<func>'."
        )
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise LegacyCallableSpecError(
            f"{AIDPF_2061_LEGACY_CALLABLE_SPEC_MALFORMED}: python_legacy "
            f"implementation.callable={spec!r} — module {module_name!r} "
            f"is not importable ({exc})."
        ) from exc
    target = getattr(module, func_name, None)
    if target is None:
        raise LegacyCallableSpecError(
            f"{AIDPF_2061_LEGACY_CALLABLE_SPEC_MALFORMED}: python_legacy "
            f"implementation.callable={spec!r} — module {module_name!r} "
            f"has no attribute {func_name!r}."
        )
    if not callable(target):
        raise LegacyCallableSpecError(
            f"{AIDPF_2061_LEGACY_CALLABLE_SPEC_MALFORMED}: python_legacy "
            f"implementation.callable={spec!r} — {module_name}.{func_name} "
            f"exists but is not callable (got type "
            f"{type(target).__name__})."
        )
    return target


# ---------------------------------------------------------------------------
# Public API — _bind_legacy_kwargs
# ---------------------------------------------------------------------------


def _bind_legacy_kwargs(
    *,
    node: "NodeYaml",  # noqa: F821
    pack: "ResolvedPack",  # noqa: F821
    profile: "TenantProfile",  # noqa: F821
    ctx: "RunContext",
    paths: "TablePaths",  # noqa: F821
) -> dict[str, Any]:
    """Build the v1-correct kwarg dict for a python_legacy callable.

    Constructs kwargs in the same shape v1 ``build(spark, *, ...)``
    functions accept. The orchestrator's ``TablePaths`` flows through
    so v1 builders DO NOT fall back to ``DEFAULT_PATHS`` (which would
    write to ``fusion_catalog.bronze/silver/gold`` instead of the
    tenant's configured catalog/schemas).

    Args:
        node: the NodeYaml (its ``depends_on`` drives bronze/silver kwargs).
        pack: assembled ResolvedPack (unused today — reserved for future
            customer python_legacy adapters that want pack-side metadata).
        profile: validated TenantProfile (unused today — reserved).
        ctx: RunContext (supplies ``run_id`` + ``prior_watermark`` +
            ``mode``).
        paths: TablePaths from the bundle.

    Returns:
        Dict of kwargs in v1-conventional names. Caller is responsible
        for applying ``inspect.signature`` filtering before passing to
        the callable.
    """
    # ``pack`` and ``profile`` are kept on the signature so the binding
    # layer has access to them when future customer modules need
    # pack-side metadata. The current shipped v1 modules ignore them.
    del pack, profile  # acknowledged: unused in v1 builder kwargs

    kwargs: dict[str, Any] = {
        "paths": paths,
        "run_id": ctx.run_id,
        "refresh_mode": ctx.mode,
    }

    # Bronze dependencies — iterate dependsOn.bronze (list[SourceRef]).
    # The v1-conventional kwarg name comes from _bronze_kwarg_name; the
    # fully-qualified bronze identifier comes from paths.bronze(source_id).
    for source in node.depends_on.bronze:
        kw_name = _bronze_kwarg_name(source.id)
        kwargs[kw_name] = paths.bronze(source.id)

    # Silver dependencies — v1 gold marts accept the silver dim they
    # join as ``silver_dim`` (a single string identifier). If the node
    # depends on more than one silver dim, pass the first; future
    # multi-silver customer modules can extend the convention.
    if node.depends_on.silver:
        kwargs["silver_dim"] = paths.silver(node.depends_on.silver[0].id)

    # The node's own target — silver dims accept ``silver_table``,
    # gold marts accept ``gold_table``. Layer drives which one.
    if node.layer == "silver":
        kwargs["silver_table"] = paths.silver(node.target)
    else:
        kwargs["gold_table"] = paths.gold(node.target)

    # Watermark — RunContext.prior_watermark is per-source. v1 builders
    # accept a single ``watermark: datetime | None``. Use the primary
    # source's watermark when one exists; otherwise None. The primary
    # source is the first bronze in dependsOn.bronze (single-bronze
    # nodes have implicit primary; multi-bronze with explicit roles
    # picks role=='primary').
    primary_source_id: str | None = None
    primaries = [s for s in node.depends_on.bronze if s.role == "primary"]
    if primaries:
        primary_source_id = primaries[0].id
    elif node.depends_on.bronze:
        primary_source_id = node.depends_on.bronze[0].id
    if primary_source_id is not None:
        kwargs["watermark"] = ctx.prior_watermark.get(primary_source_id)
    else:
        kwargs["watermark"] = None

    return kwargs


# ---------------------------------------------------------------------------
# Public API — invoke_legacy_callable
# ---------------------------------------------------------------------------


def invoke_legacy_callable(
    callable_: Callable[..., Any],
    spark: "SparkSession",
    *,
    node: "NodeYaml",  # noqa: F821
    pack: "ResolvedPack",  # noqa: F821
    profile: "TenantProfile",  # noqa: F821
    ctx: "RunContext",
    paths: "TablePaths",  # noqa: F821
) -> "DataFrame":
    """Invoke the resolved v1 callable with v1-correct kwargs.

    Applies ``inspect.signature`` filtering: any kwarg the callee does
    not accept is dropped silently. This tolerates narrower customer
    callables (e.g. a v1 module that doesn't take ``run_id``); it does
    NOT paper over wrong names — the binding layer is the source of
    truth for kwarg shape.

    The callable MUST materialise its own target (via CREATE OR REPLACE
    TABLE for replace strategy or MERGE INTO for merge strategy) and
    return a DataFrame. The adapter does NOT write the returned
    DataFrame back to the target.

    Returns:
        Whatever the v1 callable returned (typically
        ``spark.table(<silver|gold>_table)``).
    """
    canonical = _bind_legacy_kwargs(
        node=node, pack=pack, profile=profile, ctx=ctx, paths=paths,
    )

    # ``inspect.signature`` filtering — drop any kwarg the callee
    # doesn't accept. Builders that accept **kwargs effectively accept
    # everything (Parameter.kind == VAR_KEYWORD); detect that and skip
    # the filtering for symmetry with the legacy direct-call path.
    sig = inspect.signature(callable_)
    accepts_var_keyword = any(
        p.kind is inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
    )
    if accepts_var_keyword:
        filtered = canonical
    else:
        accepted_names = set(sig.parameters.keys())
        filtered = {k: v for k, v in canonical.items() if k in accepted_names}

    return callable_(spark, **filtered)


__all__ = [
    "AIDPF_2061_LEGACY_CALLABLE_SPEC_MALFORMED",
    "LegacyCallableSpecError",
    "VERSION",
    "_bind_legacy_kwargs",
    "import_legacy_callable",
    "invoke_legacy_callable",
]

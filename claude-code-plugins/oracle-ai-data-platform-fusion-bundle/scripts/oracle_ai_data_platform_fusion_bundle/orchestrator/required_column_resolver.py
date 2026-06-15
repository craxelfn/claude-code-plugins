"""Required-column alias resolver shared by run-level + per-node preflight gates.

``requiredColumns`` entries in a content-pack node may be one of two
shapes:

* A **literal** column name like ``ApInvoicesVendorId`` — used as-is.
* A ``$column.<key>`` **reference** like ``$column.invoice_currency_code``
  — resolved against ``pack.column_aliases.<key>`` (which lists the
  candidate physical column names per tenant) and the tenant profile's
  ``resolved.column[<key>]`` (which records which candidate was picked
  at bootstrap).

The per-node preflight at :mod:`orchestrator.node_preflight` already
resolves these references before checking the live bronze schema.
Without an equivalent step in the run-level gates
(:mod:`orchestrator.bronze_readiness` for AIDPF-2071,
:mod:`orchestrator.fusion_pvo_drift` for AIDPF-2072), those gates
compare literal ``$column.*`` strings against live source columns
and false-fail the run for any pack that uses the alias syntax — the
starter pack at ``content_packs/fusion-finance-starter/`` does, so
this is on the default-flipped path.

This module factors the resolver into one helper so both gates can
reuse it without duplicating the ``$column.`` prefix logic.

The per-node equivalent is
:func:`orchestrator.node_preflight._resolve_required_column_entry`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from ..schema.tenant_profile import TenantProfile
    from .content_pack import ResolvedPack


_COLUMN_REF_PREFIX = "$column."
"""Same constant as :data:`orchestrator.node_preflight._COLUMN_REF_PREFIX`;
duplicated here so the run-level gates don't import the per-node
preflight module (which has heavier Spark imports)."""


def resolve_required_column_entries(
    entries: "list[str] | set[str] | tuple[str, ...]",
    *,
    resolved_pack: "ResolvedPack | None",
    tenant_profile: "TenantProfile | None",
) -> set[str]:
    """Resolve a list of ``requiredColumns`` entries to physical column names.

    For each entry:

    * Literal (no prefix) → kept as-is.
    * ``$column.<key>`` and ``<key>`` is in both ``pack.column_aliases``
      AND ``profile.resolved.column`` → use the resolved value.
    * ``$column.<key>`` but the key cannot be resolved → **drop silently**.

    The "drop silently" branch is deliberate. The run-level gates run
    BEFORE the per-node preflight; per-node preflight is the canonical
    place to surface AIDPF-2046 (``$column.*`` reference unresolved).
    Re-raising here would either (a) duplicate the AIDPF-2046
    diagnostic, or (b) consume an alias-resolution failure as a fake
    AIDPF-2071/2072 "column missing in live bronze" which obscures the
    real cause. Drop the unresolvable entry and let per-node preflight
    issue the proper AIDPF-2046 diagnostic when the run actually
    dispatches the affected node.

    When ``resolved_pack`` or ``tenant_profile`` is ``None``, every
    ``$column.*`` entry is dropped — the resolver has nothing to look
    in. Literals pass through. This is the "bronze-only run with no
    cp scope" path: AIDPF-2072 still wants the literal cols, just not
    the alias refs.

    Args:
        entries: the raw ``requiredColumns`` value list as it appears
            on a ``NodeYaml``.
        resolved_pack: the loaded pack (for ``column_aliases`` keys).
            ``None`` when no pack is in scope.
        tenant_profile: the loaded tenant profile (for
            ``resolved.column`` values). ``None`` when no profile is
            in scope.

    Returns:
        Set of resolved physical column names, with ``$column.*`` refs
        substituted via the profile's pinned value where possible.
        Literals pass through verbatim.
    """
    resolved: set[str] = set()
    pack_alias_keys: set[str] = set()
    if resolved_pack is not None:
        pack_alias_keys = set(resolved_pack.pack.column_aliases.keys())

    for entry in entries:
        if not entry.startswith(_COLUMN_REF_PREFIX):
            resolved.add(entry)
            continue
        if tenant_profile is None or resolved_pack is None:
            continue
        key = entry[len(_COLUMN_REF_PREFIX):]
        if key not in pack_alias_keys:
            continue
        physical = tenant_profile.resolved.column.get(key)
        if not physical:
            continue
        resolved.add(physical)

    return resolved


__all__ = ["resolve_required_column_entries"]

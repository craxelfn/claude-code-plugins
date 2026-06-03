"""Cross-boundary error classes (P1.5ε §Step 1a).

Houses the subset of orchestrator error classes that schema-level modules
(``schema.bundle.load_bundle``, ``schema.plan_resolver.resolve_dry_run_plan``)
need to raise, and that downstream consumers (dispatch package, CLI) need to
catch. Schema-level callers raising from ``orchestrator.errors`` would
transitively import ``orchestrator/__init__.py`` and load every engine
subsystem — a direct violation of the dispatch import-boundary rule.

``orchestrator.errors`` re-exports these names for back-compat so every
existing engine-side import path continues to resolve. Identity is preserved:
``orchestrator.errors.BundleLoadError is schema.errors.BundleLoadError``.
"""

from __future__ import annotations


class OrchestratorConfigError(Exception):
    """Marker base class for user-facing config / pre-dispatch errors.

    The CLI catches ``OrchestratorConfigError`` and exits 2 with ``str(exc)``
    — no traceback. Subclasses must produce a self-explanatory ``__str__``
    (the CLI prints it verbatim without extra framing).

    Engine-only subclasses (``UnsupportedModeError``, ``PrerequisiteError``,
    ``CredentialResolutionError``, ``IncrementalCursorMissingError``, etc.)
    live in ``orchestrator.errors`` and still inherit from this class — the
    single ``except OrchestratorConfigError:`` clause in the CLI catches
    them all.
    """


class BundleLoadError(OrchestratorConfigError):
    """Wraps every bundle.yaml load failure into one class so the CLI's
    exit-2 path catches them uniformly. Failure modes: file unreadable,
    YAML parse error, env-var missing, Pydantic schema violation, bad
    ``aidp.*`` SQL identifier.
    """


class BundleVersionMismatchError(BundleLoadError):
    """Raised when ``bundle.version`` is unknown to this plugin build.
    The message names the offending version + the supported set + the
    ``aidp-fusion-bundle migrate-bundle`` remediation command. Inherits
    from ``BundleLoadError`` because it is a load failure with a specific
    remediation; the CLI's exit-2 catch picks it up via the transitive
    ``OrchestratorConfigError`` ancestor.
    """


class MissingDependencyError(OrchestratorConfigError):
    """Logical missing dependency — a bundle.yaml name that doesn't
    resolve in any registry (BRONZE_EXTRACTS / SILVER_DIMS / GOLD_MARTS /
    KNOWN_DEFERRED_*), or a ``--datasets`` / ``--layers`` filter naming a
    value that doesn't exist. Raised by the schema-level plan resolver
    and by engine-side ``_resolve_*`` functions.
    """


__all__ = [
    "OrchestratorConfigError",
    "BundleLoadError",
    "BundleVersionMismatchError",
    "MissingDependencyError",
]

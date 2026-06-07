"""Dispatch-layer error taxonomy (P1.5ε §Step 7 — error codes).

Stable ``DISPATCH_*`` codes for the operator-facing message format. Each
class carries one ``code`` class-var; ``__str__`` renders as
``[CODE] message`` so the CLI's red one-liner stays predictable.

The dispatch entry point catches :class:`AidpRestError` at every REST
call site and wraps it into the matching subclass below — the underlying
exception is preserved as ``__cause__`` for ``--verbose`` consumers. The
CLI's ``except (DispatchError, OrchestratorConfigError)`` catch never
sees a bare ``AidpRestError`` escape from ``dispatch_via_rest``.

Distinct from :class:`OrchestratorConfigError` — those are user-facing
config bugs (multi-inherits ``ValueError``). Dispatch errors are network
/ state errors that don't fit the config-error shape.
"""

from __future__ import annotations

from typing import ClassVar


class DispatchError(Exception):
    """Base for dispatch-layer errors. Carries a stable ``code`` class var."""

    code: ClassVar[str] = "DISPATCH_UNKNOWN"

    def __str__(self) -> str:
        msg = super().__str__()
        return f"[{self.code}] {msg}" if msg else f"[{self.code}]"


class DispatchConfigError(DispatchError):
    code: ClassVar[str] = "DISPATCH_CONFIG_INVALID"


class DispatchAuthError(DispatchError):
    """OCI signer construction failed (missing profile, expired session,
    bad keys) at :meth:`AidpRestClient.__init__`."""

    code: ClassVar[str] = "DISPATCH_AUTH_OCI"


class DispatchPreflightError(DispatchError):
    """One or more preflight checks failed. Message includes a one-line
    summary per failed check + its remediation hint."""

    code: ClassVar[str] = "DISPATCH_PREFLIGHT_FAILED"


class DispatchWheelBuildError(DispatchError):
    code: ClassVar[str] = "DISPATCH_WHEEL_BUILD_FAILED"


class DispatchClusterNotActiveError(DispatchError):
    """Cluster STOPPED with ``auto_start=False`` OR FAILED / CREATING.
    Currently surfaced through DispatchPreflightError (Phase B check
    5); reserved for future direct use."""

    code: ClassVar[str] = "DISPATCH_CLUSTER_NOT_ACTIVE"


class DispatchUploadError(DispatchError):
    """Contents-API PUT returned non-2xx. Wraps :class:`AidpRestError`."""

    code: ClassVar[str] = "DISPATCH_UPLOAD_HTTP"


class DispatchJobSubmitError(DispatchError):
    """``POST /jobs`` or ``POST /jobRuns`` returned non-2xx. The most
    CircuitBreaker-trippy code path."""

    code: ClassVar[str] = "DISPATCH_JOB_SUBMIT"


class DispatchPollTimeoutError(DispatchError):
    """``poll_run`` deadline exceeded. Distinct from RUN_FAILED — this is
    laptop-side patience exhaustion, not cluster-side run failure."""

    code: ClassVar[str] = "DISPATCH_TIMEOUT"


class DispatchRunFailedError(DispatchError):
    """Job reached terminal status ``FAILED`` / ``CANCELED`` / ``TIMED_OUT``."""

    code: ClassVar[str] = "DISPATCH_RUN_FAILED"


class DispatchFetchOutputError(DispatchError):
    """``fetchOutput`` returned non-200 OR ``data[0].value`` missing.
    Distinct from MARKER_MISSING — the API call itself failed."""

    code: ClassVar[str] = "DISPATCH_FETCH_OUTPUT"


class DispatchMarkerMissingError(DispatchError):
    """Job reported SUCCESS but no ``AIDP_LIVE_TEST_RESULT_BEGIN/END``
    marker was found in the executed notebook. Evidence-capture failure
    — exit 2, not 1, because the run summary is unavailable."""

    code: ClassVar[str] = "DISPATCH_MARKER_MISSING"


class DispatchMarkerEnvelopeMissing(DispatchError):
    """Phase 4.1 / D3 — ``parse_marker`` walked every cell + output
    channel and never found the configured ``MARKER_BEGIN``/``MARKER_END``
    envelope. Sibling of :class:`DispatchMarkerMissingError` but
    carries the executed notebook + extracted stdout excerpt so the
    caller can write the AIDPF-2049 ``cluster_stdout.log`` companion
    (the existing class is kept for backward compat with the run
    dispatcher, which doesn't need the payload)."""

    code: ClassVar[str] = "DISPATCH_MARKER_ENVELOPE_MISSING"

    def __init__(
        self,
        msg: str,
        *,
        executed_notebook: dict | None = None,
        stdout_excerpt: str = "",
    ) -> None:
        super().__init__(msg)
        self.executed_notebook = executed_notebook
        self.stdout_excerpt = stdout_excerpt


class DispatchMarkerDecodeError(DispatchError):
    """Phase 4.1 / D3 — envelope was found but base64 / JSON decoding
    failed. Same attributes as :class:`DispatchMarkerEnvelopeMissing`
    — the AIDPF-2049 companion log needs the same payload regardless
    of the precise failure mode."""

    code: ClassVar[str] = "DISPATCH_MARKER_DECODE"

    def __init__(
        self,
        msg: str,
        *,
        executed_notebook: dict | None = None,
        stdout_excerpt: str = "",
    ) -> None:
        super().__init__(msg)
        self.executed_notebook = executed_notebook
        self.stdout_excerpt = stdout_excerpt


__all__ = [
    "DispatchAuthError",
    "DispatchClusterNotActiveError",
    "DispatchConfigError",
    "DispatchError",
    "DispatchFetchOutputError",
    "DispatchJobSubmitError",
    "DispatchMarkerDecodeError",
    "DispatchMarkerEnvelopeMissing",
    "DispatchMarkerMissingError",
    "DispatchPollTimeoutError",
    "DispatchPreflightError",
    "DispatchRunFailedError",
    "DispatchUploadError",
    "DispatchWheelBuildError",
]

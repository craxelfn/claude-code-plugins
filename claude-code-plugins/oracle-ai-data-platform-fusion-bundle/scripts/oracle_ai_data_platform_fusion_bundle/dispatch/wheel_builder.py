"""Content-hash wheel cache (P1.5ε §Step 3).

Iterating on a bug fix shouldn't pay the 30–60s ``python -m build`` tax on
every ``aidp-fusion-bundle run`` invocation. The cache key is a SHA256 over
the runtime-affecting source files (pyproject.toml + the plugin's Python
modules). Test files don't affect runtime behavior and are excluded from
the hash so editing a test never invalidates the cache.

Cache layout::

    ~/.aidp/wheels/
      oracle_ai_data_platform_fusion_bundle-<hash16>.whl

No automatic garbage collection — the operator nukes the directory by hand
if it grows too large. Tracked as a P3.x polish item.
"""

from __future__ import annotations

import hashlib
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path


_DEFAULT_CACHE_DIR = Path.home() / ".aidp" / "wheels"

# Paths under the checkout whose content affects runtime behavior.
_HASH_INCLUDE_GLOBS: tuple[str, ...] = (
    "pyproject.toml",
    "scripts/oracle_ai_data_platform_fusion_bundle/**/*.py",
)

# Skip files matching any of these path-substring filters (post-glob).
# Keeps test files + bytecode caches out of the runtime hash.
_HASH_EXCLUDE_SUBSTRINGS: tuple[str, ...] = (
    "/__pycache__/",
    "/tests/",
)


class DispatchWheelBuildError(RuntimeError):
    """``python -m build`` returned non-zero. Carries stdout/stderr in the
    message for operator diagnosis without re-running."""


def _compute_source_hash(plugin_checkout: Path) -> str:
    """Hash sorted (relative_path, file_bytes) pairs for every runtime-
    affecting source file. First 16 hex chars of SHA256."""
    digest = hashlib.sha256()
    seen: list[tuple[str, bytes]] = []
    for pattern in _HASH_INCLUDE_GLOBS:
        for path in sorted(plugin_checkout.glob(pattern)):
            if not path.is_file():
                continue
            rel = path.relative_to(plugin_checkout).as_posix()
            if any(skip in f"/{rel}" for skip in _HASH_EXCLUDE_SUBSTRINGS):
                continue
            seen.append((rel, path.read_bytes()))
    seen.sort(key=lambda kv: kv[0])
    for rel, content in seen:
        digest.update(rel.encode("utf-8"))
        digest.update(b"\0")
        digest.update(content)
        digest.update(b"\0")
    return digest.hexdigest()[:16]


def build_wheel(
    *,
    plugin_checkout: Path,
    cache_dir: Path | None = None,
    force_rebuild: bool = False,
    log: Callable[[str], None] = lambda msg: None,
) -> Path:
    """Build the plugin wheel, caching by content hash of source + pyproject.

    Cache hit → return the cached path without invoking the build subprocess.
    Cache miss or ``force_rebuild=True`` → run ``python -m build --wheel
    --outdir <tmpdir>``, copy the resulting wheel into the cache, return
    the cached path.

    Raises:
        DispatchWheelBuildError: ``python -m build`` returned non-zero.
    """
    cache_dir = cache_dir if cache_dir is not None else _DEFAULT_CACHE_DIR
    cache_dir.mkdir(parents=True, exist_ok=True)

    source_hash = _compute_source_hash(plugin_checkout)
    cached_wheel = cache_dir / (
        f"oracle_ai_data_platform_fusion_bundle-{source_hash}.whl"
    )

    if cached_wheel.exists() and not force_rebuild:
        log(f"wheel cache hit: {cached_wheel.name}")
        return cached_wheel

    log(f"wheel cache miss (hash={source_hash}); running `python -m build`")
    with tempfile.TemporaryDirectory(prefix="aidp-wheel-build-") as tmpdir:
        outdir = Path(tmpdir)
        proc = subprocess.run(
            [sys.executable, "-m", "build", "--wheel", "--outdir", str(outdir)],
            cwd=str(plugin_checkout),
            capture_output=True,
            text=True,
            timeout=180,
            check=False,
        )
        if proc.returncode != 0:
            raise DispatchWheelBuildError(
                f"`python -m build` failed with rc={proc.returncode}\n"
                f"stdout:\n{proc.stdout}\n"
                f"stderr:\n{proc.stderr}"
            )
        built_wheels = sorted(outdir.glob("*.whl"))
        if not built_wheels:
            raise DispatchWheelBuildError(
                f"`python -m build` returned rc=0 but no .whl found in {outdir}"
            )
        # `python -m build` produces exactly one wheel per --wheel run.
        shutil.copy2(built_wheels[0], cached_wheel)
        log(f"wheel cached: {cached_wheel.name}")
        return cached_wheel


__all__ = [
    "DispatchWheelBuildError",
    "build_wheel",
]

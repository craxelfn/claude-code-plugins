"""Schema for the ``--resolutions <json-file>`` flag (PLAN §9.5.5 / §9.5.9).

Bootstrap accepts a scripted-resolution file for multi-match cases so
feature #3 (``v2-phase-3b-medallion-author-skill``) can commit overlay
choices without driving a terminal. A careful operator can also use
this in CI to make a deterministic re-bootstrap reproducible.

The file is JSON. Validation runs in two layers:

1. **Pydantic schema** — :class:`ResolutionsInputV1`: rejects unknown
   keys via ``extra="forbid"``, validates ``kind`` against the enum,
   rejects empty input.
2. **Pack-aware semantic validation** — :func:`validate_against_pack`:
   checks every entry against the resolved pack's declared variation
   points + the walker's matched-candidate set (see plan Step 8's
   "Resolutions file schema" section for the 7 rules).
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class ScriptedResolution(BaseModel):
    """One scripted multi-match resolution.

    Accepts both ``chosen_candidate`` (snake) and ``chosenCandidate``
    (camel) via the field alias + ``populate_by_name=True``.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    name: str
    """Variation-point name (must exist in the resolved pack)."""

    kind: Literal["columnAliases", "semanticVariants"]
    """Must match the declared kind for ``name``."""

    chosen_candidate: str = Field(alias="chosenCandidate")
    """Logical id of the candidate to pin. For ``columnAliases`` this is
    a physical column name; for ``semanticVariants`` it's the candidate
    ``id`` (e.g. ``cancelled_date``)."""


class ResolutionsInputV1(BaseModel):
    """Top-level schema for the ``--resolutions`` file."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    schema_version: Literal[1] = Field(default=1, alias="schemaVersion")
    tenant: str
    """Must match ``bundle.contentPack.profile`` — rejected with
    :class:`ResolutionsFileTenantMismatch` otherwise."""

    resolutions: list[ScriptedResolution]


# ---------------------------------------------------------------------------
# Pack-aware validators
# ---------------------------------------------------------------------------


class ResolutionsFileError(Exception):
    """Base class for ``--resolutions`` validation errors."""


class ResolutionsFileTenantMismatch(ResolutionsFileError):
    """``resolutions.tenant`` does not match ``bundle.contentPack.profile``."""


class ResolutionsFileUnknownEntry(ResolutionsFileError):
    """A resolution entry names a variation point the pack does not declare."""


class ResolutionsFileKindMismatch(ResolutionsFileError):
    """A resolution entry's ``kind`` does not match the declared kind."""


class ResolutionsFileBadCandidate(ResolutionsFileError):
    """A resolution entry's ``chosenCandidate`` is not in the variation
    point's matched candidate set (either not declared or not observed
    in the current bronze schema)."""


class ResolutionsFileDuplicate(ResolutionsFileError):
    """Two entries share the same ``(name, kind)`` pair."""


class ResolutionsFileExtraneousEntry(ResolutionsFileError):
    """A resolution entry targets a variation point whose walker outcome
    was :class:`AutoResolved` or :class:`NoMatch` — neither needs the
    operator's input."""


class ResolutionsFileIncomplete(ResolutionsFileError):
    """A multi-match variation point exists for which the file has no
    entry. Operator must either provide an entry or omit ``--resolutions``
    entirely (falling back to interactive / non-interactive defaults)."""


def validate_against_pack(
    *,
    input_data: ResolutionsInputV1,
    expected_tenant: str,
    column_alias_names: set[str],
    semantic_variant_names: set[str],
    walker_outcomes: dict[tuple[str, str], list[str]],
) -> None:
    """Apply the pack-aware validation rules to a parsed
    :class:`ResolutionsInputV1`.

    Args:
        input_data: the parsed file contents.
        expected_tenant: ``bundle.contentPack.profile``.
        column_alias_names: set of declared ``columnAliases`` names in
            the resolved pack.
        semantic_variant_names: set of declared ``semanticVariants``
            names in the resolved pack.
        walker_outcomes: ``{(name, kind): [matched_candidate, ...]}``
            for every variation point with a :class:`MultiMatch`
            outcome. AutoResolved / NoMatch variation points should
            NOT appear in this map; entries for them in the file are
            rejected as extraneous.

    Raises:
        ResolutionsFileError or a more specific subclass on the first
        rule violation found.
    """
    if input_data.tenant != expected_tenant:
        raise ResolutionsFileTenantMismatch(
            f"resolutions file tenant {input_data.tenant!r} does not match "
            f"bundle.contentPack.profile {expected_tenant!r}"
        )

    seen: set[tuple[str, str]] = set()
    for entry in input_data.resolutions:
        key = (entry.name, entry.kind)
        # Rule 5: no duplicates.
        if key in seen:
            raise ResolutionsFileDuplicate(
                f"duplicate resolution entry for ({entry.name!r}, {entry.kind!r})"
            )
        seen.add(key)

        # Rule 2 + Rule 3: name + kind match declared variation point.
        if entry.kind == "columnAliases":
            if entry.name not in column_alias_names:
                raise ResolutionsFileUnknownEntry(
                    f"columnAliases entry {entry.name!r} is not declared in "
                    f"the resolved pack"
                )
        else:  # semanticVariants
            if entry.name not in semantic_variant_names:
                raise ResolutionsFileUnknownEntry(
                    f"semanticVariants entry {entry.name!r} is not declared "
                    f"in the resolved pack"
                )
        # Rule 3: detect kind mismatch between the entry and the pack's
        # declared kind for that name.
        in_columns = entry.name in column_alias_names
        in_semantics = entry.name in semantic_variant_names
        if in_columns and entry.kind != "columnAliases":
            raise ResolutionsFileKindMismatch(
                f"entry for {entry.name!r} declares kind={entry.kind!r} "
                f"but pack declares it as columnAliases"
            )
        if in_semantics and entry.kind != "semanticVariants":
            raise ResolutionsFileKindMismatch(
                f"entry for {entry.name!r} declares kind={entry.kind!r} "
                f"but pack declares it as semanticVariants"
            )

        # Rule 4 + Rule 7: candidate must be in the current MultiMatch set.
        if key not in walker_outcomes:
            raise ResolutionsFileExtraneousEntry(
                f"variation point ({entry.name!r}, {entry.kind!r}) is not "
                f"a multi-match — either it auto-resolved or it has no match. "
                f"Remove the entry."
            )
        if entry.chosen_candidate not in walker_outcomes[key]:
            raise ResolutionsFileBadCandidate(
                f"chosenCandidate {entry.chosen_candidate!r} for "
                f"({entry.name!r}, {entry.kind!r}) is not in the matched "
                f"candidate set {walker_outcomes[key]}"
            )

    # Rule 6: every multi-match must have an entry.
    missing = sorted(walker_outcomes.keys() - seen)
    if missing:
        raise ResolutionsFileIncomplete(
            f"--resolutions file is missing entries for multi-match "
            f"variation points: {missing}"
        )


__all__ = [
    "ResolutionsFileBadCandidate",
    "ResolutionsFileDuplicate",
    "ResolutionsFileError",
    "ResolutionsFileExtraneousEntry",
    "ResolutionsFileIncomplete",
    "ResolutionsFileKindMismatch",
    "ResolutionsFileTenantMismatch",
    "ResolutionsFileUnknownEntry",
    "ResolutionsInputV1",
    "validate_against_pack",
]

"""Unit tests for the ``--resolutions`` file Pydantic schema + the
pack-aware semantic validator.

Covers each of the 7 validation rules documented in plan Step 8's
"Resolutions file schema" section.
"""

from __future__ import annotations

import json

import pytest

from oracle_ai_data_platform_fusion_bundle.schema.resolutions_input import (
    ResolutionsFileBadCandidate,
    ResolutionsFileDuplicate,
    ResolutionsFileExtraneousEntry,
    ResolutionsFileIncomplete,
    ResolutionsFileKindMismatch,
    ResolutionsFileTenantMismatch,
    ResolutionsFileUnknownEntry,
    ResolutionsInputV1,
    validate_against_pack,
)


# Standard pack-aware validator inputs.
_DECLARED_COLUMNS = {"invoice_currency_code", "supplier_natural_key"}
_DECLARED_SEMANTICS = {"cancelled_status"}


def _valid_input(**overrides):
    base = {
        "schemaVersion": 1,
        "tenant": "finance-default",
        "resolutions": [
            {
                "name": "invoice_currency_code",
                "kind": "columnAliases",
                "chosenCandidate": "ApInvoicesInvoiceCurrencyCode",
            }
        ],
    }
    base.update(overrides)
    return ResolutionsInputV1.model_validate(base)


class TestPydanticLayer:
    def test_camel_and_snake_case_both_accepted(self) -> None:
        # camel.
        a = ResolutionsInputV1.model_validate(
            {
                "schemaVersion": 1,
                "tenant": "t",
                "resolutions": [
                    {
                        "name": "x",
                        "kind": "columnAliases",
                        "chosenCandidate": "y",
                    }
                ],
            }
        )
        assert a.resolutions[0].chosen_candidate == "y"

        # snake.
        b = ResolutionsInputV1.model_validate(
            {
                "schema_version": 1,
                "tenant": "t",
                "resolutions": [
                    {
                        "name": "x",
                        "kind": "columnAliases",
                        "chosen_candidate": "y",
                    }
                ],
            }
        )
        assert b.resolutions[0].chosen_candidate == "y"

    def test_unknown_top_level_key_rejected(self) -> None:
        with pytest.raises(Exception):
            ResolutionsInputV1.model_validate(
                {
                    "schemaVersion": 1,
                    "tenant": "t",
                    "resolutions": [],
                    "extra": "bad",
                }
            )

    def test_unknown_entry_key_rejected(self) -> None:
        with pytest.raises(Exception):
            ResolutionsInputV1.model_validate(
                {
                    "schemaVersion": 1,
                    "tenant": "t",
                    "resolutions": [
                        {
                            "name": "x",
                            "kind": "columnAliases",
                            "chosenCandidate": "y",
                            "extra": "bad",
                        }
                    ],
                }
            )

    def test_invalid_kind_rejected(self) -> None:
        with pytest.raises(Exception):
            ResolutionsInputV1.model_validate(
                {
                    "schemaVersion": 1,
                    "tenant": "t",
                    "resolutions": [
                        {
                            "name": "x",
                            "kind": "NOT_A_KIND",
                            "chosenCandidate": "y",
                        }
                    ],
                }
            )


class TestPackAwareValidation:
    def test_tenant_mismatch_rule_1(self) -> None:
        input_data = _valid_input(tenant="wrong-tenant")
        with pytest.raises(ResolutionsFileTenantMismatch):
            validate_against_pack(
                input_data=input_data,
                expected_tenant="finance-default",
                column_alias_names=_DECLARED_COLUMNS,
                semantic_variant_names=_DECLARED_SEMANTICS,
                walker_outcomes={
                    ("invoice_currency_code", "columnAliases"): [
                        "ApInvoicesInvoiceCurrencyCode",
                        "ApInvoicesCurrencyCode",
                    ],
                },
            )

    def test_unknown_name_rule_2(self) -> None:
        input_data = ResolutionsInputV1.model_validate(
            {
                "schemaVersion": 1,
                "tenant": "finance-default",
                "resolutions": [
                    {
                        "name": "not_declared",
                        "kind": "columnAliases",
                        "chosenCandidate": "X",
                    }
                ],
            }
        )
        with pytest.raises(ResolutionsFileUnknownEntry):
            validate_against_pack(
                input_data=input_data,
                expected_tenant="finance-default",
                column_alias_names=_DECLARED_COLUMNS,
                semantic_variant_names=_DECLARED_SEMANTICS,
                walker_outcomes={},
            )

    def test_kind_mismatch_rule_3(self) -> None:
        # Declare cancelled_status as columnAliases — wrong kind for the pack.
        input_data = ResolutionsInputV1.model_validate(
            {
                "schemaVersion": 1,
                "tenant": "finance-default",
                "resolutions": [
                    {
                        "name": "cancelled_status",
                        "kind": "columnAliases",  # but it's semantic
                        "chosenCandidate": "cancelled_date",
                    }
                ],
            }
        )
        with pytest.raises((ResolutionsFileKindMismatch, ResolutionsFileUnknownEntry)):
            validate_against_pack(
                input_data=input_data,
                expected_tenant="finance-default",
                column_alias_names=_DECLARED_COLUMNS,
                semantic_variant_names=_DECLARED_SEMANTICS,
                walker_outcomes={},
            )

    def test_bad_candidate_rule_4(self) -> None:
        input_data = _valid_input()
        with pytest.raises(ResolutionsFileBadCandidate):
            validate_against_pack(
                input_data=input_data,
                expected_tenant="finance-default",
                column_alias_names=_DECLARED_COLUMNS,
                semantic_variant_names=_DECLARED_SEMANTICS,
                walker_outcomes={
                    ("invoice_currency_code", "columnAliases"): [
                        # Pinned candidate not in matched set.
                        "ApInvoicesCurrencyCode",
                    ],
                },
            )

    def test_duplicate_rule_5(self) -> None:
        input_data = ResolutionsInputV1.model_validate(
            {
                "schemaVersion": 1,
                "tenant": "finance-default",
                "resolutions": [
                    {
                        "name": "invoice_currency_code",
                        "kind": "columnAliases",
                        "chosenCandidate": "ApInvoicesInvoiceCurrencyCode",
                    },
                    {
                        "name": "invoice_currency_code",
                        "kind": "columnAliases",
                        "chosenCandidate": "ApInvoicesCurrencyCode",
                    },
                ],
            }
        )
        with pytest.raises(ResolutionsFileDuplicate):
            validate_against_pack(
                input_data=input_data,
                expected_tenant="finance-default",
                column_alias_names=_DECLARED_COLUMNS,
                semantic_variant_names=_DECLARED_SEMANTICS,
                walker_outcomes={
                    ("invoice_currency_code", "columnAliases"): [
                        "ApInvoicesInvoiceCurrencyCode",
                        "ApInvoicesCurrencyCode",
                    ],
                },
            )

    def test_incomplete_rule_6(self) -> None:
        # Walker produced two multi-matches; file covers only one.
        input_data = _valid_input()
        with pytest.raises(ResolutionsFileIncomplete):
            validate_against_pack(
                input_data=input_data,
                expected_tenant="finance-default",
                column_alias_names=_DECLARED_COLUMNS,
                semantic_variant_names=_DECLARED_SEMANTICS,
                walker_outcomes={
                    ("invoice_currency_code", "columnAliases"): [
                        "ApInvoicesInvoiceCurrencyCode",
                        "ApInvoicesCurrencyCode",
                    ],
                    ("cancelled_status", "semanticVariants"): [
                        "cancelled_date",
                        "cancelled_flag",
                    ],
                },
            )

    def test_extraneous_rule_7(self) -> None:
        # File entry targets a variation point that was AutoResolved
        # (not in walker_outcomes' MultiMatch map).
        input_data = ResolutionsInputV1.model_validate(
            {
                "schemaVersion": 1,
                "tenant": "finance-default",
                "resolutions": [
                    {
                        "name": "supplier_natural_key",
                        "kind": "columnAliases",
                        "chosenCandidate": "SEGMENT1",
                    }
                ],
            }
        )
        with pytest.raises(ResolutionsFileExtraneousEntry):
            validate_against_pack(
                input_data=input_data,
                expected_tenant="finance-default",
                column_alias_names=_DECLARED_COLUMNS,
                semantic_variant_names=_DECLARED_SEMANTICS,
                walker_outcomes={},  # supplier_natural_key was AutoResolved
            )

    def test_happy_path_passes(self) -> None:
        input_data = _valid_input()
        # No raise == pass.
        validate_against_pack(
            input_data=input_data,
            expected_tenant="finance-default",
            column_alias_names=_DECLARED_COLUMNS,
            semantic_variant_names=_DECLARED_SEMANTICS,
            walker_outcomes={
                ("invoice_currency_code", "columnAliases"): [
                    "ApInvoicesInvoiceCurrencyCode",
                    "ApInvoicesCurrencyCode",
                ],
            },
        )

"""Tests for the ArtisanError envelope."""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from artisan.errors import (
    ArtisanError,
    ArtisanErrorEnvelope,
    ErrorCode,
    _default_doc_uri,
    suggest,
)


class TestArtisanErrorEnvelope:
    """Construction and shape of the envelope."""

    def test_envelope_round_trips_through_json(self) -> None:
        err = ArtisanError(
            code=ErrorCode.PARAM_TYPE_MISMATCH,
            error_type="validation",
            message="Unknown params for op: ['multiplyer']",
            operation_name="data_transformer",
            field="params.multiplyer",
            suggestions=["multiplier"],
            recovery_hint="CHECK_INPUT",
        )

        as_dict = err.to_dict()
        round_tripped = json.loads(json.dumps(as_dict))
        assert round_tripped == as_dict
        assert round_tripped["code"] == "param_type_mismatch"
        assert round_tripped["error_type"] == "validation"
        assert round_tripped["recovery_hint"] == "CHECK_INPUT"
        assert round_tripped["suggestions"] == ["multiplier"]

    def test_default_doc_uri_uses_code(self) -> None:
        err = ArtisanError(
            code=ErrorCode.OP_PARAMS_UNDOCUMENTED,
            error_type="config",
            message="x",
        )
        assert (
            err.envelope.doc_uri
            == "https://artisan.dev/docs/errors/op_params_undocumented"
        )

    def test_explicit_doc_uri_overrides_default(self) -> None:
        err = ArtisanError(
            code=ErrorCode.PARAM_TYPE_MISMATCH,
            error_type="validation",
            message="x",
            doc_uri="https://example.com/custom",
        )
        assert err.envelope.doc_uri == "https://example.com/custom"

    def test_default_doc_uri_helper_is_format_stable(self) -> None:
        assert _default_doc_uri("foo") == "https://artisan.dev/docs/errors/foo"

    def test_properties_proxy_envelope(self) -> None:
        err = ArtisanError(
            code=ErrorCode.PARAM_TYPE_MISMATCH,
            error_type="validation",
            message="x",
        )
        assert err.code == "param_type_mismatch"
        assert err.error_type == "validation"


class TestErrorTypeRequired:
    """``error_type`` has no default — every raise site must declare one."""

    def test_envelope_rejects_missing_error_type(self) -> None:
        with pytest.raises(ValidationError):
            ArtisanErrorEnvelope(code="x", message="y")  # type: ignore[call-arg]

    def test_artisan_error_signature_requires_error_type(self) -> None:
        # Missing the kw-only ``error_type`` triggers TypeError from Python
        # before Pydantic sees the call.
        with pytest.raises(TypeError):
            ArtisanError(code="x", message="y")  # type: ignore[call-arg]


class TestCauseChain:
    """``raise X from Y`` should serialize the cause one level deep."""

    def test_cause_chain_included_in_to_dict(self) -> None:
        cause_msg = "multiplier must be positive"
        try:
            try:
                raise ValueError(cause_msg)
            except ValueError as exc:
                raise ArtisanError(
                    code=ErrorCode.OP_EXECUTE_FAILED,
                    error_type="runtime",
                    message="Operation failed during execute().",
                    recovery_hint="REPORT_TO_USER",
                ) from exc
        except ArtisanError as err:
            payload = err.to_dict()

        assert "cause" in payload
        assert payload["cause"]["type"] == "ValueError"
        assert payload["cause"]["message"] == "multiplier must be positive"
        # format_error returns either a full traceback or "Type: message"
        # when no __traceback__ is attached.
        assert (
            "Traceback" in payload["cause"]["traceback"]
            or "ValueError" in payload["cause"]["traceback"]
        )

    def test_to_dict_omits_cause_when_no_chain(self) -> None:
        err = ArtisanError(
            code=ErrorCode.PARAM_TYPE_MISMATCH,
            error_type="validation",
            message="x",
        )
        assert "cause" not in err.to_dict()

    def test_include_cause_false_drops_cause(self) -> None:
        inner_msg = "inner"
        try:
            try:
                raise RuntimeError(inner_msg)
            except RuntimeError as exc:
                raise ArtisanError(
                    code=ErrorCode.OP_EXECUTE_FAILED,
                    error_type="runtime",
                    message="outer",
                ) from exc
        except ArtisanError as err:
            assert "cause" not in err.to_dict(include_cause=False)


class TestSuggest:
    """``suggest`` provides did-you-mean candidates."""

    def test_returns_close_match(self) -> None:
        assert suggest("multiplyer", ["multiplier", "scale", "seed"]) == ["multiplier"]

    def test_respects_n(self) -> None:
        # Two candidates similar to "fo"; n=1 returns only the closest.
        result = suggest("fo", ["foo", "for", "bar"], n=1, cutoff=0.4)
        assert len(result) == 1

    def test_returns_empty_when_no_match_above_cutoff(self) -> None:
        assert suggest("xyz", ["alpha", "beta"]) == []

    def test_accepts_set_input(self) -> None:
        assert suggest("multiplyer", {"multiplier", "scale"}) == ["multiplier"]

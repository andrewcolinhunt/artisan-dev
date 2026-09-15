"""Tests for deployment-owned endpoint data policies."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from artisan.schemas.operation_config import (
    ToolEndpointDataPolicy as PublicToolEndpointDataPolicy,
)
from artisan.schemas.operation_config.endpoint_policy import ToolEndpointDataPolicy


class TestToolEndpointDataPolicy:
    def test_operation_config_exports_policy(self):
        assert PublicToolEndpointDataPolicy is ToolEndpointDataPolicy

    def test_empty_policy_denies_remote_access(self):
        policy = ToolEndpointDataPolicy()

        with pytest.raises(ValueError, match="input URI is not allowed"):
            policy.authorize_input("s3://bucket/key")
        with pytest.raises(ValueError, match="output URI is not allowed"):
            policy.authorize_output("https://uploads.example/key?signature=secret")

    def test_directional_lists_are_independent(self):
        policy = ToolEndpointDataPolicy(
            input_allowlist=("s3://bucket/reads",),
            output_allowlist=("s3://bucket/writes",),
        )

        assert (
            policy.authorize_input("s3://bucket/reads/item").transport_target
            == "s3://bucket/reads/item"
        )
        with pytest.raises(ValueError, match="input URI is not allowed"):
            policy.authorize_input("s3://bucket/writes/item")
        with pytest.raises(ValueError, match="output URI is not allowed"):
            policy.authorize_output("s3://bucket/reads/item")

    @pytest.mark.parametrize(
        "uri",
        [
            "s3://other/team/item",
            "s3://bucket/team-two/item",
            "s3://bucket/tea",
        ],
    )
    def test_s3_matching_uses_bucket_and_segment_prefix(self, uri):
        policy = ToolEndpointDataPolicy(input_allowlist=("s3://bucket/team",))
        with pytest.raises(ValueError, match="not allowed"):
            policy.authorize_input(uri)

    def test_s3_matching_accepts_root_and_descendants(self):
        policy = ToolEndpointDataPolicy(input_allowlist=("s3://bucket/team/",))

        assert policy.input_allowlist == ("s3://bucket/team",)
        assert policy.authorize_input("s3://bucket/team").segments == ("team",)
        assert policy.authorize_input("s3://bucket/team/run/a").segments == (
            "team",
            "run",
            "a",
        )

    def test_s3_target_is_canonicalized_after_one_decode(self):
        policy = ToolEndpointDataPolicy(input_allowlist=("s3://BÜCKET/a b",))

        assert policy.input_allowlist == ("s3://xn--bcket-kva/a%20b",)
        target = policy.authorize_input("S3://BÜCKET/a%20b/caf%C3%A9")
        assert target.transport_target == "s3://xn--bcket-kva/a%20b/caf%C3%A9"

    @pytest.mark.parametrize(
        "uri",
        [
            "s3://bucket/a//b",
            "s3://bucket/a/./b",
            "s3://bucket/a/%2E%2E/b",
            "s3://bucket/a%2Fb",
            "s3://bucket/a%5Cb",
            "s3://bucket/a%3Fb",
            "s3://bucket/a%ZZ",
            "s3://bucket/a?token=secret",
            "s3://bucket:9000/a",
            "s3://bucket:/a",
            "s3://user:secret@bucket/a",
            "s3://buck*/a",
            "s3://[::1]/a",
            "s3://127.0.0.1/a",
            "s3://bucket_name/a",
            "s3://-bucket/a",
            "s3://bucket-/a",
            "file:///tmp/a",
            "memory://bucket/a",
        ],
    )
    def test_unsafe_or_unsupported_s3_roots_are_rejected(self, uri):
        with pytest.raises(ValidationError):
            ToolEndpointDataPolicy(input_allowlist=(uri,))

    def test_encoded_percent_is_not_decoded_twice(self):
        policy = ToolEndpointDataPolicy(input_allowlist=("s3://bucket/a%252Fb",))

        assert policy.input_allowlist == ("s3://bucket/a%252Fb",)
        assert (
            policy.authorize_input("s3://bucket/a%252Fb/item").transport_target
            == "s3://bucket/a%252Fb/item"
        )

    def test_http_origin_matching_is_exact_and_directional(self):
        policy = ToolEndpointDataPolicy(
            input_allowlist=("HTTPS://EXAMPLE.COM.:443/",),
            output_allowlist=("http://example.com:8080",),
        )

        assert policy.input_allowlist == ("https://example.com",)
        assert (
            policy.authorize_input("https://example.com/data?a=secret").transport_target
            == "https://example.com/data?a=secret"
        )
        assert (
            policy.authorize_output("http://example.com:8080/upload?sig=x").safe_display
            == "http://example.com:8080/upload"
        )
        for uri in (
            "http://example.com/data",
            "https://example.com:444/data",
            "https://sub.example.com/data",
        ):
            with pytest.raises(ValueError, match="not allowed"):
                policy.authorize_input(uri)

    def test_http_host_uses_transport_idna_without_alias_collisions(self):
        policy = ToolEndpointDataPolicy(input_allowlist=("https://faß.de",))

        assert policy.input_allowlist == ("https://xn--fa-hia.de",)
        assert (
            policy.authorize_input("https://faß.de/input").transport_target
            == "https://xn--fa-hia.de/input"
        )
        with pytest.raises(ValueError, match="not allowed"):
            policy.authorize_input("https://fass.de/input")

    def test_unicode_terminal_dot_is_normalized_once_and_idempotently(self):
        policy = ToolEndpointDataPolicy(input_allowlist=("https://example.com。",))

        assert policy.input_allowlist == ("https://example.com",)
        assert ToolEndpointDataPolicy.model_validate(policy.model_dump()) == policy

    @pytest.mark.parametrize(
        "root",
        [
            "https://[v1.example]",
            "https://example.com..",
            " https://example.com",
            "https://example.com?",
            "https://example.com#",
            "s3://bucket/path?",
            "s3://bucket/path#",
        ],
    )
    def test_ambiguous_authority_and_empty_delimiters_are_rejected(self, root):
        with pytest.raises(ValidationError):
            ToolEndpointDataPolicy(input_allowlist=(root,))

    def test_http_capability_requires_explicit_path(self):
        policy = ToolEndpointDataPolicy(input_allowlist=("https://example.com",))

        for uri in ("https://example.com", "https://example.com?sig=x"):
            with pytest.raises(ValueError, match="input URI is invalid"):
                policy.authorize_input(uri)

    @pytest.mark.parametrize(
        "root",
        [
            "https://example.com/path",
            "https://example.com?query=x",
            "https://example.com/#fragment",
            "https://user:password@example.com",
            "https://*.example.com",
            "ftp://example.com",
            "example.com",
        ],
    )
    def test_http_allowlist_requires_exact_origins(self, root):
        with pytest.raises(ValidationError):
            ToolEndpointDataPolicy(input_allowlist=(root,))

    def test_default_ports_and_ipv6_are_normalized(self):
        policy = ToolEndpointDataPolicy(
            input_allowlist=("http://[2001:0db8::1]:80",),
            output_allowlist=("https://example.com:443",),
        )

        assert policy.input_allowlist == ("http://[2001:db8::1]",)
        assert policy.output_allowlist == ("https://example.com",)

    def test_policy_is_frozen_and_forbids_extra_fields(self):
        policy = ToolEndpointDataPolicy()
        with pytest.raises(ValidationError, match="frozen"):
            policy.input_allowlist = ("s3://bucket",)
        with pytest.raises(ValidationError, match="Extra inputs"):
            ToolEndpointDataPolicy(allow_all=True)

    def test_policy_validation_errors_hide_sensitive_inputs(self):
        secret = "fake-password"
        username = "alice-private"

        with pytest.raises(ValidationError) as exc_info:
            ToolEndpointDataPolicy(
                input_allowlist=(
                    f"https://{username}:{secret}@example.com?signature=fake-token",
                )
            )

        message = str(exc_info.value)
        for value in (username, secret, "signature", "fake-token"):
            assert value not in message

    def test_rejection_removes_credentials_query_fragment_and_signature(self):
        policy = ToolEndpointDataPolicy()
        uri = "https://user:password@example.com/private?signature=fake-secret#token"

        with pytest.raises(ValueError) as exc_info:
            policy.authorize_input(uri)

        message = str(exc_info.value)
        assert "https://example.com/private" in message
        for secret in ("user", "password", "signature", "fake-secret", "token"):
            assert secret not in message

    def test_safe_display_normalizes_matching_default_port(self):
        policy = ToolEndpointDataPolicy()

        with pytest.raises(ValueError) as exc_info:
            policy.authorize_input(
                "https://user:fake-secret@example.com:443/private#token"
            )

        assert "https://example.com/private" in str(exc_info.value)
        assert ":443" not in str(exc_info.value)

    def test_safe_display_is_bounded_and_never_emits_control_characters(self):
        policy = ToolEndpointDataPolicy()
        long_uri = f"https://example.com/{'a' * 1_000}?signature=fake-secret"

        with pytest.raises(ValueError) as exc_info:
            policy.authorize_input(long_uri)

        message = str(exc_info.value)
        assert len(message) < 400
        assert "signature" not in message
        assert "fake-secret" not in message

        with pytest.raises(ValueError) as control_exc:
            policy.authorize_input("https://example.com/a\x1btoken")
        assert "\x1b" not in str(control_exc.value)
        assert "token" not in str(control_exc.value)

    @pytest.mark.parametrize(
        "uri",
        [
            "https://example.com/a%ZZ",
            "https://example.com:/path",
            "https://example.com/a%0Aheader",
            "https://example.com/a?value=%7f",
            "https://example.com/a%C2%80header",
            "https://example.com/a%E2%80%AEheader",
            "https://example.com/a\nheader",
            "https://example.com/a raw-space",
            "https://example.com/path#fragment",
            "https://user@example.com/path",
        ],
    )
    def test_runtime_capabilities_reject_ambiguous_uris(self, uri):
        policy = ToolEndpointDataPolicy(input_allowlist=("https://example.com",))
        with pytest.raises(ValueError, match="input URI is invalid"):
            policy.authorize_input(uri)

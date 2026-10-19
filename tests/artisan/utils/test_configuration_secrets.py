"""Conservative classification shared by declarations and command evidence."""

from __future__ import annotations

import pytest

from artisan.utils.configuration_secrets import (
    credential_name,
    credential_url,
    sensitive_environment,
)


@pytest.mark.parametrize(
    "name", ["api-key", "TOKEN", "db_password", "AWS_ACCESS_KEY_ID", "Auth"]
)
def test_credential_names_keep_existing_recognition(name):
    assert credential_name(name)


@pytest.mark.parametrize(
    "value",
    [
        "https://user:pass@host/path",
        "https://:password-only@host/path",
        "s3://bucket/key?sig=capability",
        "https://host/#fragment",
        "https://[invalid",
    ],
)
def test_credential_url_recognizes_capabilities(value):
    assert credential_url(value)


def test_public_environment_is_explicit_and_does_not_discard_secrets():
    env = {"ROOT": "/opt/application", "MODE": "private-mode"}
    assert sensitive_environment(env) == env
    assert sensitive_environment(env, ("ROOT",)) == {"MODE": "private-mode"}
    assert not credential_url("/opt/application")
    assert not credential_name("ROOT")

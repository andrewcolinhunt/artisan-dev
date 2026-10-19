"""Conservative classification shared by declarations and command evidence."""

from __future__ import annotations

import pytest

from artisan.utils.configuration_secrets import (
    credential_name,
    credential_url,
    sensitive_environment,
    validate_public_env,
)


@pytest.mark.parametrize(
    "name", ["api-key", "TOKEN", "db_password", "AWS_ACCESS_KEY_ID", "Auth"]
)
def test_credential_names_keep_existing_recognition(name):
    assert credential_name(name)


@pytest.mark.parametrize(
    "name",
    [
        "auth",
        "AUTHENTICATION",
        "authorization",
        "authorisation",
        "AuthorisationHeader",
        "http_auth",
        "HTTP-AUTHORIZATION",
        "service.authentication",
        "apiAuthorization",
        "AuthorizationHeader",
        "authHeader",
        "HTTPAuthorization",
        "basicAuth",
        "BASICAUTH",
        "OAuth",
        "oauth",
        "OAuth2",
        "OAuth2Client",
        "AUTHCODE",
        "AUTHCODE2",
        "AUTHKEY",
        "AUTHHEADER",
        "AUTHHEADERS",
        "AUTHORIZATIONHEADER",
        "XAUTHORITY",
    ],
)
def test_authentication_words_and_compounds_remain_sensitive(name):
    assert credential_name(name)
    with pytest.raises(ValueError, match="recognized credential"):
        validate_public_env({name: "opaque-credential-value"}, (name,))


@pytest.mark.parametrize(
    "name",
    [
        "author",
        "authors",
        "authored",
        "AUTHOR",
        "AUTHORS",
        "AUTHORED",
        "authorName",
        "author_id",
        "book.author",
        "coauthors",
        "authorship",
        "authoredMetadata",
        "metadata_authored",
        "inauthentic",
        "unauthorized",
    ],
)
def test_auth_substrings_in_ordinary_words_remain_public(name):
    assert not credential_name(name)
    assert sensitive_environment({name: "ordinary value"}, (name,)) == {}


def test_authored_metadata_stays_public_while_real_auth_aliases_are_redacted():
    from artisan.execution.recording.commands import CommandRecorder
    from artisan.execution.recording.durable import sanitize_metadata

    metadata = {
        "authored": {"selection": ["exact-source"]},
        "author": "ordinary author",
        "authors": ["ordinary author"],
        "basicAuth": "actual-auth-value",
        "alias": "actual-auth-value",
    }
    clean = sanitize_metadata(CommandRecorder(), metadata)
    assert clean["authored"] == metadata["authored"]
    assert clean["author"] == metadata["author"]
    assert clean["authors"] == metadata["authors"]
    assert clean["basicAuth"] == clean["alias"] == "<redacted>"
    assert metadata["basicAuth"] == "actual-auth-value"


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

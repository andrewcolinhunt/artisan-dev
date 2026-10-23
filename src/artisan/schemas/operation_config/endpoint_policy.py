"""Deployment-owned URI policy for tool-endpoint data transport."""

from __future__ import annotations

import ipaddress
import re
import unicodedata
from dataclasses import dataclass
from urllib.parse import SplitResult, quote, unquote, urlsplit, urlunsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator

_SUPPORTED_SCHEMES = frozenset({"s3", "http", "https"})
_HTTP_SCHEMES = frozenset({"http", "https"})
_DEFAULT_PORTS = {"http": 80, "https": 443}
_BAD_PERCENT_ESCAPE = re.compile(r"%(?![0-9A-Fa-f]{2})")
_S3_BUCKET_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?")
_MAX_SAFE_URI_DISPLAY = 256


@dataclass(frozen=True)
class _EndpointUri:
    """One normalized endpoint-data URI."""

    scheme: str
    authority: str
    segments: tuple[str, ...]
    transport_target: str
    safe_display: str


class ToolEndpointDataPolicy(BaseModel):
    """Directional remote-data permissions baked into a tool deployment.

    Inline values require no permission. Remote reads and writes are denied
    unless their normalized S3 prefix or exact HTTP origin appears in the
    corresponding allowlist.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    input_allowlist: tuple[str, ...] = Field(default_factory=tuple)
    output_allowlist: tuple[str, ...] = Field(default_factory=tuple)

    @field_validator("input_allowlist", "output_allowlist", mode="before")
    @classmethod
    def _normalize_allowlist(cls, value: object) -> tuple[str, ...]:
        """Validate and normalize allowlist roots once at construction."""
        if value is None:
            return ()
        if not isinstance(value, (list, tuple)):
            msg = "endpoint data allowlists must be sequences of roots"
            raise ValueError(msg)
        roots: list[str] = []
        for raw in value:
            if not isinstance(raw, str):
                msg = "endpoint data allowlist roots must be strings"
                raise ValueError(msg)
            roots.append(_parse_endpoint_uri(raw, allowlist_root=True).transport_target)
        return tuple(dict.fromkeys(roots))

    def authorize_input(self, uri: str) -> _EndpointUri:
        """Return an authorized input target or raise a sanitized error."""
        return self._authorize(uri, self.input_allowlist, "input")

    def authorize_output(self, uri: str) -> _EndpointUri:
        """Return an authorized output target or raise a sanitized error."""
        return self._authorize(uri, self.output_allowlist, "output")

    @staticmethod
    def _authorize(
        uri: str, allowlist: tuple[str, ...], direction: str
    ) -> _EndpointUri:
        """Match one candidate against one normalized directional list."""
        try:
            candidate = _parse_endpoint_uri(uri, allowlist_root=False)
        except (TypeError, ValueError) as exc:
            msg = f"{direction} URI is invalid: {safe_uri_display(uri)}"
            raise ValueError(msg) from exc
        roots = tuple(
            _parse_endpoint_uri(root, allowlist_root=True) for root in allowlist
        )
        if not any(_matches(candidate, root) for root in roots):
            msg = (
                f"{direction} URI is not allowed by this endpoint deployment: "
                f"{candidate.safe_display}"
            )
            raise ValueError(msg)
        return candidate


def _normalize_http_root(uri: str) -> str:
    """Return the canonical root HTTP(S) URL used by endpoint control traffic."""
    parsed = _parse_endpoint_uri(uri, allowlist_root=True)
    if parsed.scheme not in _HTTP_SCHEMES:
        msg = "endpoint_url must use http:// or https://"
        raise ValueError(msg)
    return parsed.transport_target


def _parse_endpoint_uri(uri: str, *, allowlist_root: bool) -> _EndpointUri:
    """Parse one policy root or caller-supplied endpoint-data URI."""
    if not uri:
        msg = "URI must be a nonempty string"
        raise ValueError(msg)
    if uri[:1].isspace():
        msg = "URI cannot contain leading whitespace"
        raise ValueError(msg)
    if _has_control(uri) or _BAD_PERCENT_ESCAPE.search(uri):
        msg = "URI contains invalid characters"
        raise ValueError(msg)
    has_query = "?" in uri
    has_fragment = "#" in uri
    try:
        parts = urlsplit(uri)
        port = parts.port
    except ValueError as exc:
        msg = "URI authority is malformed"
        raise ValueError(msg) from exc
    scheme = parts.scheme.lower()
    if scheme not in _SUPPORTED_SCHEMES:
        msg = "URI scheme is unsupported"
        raise ValueError(msg)
    if parts.username is not None or parts.password is not None:
        msg = "URI user information is forbidden"
        raise ValueError(msg)
    if parts.netloc.rsplit("@", 1)[-1].endswith(":"):
        msg = "URI authority contains an empty port"
        raise ValueError(msg)
    if has_fragment:
        msg = "URI fragments are forbidden"
        raise ValueError(msg)
    if has_query and not parts.query:
        msg = "URI query delimiter cannot be empty"
        raise ValueError(msg)
    if _has_control(unquote(parts.path, errors="replace")) or _has_control(
        unquote(parts.query, errors="replace")
    ):
        msg = "URI contains encoded control characters"
        raise ValueError(msg)
    raw_authority = parts.netloc.rsplit("@", 1)[-1]
    if raw_authority.startswith("["):
        try:
            ipaddress.IPv6Address(parts.hostname or "")
        except ValueError as exc:
            msg = "URI bracketed authority must be an IPv6 literal"
            raise ValueError(msg) from exc
    host = _normalize_host(parts.hostname)
    if scheme == "s3":
        return _parse_s3(parts, host, port, has_query)
    return _parse_http(parts, scheme, host, port, allowlist_root, has_query)


def _parse_s3(
    parts: SplitResult,
    host: str,
    port: int | None,
    has_query: bool,
) -> _EndpointUri:
    """Parse and canonicalize an S3 URI without transport ambiguity."""
    if port is not None or has_query:
        msg = "S3 roots cannot contain ports or queries"
        raise ValueError(msg)
    if not host or "*" in host or not _is_s3_bucket(host):
        msg = "S3 URI requires an exact bucket"
        raise ValueError(msg)
    segments = _s3_segments(parts.path)
    suffix = "/".join(quote(segment, safe="-._~") for segment in segments)
    target = f"s3://{host}" + (f"/{suffix}" if suffix else "")
    return _EndpointUri("s3", host, segments, target, _bounded_display(target))


def _parse_http(
    parts: SplitResult,
    scheme: str,
    host: str,
    port: int | None,
    allowlist_root: bool,
    has_query: bool,
) -> _EndpointUri:
    """Parse an exact HTTP origin while preserving capability paths."""
    if not host or "*" in host:
        msg = "HTTP URI requires an exact host"
        raise ValueError(msg)
    if any(char.isspace() for char in f"{parts.path}{parts.query}"):
        msg = "HTTP URI contains raw whitespace"
        raise ValueError(msg)
    normalized_port = None if port == _DEFAULT_PORTS[scheme] else port
    authority = _format_authority(host, normalized_port)
    if allowlist_root:
        if parts.path not in {"", "/"} or has_query:
            msg = "HTTP allowlist entries must be origins"
            raise ValueError(msg)
        target = f"{scheme}://{authority}"
        return _EndpointUri(scheme, authority, (), target, target)
    if not parts.path:
        msg = "HTTP capability URI requires an explicit path"
        raise ValueError(msg)
    target = f"{scheme}://{authority}{parts.path}"
    if has_query:
        target = f"{target}?{parts.query}"
    safe = _bounded_display(urlunsplit((scheme, authority, parts.path, "", "")))
    return _EndpointUri(scheme, authority, (), target, safe)


def _s3_segments(path: str) -> tuple[str, ...]:
    """Decode and validate S3 path segments exactly once."""
    raw_segments = path[1:].split("/") if path.startswith("/") else path.split("/")
    if raw_segments == [""]:
        return ()
    if raw_segments and raw_segments[-1] == "":
        raw_segments.pop()
    if any(not segment for segment in raw_segments):
        msg = "S3 URI contains an empty path segment"
        raise ValueError(msg)
    decoded: list[str] = []
    for segment in raw_segments:
        value = unquote(segment, encoding="utf-8", errors="strict")
        if (
            value in {".", ".."}
            or "/" in value
            or "\\" in value
            # s3fs reserves ``?versionId=`` inside its fs-native path. Reject
            # every decoded question mark so an authorized object name can
            # never be reinterpreted as a version selector at the sink.
            or "?" in value
            or "*" in value
            or _has_control(value)
        ):
            msg = "S3 URI contains an unsafe path segment"
            raise ValueError(msg)
        decoded.append(value)
    return tuple(decoded)


def _normalize_host(host: str | None) -> str:
    """Normalize a DNS name or literal IP for exact-origin comparisons."""
    if not host:
        return ""
    if "%" in host or any(char.isspace() for char in host):
        msg = "URI host contains invalid characters"
        raise ValueError(msg)
    value = host
    try:
        return ipaddress.ip_address(value).compressed.lower()
    except ValueError:
        try:
            normalized = (
                httpx.URL(f"http://{_format_authority(value, None)}")
                .raw_host.decode("ascii")
                .lower()
            )
        except (UnicodeError, httpx.InvalidURL) as exc:
            msg = "URI host is invalid"
            raise ValueError(msg) from exc
    if normalized.endswith("."):
        normalized = normalized[:-1]
    if not normalized or normalized.endswith("."):
        msg = "URI host is invalid"
        raise ValueError(msg)
    return normalized


def _is_s3_bucket(host: str) -> bool:
    """Return whether a normalized authority has unambiguous bucket syntax."""
    try:
        ipaddress.ip_address(host)
    except ValueError:
        return all(_S3_BUCKET_LABEL.fullmatch(label) for label in host.split("."))
    return False


def _format_authority(host: str, port: int | None) -> str:
    """Format a normalized host and optional port as a URL authority."""
    display_host = f"[{host}]" if ":" in host else host
    return display_host if port is None else f"{display_host}:{port}"


def _matches(candidate: _EndpointUri, root: _EndpointUri) -> bool:
    """Return whether a normalized candidate is within a normalized root."""
    if candidate.scheme != root.scheme or candidate.authority != root.authority:
        return False
    if candidate.scheme in _HTTP_SCHEMES:
        return True
    return candidate.segments[: len(root.segments)] == root.segments


def safe_uri_display(uri: object) -> str:
    """Remove credentials, query data, and fragments from a URI diagnostic."""
    if not isinstance(uri, str):
        return "<invalid URI>"
    raw = uri.split("#", 1)[0].split("?", 1)[0]
    if _has_control(raw):
        return "<invalid URI>"
    try:
        parts = urlsplit(raw)
        scheme = parts.scheme.lower()
        host = _normalize_host(parts.hostname)
        port = parts.port
    except (TypeError, ValueError):
        return "<invalid URI>"
    if not scheme:
        return "<invalid URI>"
    normalized_port = None if port == _DEFAULT_PORTS.get(scheme) else port
    authority = _format_authority(host, normalized_port) if host else "<invalid>"
    return _bounded_display(urlunsplit((scheme, authority, parts.path, "", "")))


def _bounded_display(value: str) -> str:
    """Bound one already-sanitized URI display for returned diagnostics."""
    if len(value) <= _MAX_SAFE_URI_DISPLAY:
        return value
    return f"{value[: _MAX_SAFE_URI_DISPLAY - 3]}..."


def _has_control(value: str) -> bool:
    """Return whether a URI component contains a control character."""
    return any(unicodedata.category(char) in {"Cc", "Cf"} for char in value)

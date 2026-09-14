"""Deployment-owned URI policy for tool-endpoint data transport."""

from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass
from urllib.parse import SplitResult, quote, unquote, urlsplit, urlunsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator

_SUPPORTED_SCHEMES = frozenset({"s3", "http", "https"})
_HTTP_SCHEMES = frozenset({"http", "https"})
_DEFAULT_PORTS = {"http": 80, "https": 443}
_BAD_PERCENT_ESCAPE = re.compile(r"%(?![0-9A-Fa-f]{2})")


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

    model_config = ConfigDict(extra="forbid", frozen=True)

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
            msg = f"{direction} URI is invalid: {_safe_uri_display(uri)}"
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
    if _has_control(uri) or _BAD_PERCENT_ESCAPE.search(uri):
        msg = "URI contains invalid characters"
        raise ValueError(msg)
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
    if parts.fragment:
        msg = "URI fragments are forbidden"
        raise ValueError(msg)
    host = _normalize_host(parts.hostname)
    if scheme == "s3":
        return _parse_s3(parts, host, port)
    return _parse_http(parts, scheme, host, port, allowlist_root)


def _parse_s3(parts: SplitResult, host: str, port: int | None) -> _EndpointUri:
    """Parse and canonicalize an S3 URI without transport ambiguity."""
    if port is not None or parts.query:
        msg = "S3 roots cannot contain ports or queries"
        raise ValueError(msg)
    if not host or "*" in host:
        msg = "S3 URI requires an exact bucket"
        raise ValueError(msg)
    segments = _s3_segments(parts.path)
    suffix = "/".join(quote(segment, safe="-._~") for segment in segments)
    target = f"s3://{host}" + (f"/{suffix}" if suffix else "")
    return _EndpointUri("s3", host, segments, target, target)


def _parse_http(
    parts: SplitResult,
    scheme: str,
    host: str,
    port: int | None,
    allowlist_root: bool,
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
        if parts.path not in {"", "/"} or parts.query:
            msg = "HTTP allowlist entries must be origins"
            raise ValueError(msg)
        target = f"{scheme}://{authority}"
        return _EndpointUri(scheme, authority, (), target, target)
    target = urlunsplit(
        (
            scheme,
            authority,
            parts.path or "/",
            parts.query,
            "",
        )
    )
    safe = urlunsplit((scheme, authority, parts.path or "/", "", ""))
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
    value = host[:-1] if host.endswith(".") else host
    try:
        return ipaddress.ip_address(value).compressed.lower()
    except ValueError:
        try:
            return value.encode("idna").decode("ascii").lower()
        except UnicodeError as exc:
            msg = "URI host is invalid"
            raise ValueError(msg) from exc


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


def _safe_uri_display(uri: object) -> str:
    """Remove credentials, query data, and fragments from a URI diagnostic."""
    if not isinstance(uri, str):
        return "<invalid URI>"
    raw = uri.split("#", 1)[0].split("?", 1)[0]
    try:
        parts = urlsplit(raw)
        scheme = parts.scheme.lower()
        host = _normalize_host(parts.hostname)
        port = parts.port
    except (TypeError, ValueError):
        return "<invalid URI>"
    if not scheme:
        return "<invalid URI>"
    authority = _format_authority(host, port) if host else "<invalid>"
    return urlunsplit((scheme, authority, parts.path, "", ""))


def _has_control(value: str) -> bool:
    """Return whether a URI component contains a control character."""
    return any(ord(char) < 32 or ord(char) == 127 for char in value)

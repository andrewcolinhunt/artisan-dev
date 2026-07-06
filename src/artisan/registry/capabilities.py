"""Capabilities payload — the server's self-description for agents.

Co-located with its shape (the ``ProvenanceEdges`` precedent) so the MCP
``artisan_capabilities`` tool and any future CLI ``capabilities`` command
share one contract. The reader composes the artisan version and the
one-time ``DiscoveryReport`` with the server-supplied bits (version, delta
root, read-only flag); it holds no MCP dependency.
"""

from __future__ import annotations

from pydantic import BaseModel

from artisan._version import __version__ as _artisan_version
from artisan.registry.models import DiscoveryReport


class CapabilitiesPayload(BaseModel):
    """What an agent can do against this server right now.

    Attributes:
        artisan_version: Installed artisan package version.
        server_version: Version of the surface serving this payload (the
            MCP server package version).
        read_only: True when write tools are unregistered (the v1 default).
        delta_root: Configured Delta root, or None when unset (store-reading
            tools will then return the ``delta_root_unset`` envelope).
        discovery: Outcome of the one startup ``discover()`` — imported
            modules, failed imports, name collisions, and the op count.
    """

    artisan_version: str
    server_version: str
    read_only: bool
    delta_root: str | None
    discovery: DiscoveryReport


def capabilities(
    *,
    server_version: str,
    discovery: DiscoveryReport,
    delta_root: str | None,
    read_only: bool,
) -> CapabilitiesPayload:
    """Assemble the capabilities payload.

    Args:
        server_version: Version of the serving surface.
        discovery: The startup discovery report.
        delta_root: Configured Delta root, or None.
        read_only: Whether write tools are unregistered.

    Returns:
        The composed ``CapabilitiesPayload``.
    """
    return CapabilitiesPayload(
        artisan_version=_artisan_version,
        server_version=server_version,
        read_only=read_only,
        delta_root=delta_root,
        discovery=discovery,
    )

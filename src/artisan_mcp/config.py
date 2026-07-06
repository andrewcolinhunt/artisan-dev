"""Server configuration for the Artisan MCP server.

Env names are pinned per field with ``validation_alias`` — an ``env_prefix``
would derive ``ARTISAN_WRITE_ENABLED`` and break the intended
``ARTISAN_WRITE`` contract. The ``artisan-mcp`` CLI sets these same env vars
from its flags, so the CLI and a client-launched server share one resolution
path.
"""

from __future__ import annotations

from typing import Annotated

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode


class ArtisanMCPConfig(BaseSettings):
    """Server configuration; env names pinned explicitly per field.

    Attributes:
        write_enabled: When True, write tools register (Phase 2). Read-only
            otherwise. From ``ARTISAN_WRITE``.
        delta_root: Delta Lake root the store-reading tools resolve. From
            ``ARTISAN_DELTA_ROOT``. Unset yields the ``delta_root_unset``
            envelope at the first store-reading tool call.
        load_modules: Extra dotted op modules to import at discovery, on top
            of the curator builtins. From the comma-separated
            ``ARTISAN_LOAD_MODULES``.
        log_level: Root log level for the server. From
            ``ARTISAN_MCP_LOG_LEVEL``.
    """

    write_enabled: bool = Field(False, validation_alias="ARTISAN_WRITE")
    delta_root: str | None = Field(None, validation_alias="ARTISAN_DELTA_ROOT")
    load_modules: Annotated[list[str], NoDecode] = Field(
        [], validation_alias="ARTISAN_LOAD_MODULES"
    )
    log_level: str = Field("INFO", validation_alias="ARTISAN_MCP_LOG_LEVEL")

    @field_validator("load_modules", mode="before")
    @classmethod
    def _split_modules(cls, value: object) -> object:
        """Parse the comma-separated ``ARTISAN_LOAD_MODULES`` into a list.

        ``NoDecode`` hands the raw env string to this validator instead of
        JSON-decoding it (a comma list is not JSON). Matches the comma
        convention that ``registry.discover`` already applies to the same
        variable.
        """
        if isinstance(value, str):
            return [module.strip() for module in value.split(",") if module.strip()]
        return value

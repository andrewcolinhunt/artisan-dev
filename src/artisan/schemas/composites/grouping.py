"""Fixed exact-name grouping declarations for bounded creator invocations."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class AnchoredNameGrouping(BaseModel):
    """Keep each primary name group intact and attach its optional named context.

    Names are exact, case-sensitive ``original_name`` values. Matched roles
    contain at most one occurrence for each primary name. Invocation bounds
    count primary groups, including groups with multiple primary occurrences.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["anchored_name"] = "anchored_name"
    primary_role: str = Field(min_length=1)
    matched_roles: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _validate_roles(self) -> AnchoredNameGrouping:
        roles = (self.primary_role, *self.matched_roles)
        if len(roles) != len(set(roles)) or any(not role for role in roles):
            msg = "Anchored roles must be distinct nonempty names"
            raise ValueError(msg)
        return self

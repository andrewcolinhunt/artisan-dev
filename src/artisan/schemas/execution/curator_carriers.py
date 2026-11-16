"""Complete external files staged by one successful curator execution."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, model_validator

from artisan.schemas.execution.retained_result import ExternalBinding, ExternalCarrier

CURATOR_CARRIER_INVENTORY_KEY = "artisan_curator_carrier_inventory"


class CuratorCarrierInventory(BaseModel):
    """Whole-file identities and exact artifact bindings inside the worker seal.

    Carrier paths are derived from the sealed execution shard; no worker source
    path or global managed location is authority in this inventory.
    """

    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )

    domain: Literal["artisan.curator-carriers"] = "artisan.curator-carriers"
    carriers: tuple[ExternalCarrier, ...] = ()
    external_bindings: tuple[ExternalBinding, ...] = ()

    @model_validator(mode="after")
    def _validate_inventory(self) -> CuratorCarrierInventory:
        names = [item.filename for item in self.carriers]
        artifacts = [item.artifact_id for item in self.external_bindings]
        if any(values != sorted(set(values)) for values in (names, artifacts)):
            msg = "Curator carrier inventories must be unique and canonically ordered"
            raise ValueError(msg)
        if {item.filename for item in self.external_bindings} != set(names):
            msg = "Curator external bindings must cover every carrier without orphans"
            raise ValueError(msg)
        return self

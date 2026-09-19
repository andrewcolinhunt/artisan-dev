"""JSON-only evidence required to reconstruct one concrete execution unit."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from artisan.utils.hashing import CacheInputIdentity


class ReplayModel(BaseModel):
    """Immutable, closed evidence owned by the current store format."""

    model_config = ConfigDict(frozen=True, extra="forbid")


class OperationIdentity(ReplayModel):
    """Declared operation identity and digest of its defining module file."""

    module: str
    qualname: str
    name: str
    version: str
    module_digest: str | None


class ReplayOperation(ReplayModel):
    """Concrete configuration and class behavior, independent of command logs."""

    identity: OperationIdentity
    configuration: dict[str, JsonValue]
    behavior: dict[str, JsonValue]
    artisan_version: str

    @model_validator(mode="after")
    def validate_behavior(self) -> ReplayOperation:
        """Require complete class declarations while retaining their JSON values."""
        flags = {
            "runtime_defined_inputs",
            "independent_input_streams",
            "hydrate_inputs",
            "per_artifact_dispatch",
            "execute_as_tool",
        }
        if set(self.behavior) != {"inputs", "outputs", *flags} or any(
            not isinstance(self.behavior[key], bool) for key in flags
        ):
            msg = "Incomplete replay class behavior evidence"
            raise ValueError(msg)
        for key in ("inputs", "outputs"):
            specs = self.behavior[key]
            if not isinstance(specs, dict) or any(
                not isinstance(spec, dict) for spec in specs.values()
            ):
                msg = "Invalid replay input/output declarations"
                raise ValueError(msg)
        return self


class ReplaySource(ReplayModel):
    """Original unit identity and sanitized runtime context."""

    step_number: int
    step_run_id: str | None
    pipeline_run_id: str | None = None
    runner: str
    execution_spec_id: str
    roots: dict[str, str | None]


class ReplayAssociation(ReplayModel):
    """Ordered associated artifacts for one primary/type pair."""

    primary_id: str
    artifact_type: str
    artifact_ids: list[str]


class ReplayReplacement(ReplayModel):
    """One omitted whole JSON value; hints never supply values implicitly."""

    pointer: str
    suggested_environment: str | None = None


class RemoteObservation(ReplayModel):
    """Actual endpoint identity and delivery outcome for one dispatch slot."""

    dispatch_index: int
    status: Literal["observed", "not_started", "not_applicable", "unavailable"]
    identity: OperationIdentity | None = None
    diagnostic_status: Literal["complete", "incomplete", "unavailable"] | None = None
    diagnostic_error: str | None = None


class ReplayDiagnostic(ReplayModel):
    """Selected replay conditions and evidence-delivery outcomes."""

    source_execution_run_id: str
    source_execution_spec_id: str
    source_identity: OperationIdentity
    selected_identity: OperationIdentity
    source_remote_identity: list[RemoteObservation]
    allow_code_change: bool
    runner: str
    roots: dict[str, str | None]
    status: Literal["complete", "incomplete", "unavailable"] = "complete"
    errors: list[str] = Field(default_factory=list)
    reproducibility_notes: list[str] = Field(default_factory=list)


class ReplaySnapshot(ReplayModel):
    """Required replay envelope for every recorded execution outcome."""

    status: Literal["ready", "requires_operation_class", "unavailable"]
    unavailable_reason: str | None
    operation: ReplayOperation | None
    inputs: dict[str, list[CacheInputIdentity]]
    group_ids: list[str] | None
    associated: list[ReplayAssociation]
    associated_complete: bool
    source: ReplaySource | None
    required_replacements: list[ReplayReplacement]
    remote_identity: list[RemoteObservation]
    read_semantics: Literal["explicit_inputs_and_current_committed_store"] = (
        "explicit_inputs_and_current_committed_store"
    )
    diagnostic: ReplayDiagnostic | None

    @model_validator(mode="after")
    def validate_evidence(self) -> ReplaySnapshot:
        """Reject ready evidence missing the concrete reconstruction contract."""
        if self.status == "unavailable":
            if not self.unavailable_reason:
                msg = "Unavailable replay evidence requires a reason"
                raise ValueError(msg)
        elif self.operation is None or self.source is None:
            msg = "Replayable evidence requires operation and source"
            raise ValueError(msg)
        lengths = [len(entries) for entries in self.inputs.values()]
        if self.group_ids is not None and any(
            length != len(self.group_ids) for length in lengths
        ):
            msg = "Replay group IDs must match every role's occurrences"
            raise ValueError(msg)
        if (
            self.operation is not None
            and not self.operation.behavior.get("independent_input_streams")
            and len(set(lengths)) > 1
        ):
            msg = "Paired replay input roles must have equal lengths"
            raise ValueError(msg)
        owners = [(item.primary_id, item.artifact_type) for item in self.associated]
        if len(owners) != len(set(owners)):
            msg = "Duplicate replay association owner"
            raise ValueError(msg)
        if self.operation is not None and self.associated_complete:
            specs: Any = self.operation.behavior["inputs"]
            expected = {
                (entry.artifact_id, kind)
                for role, entries in self.inputs.items()
                for entry in entries
                for kind in specs.get(role, {}).get("with_associated", [])
            }
            if set(owners) != expected:
                msg = "Complete associated capture must identify every declared primary/type pair"
                raise ValueError(msg)
        slots = [item.dispatch_index for item in self.remote_identity]
        if len(slots) != len(set(slots)) or any(slot < 0 for slot in slots):
            msg = "Invalid replay remote dispatch ownership"
            raise ValueError(msg)
        pointers = [item.pointer for item in self.required_replacements]
        if len(pointers) != len(set(pointers)) or any(
            not pointer.startswith("/") for pointer in pointers
        ):
            msg = "Invalid replay replacement ownership"
            raise ValueError(msg)
        if self.operation is not None:
            for pointer in pointers:
                self._validate_replacement(pointer)
        for role, entries in self.inputs.items():
            for position, entry in enumerate(entries):
                if entry.role != role or entry.position != position:
                    msg = "Replay occurrence role/position mismatch"
                    raise ValueError(msg)
                expected_group = (
                    self.group_ids[position] if self.group_ids is not None else None
                )
                if entry.group_id != expected_group:
                    msg = "Replay occurrence group mismatch"
                    raise ValueError(msg)
        return self

    def _validate_replacement(self, pointer: str) -> None:
        """Require each replacement to own an existing redacted whole value."""
        assert self.operation is not None
        value: Any = self.operation.configuration
        try:
            for escaped in pointer.split("/")[1:]:
                if "~" in escaped.replace("~0", "").replace("~1", ""):
                    raise ValueError
                key = escaped.replace("~1", "/").replace("~0", "~")
                if isinstance(value, list):
                    if not key.isdecimal():
                        raise ValueError
                    value = value[int(key)]
                else:
                    value = value[key]
            if value is not None:
                raise ValueError
        except (KeyError, IndexError, TypeError, ValueError):
            msg = "Replacement pointer does not identify a redacted configuration value"
            raise ValueError(msg) from None

    @classmethod
    def unavailable(cls, reason: str) -> ReplaySnapshot:
        """Declare an outcome with no trustworthy reconstruction evidence."""
        return cls(
            status="unavailable",
            unavailable_reason=reason,
            operation=None,
            inputs={},
            group_ids=None,
            associated=[],
            associated_complete=False,
            source=None,
            required_replacements=[],
            remote_identity=[],
            diagnostic=None,
        )

"""Input containers for operation lifecycle phases.

Structured containers for creator lifecycle phases and curator execution.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, cast

import polars as pl

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.provenance.occurrence import InputReference


@dataclass(frozen=True)
class FrozenInputContext:
    """Ordered invocation inputs and explicitly captured dependency membership.

    Association keys are ``(role, index, associated_type)`` and reference keys
    are ``(role, index)``. Missing keys mean uncaptured context, not an empty
    result. Artifacts are finalized; their local materialization locators may
    still be updated by the owning session.
    """

    invocation_id: str
    artifacts: Mapping[str, Sequence[Artifact]]
    associations: Mapping[tuple[str, int, str], Sequence[Artifact]] = field(
        default_factory=dict
    )
    references: Mapping[tuple[str, int], Mapping[str, Artifact]] = field(
        default_factory=dict
    )

    def __post_init__(self) -> None:
        if (
            not isinstance(cast(object, self.invocation_id), str)
            or not self.invocation_id
        ):
            msg = "Frozen inputs require an invocation identity"
            raise ValueError(msg)
        artifacts = {role: tuple(values) for role, values in self.artifacts.items()}
        for role, values in artifacts.items():
            if not isinstance(cast(object, role), str) or not role:
                msg = "Input roles must be nonempty strings"
                raise ValueError(msg)
            for artifact in values:
                self._validate_artifact(artifact)
        object.__setattr__(self, "artifacts", MappingProxyType(artifacts))
        associations = {}
        for (role, index, artifact_type), associated in self.associations.items():
            self.ref(role, index=index)
            if not artifact_type:
                msg = "Associated artifact type must be nonempty"
                raise ValueError(msg)
            members = tuple(associated)
            for artifact in members:
                self._validate_artifact(artifact)
                if artifact.artifact_type != artifact_type:
                    msg = "Captured association has an unexpected artifact type"
                    raise ValueError(msg)
            associations[(role, index, artifact_type)] = members
        object.__setattr__(self, "associations", MappingProxyType(associations))
        references = {}
        for (role, index), referents in self.references.items():
            config = self.artifact(self.ref(role, index=index))
            if not isinstance(config, ExecutionConfigArtifact):
                msg = "Reference capture requires a configuration input"
                raise ValueError(msg)
            captured = dict(referents)
            if set(captured) != set(config.get_artifact_references()):
                msg = "Captured references must exactly match configuration references"
                raise ValueError(msg)
            for artifact_id, artifact in captured.items():
                self._validate_artifact(artifact)
                if artifact.artifact_id != artifact_id:
                    msg = "Captured reference has an unexpected artifact identity"
                    raise ValueError(msg)
            references[(role, index)] = MappingProxyType(captured)
        object.__setattr__(self, "references", MappingProxyType(references))

    @staticmethod
    def _validate_artifact(artifact: Artifact) -> str:
        if artifact.artifact_id is None:
            msg = "Captured artifacts must be finalized"
            raise ValueError(msg)
        artifact.finalize()
        return artifact.artifact_id

    def ref(self, role: str, *, index: int) -> InputReference:
        """Bind one input position to this invocation and its expected identity."""
        values = self.artifacts.get(role, ())
        if type(index) is not int or not 0 <= index < len(values):
            msg = f"Input position does not exist: {role!r}[{index}]"
            raise ValueError(msg)
        artifact = values[index]
        artifact_id = self._validate_artifact(artifact)
        return InputReference(
            invocation_id=self.invocation_id,
            role=role,
            index=index,
            artifact_id=artifact_id,
        )

    def reference_for(self, artifact: Artifact | InputReference) -> InputReference:
        """Resolve an exact reference or a uniquely occurring input artifact."""
        if isinstance(artifact, InputReference):
            expected = self.ref(artifact.role, index=artifact.index)
            if artifact != expected:
                msg = "Input reference owner or expected identity does not match"
                raise ValueError(msg)
            return expected
        self._validate_artifact(artifact)
        matches = [
            self.ref(role, index=index)
            for role, values in self.artifacts.items()
            for index, value in enumerate(values)
            if value.artifact_id == artifact.artifact_id
        ]
        if len(matches) != 1:
            msg = "Input artifact is missing or ambiguous; use inputs.ref(role, index=...)"
            raise ValueError(msg)
        return matches[0]

    def artifact(self, reference: InputReference) -> Artifact:
        """Return the artifact at a validated exact input position."""
        selected = self.reference_for(reference)
        return self.artifacts[selected.role][selected.index]

    def associated_artifacts(
        self, primary: Artifact | InputReference, artifact_type: str
    ) -> tuple[Artifact, ...]:
        """Return explicitly captured membership, including captured-empty."""
        reference = self.reference_for(primary)
        key = (reference.role, reference.index, artifact_type)
        if key not in self.associations:
            msg = "Associated type was not captured for this input occurrence"
            raise ValueError(msg)
        for artifact in self.associations[key]:
            self._validate_artifact(artifact)
        return tuple(self.associations[key])

    def referenced_artifacts(
        self, config: Artifact | InputReference
    ) -> Mapping[str, Artifact]:
        """Return a declared configuration's captured direct dependencies."""
        reference = self.reference_for(config)
        key = (reference.role, reference.index)
        if key not in self.references:
            msg = "Config is not a declared input with resolved references"
            raise ValueError(msg)
        for artifact in self.references[key].values():
            self._validate_artifact(artifact)
        return self.references[key]


def _immutable_associations(
    associations: Mapping[tuple[str, str], Sequence[Artifact]],
) -> Mapping[tuple[str, str], tuple[Artifact, ...]]:
    """Copy captured membership so lifecycle phases cannot change each other's inputs."""
    return MappingProxyType(
        {key: tuple(values) for key, values in associations.items()}
    )


class _InputArtifactsMixin:
    """Shared methods for containers that hold input_artifacts and _associated."""

    input_artifacts: dict[str, list[Artifact]]
    _associated: Mapping[tuple[str, str], Sequence[Artifact]]
    _references: dict[str, dict[str, Artifact]]
    _context: FrozenInputContext | None

    def __post_init__(self) -> None:
        self._associated = _immutable_associations(self._associated)
        if self._context is not None:
            captured = {
                role: list(values) for role, values in self._context.artifacts.items()
            }
            if self.input_artifacts and self.input_artifacts != captured:
                msg = "Input artifacts do not match the captured invocation context"
                raise ValueError(msg)
            self.input_artifacts = captured

    def ref(self, role: str, *, index: int) -> InputReference:
        """Return an exact lineage reference owned by the current invocation."""
        if self._context is None:
            msg = "Exact input references require an invocation context"
            raise ValueError(msg)
        return self._context.ref(role, index=index)

    def referenced_artifacts(
        self, config: ExecutionConfigArtifact | InputReference
    ) -> Mapping[str, Artifact]:
        """Return the prepared direct dependencies of one declared config.

        Args:
            config: Declared config input whose spec enables reference resolution.

        Returns:
            Read-only artifact-ID mapping. No additional store lookup occurs.

        Raises:
            ValueError: If the config is not a resolved, finalized input.
        """
        if self._context is not None:
            return self._context.referenced_artifacts(config)
        if isinstance(config, InputReference):
            msg = "Exact input references require an invocation context"
            raise ValueError(msg)
        if config.artifact_id is None or config.artifact_id not in self._references:
            msg = "Config is not a declared input with resolved references"
            raise ValueError(msg)
        return MappingProxyType(self._references[config.artifact_id])

    def associated_artifacts(
        self, artifact: Artifact | InputReference, assoc_type: str
    ) -> list[Artifact]:
        """Get associated artifacts for a primary artifact.

        Args:
            artifact: The primary artifact to find associations for.
            assoc_type: The associated artifact type string
                (e.g. "structure_annotation").

        Returns:
            List of associated artifacts, or empty list if none.
        """
        if self._context is not None:
            return list(self._context.associated_artifacts(artifact, assoc_type))
        if isinstance(artifact, InputReference):
            msg = "Exact input references require an invocation context"
            raise ValueError(msg)
        if artifact.artifact_id is None:
            return []
        return list(self._associated.get((artifact.artifact_id, assoc_type), []))

    def grouped(self) -> Iterator[dict[str, Artifact]]:
        """Yield per-index snapshots across positionally-aligned input roles.

        For multi-input operations with group_by set, all input lists are
        positionally aligned after framework pairing. This method yields
        one dict per index: {"data": artifact_i, "config": artifact_j}.

        A single role yields one-key dictionaries. All role lists must have
        equal lengths; independent streams of unequal lengths cannot be grouped.
        """
        roles = list(self.input_artifacts.keys())
        if not roles:
            return
        lists = [self.input_artifacts[r] for r in roles]
        for items in zip(*lists, strict=True):
            yield dict(zip(roles, items, strict=True))


@dataclass
class PreprocessInput(_InputArtifactsMixin):
    """Input container for preprocess phase.

    Attributes:
        preprocess_dir: Directory for writing generated files, or None for a
            fileless direct call. Require a directory before writing files.
        input_artifacts: Artifacts keyed by role. Access via artifact.materialized_path
            (for materialized inputs) or artifact.content (for non-materialized).
        metadata: Escape hatch for additional data from the engine.

    Example:
        >>> for artifact in preprocess_input.input_artifacts["data"]:
        ...     process(artifact.materialized_path)
    """

    preprocess_dir: str | None
    input_artifacts: dict[str, list[Artifact]] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    _associated: Mapping[tuple[str, str], Sequence[Artifact]] = field(
        default_factory=dict, repr=False
    )
    _references: dict[str, dict[str, Artifact]] = field(
        default_factory=dict, repr=False
    )
    _context: FrozenInputContext | None = field(default=None, repr=False)


@dataclass(frozen=True)
class CuratorInput(Mapping[str, pl.DataFrame]):
    """Role DataFrames and frozen associated inputs delivered to a curator."""

    input_frames: Mapping[str, pl.DataFrame]
    associations: Mapping[tuple[str, str], Sequence[Artifact]] = field(
        default_factory=dict, repr=False
    )

    def __post_init__(self) -> None:
        """Copy membership so callers cannot mutate the delivered snapshot."""
        object.__setattr__(
            self, "input_frames", MappingProxyType(dict(self.input_frames))
        )
        object.__setattr__(
            self,
            "associations",
            _immutable_associations(self.associations),
        )

    def __getitem__(self, role: str) -> pl.DataFrame:
        return self.input_frames[role]

    def __iter__(self) -> Iterator[str]:
        return iter(self.input_frames)

    def __len__(self) -> int:
        return len(self.input_frames)

    def associated_artifacts(
        self, artifact: Artifact | str, assoc_type: str
    ) -> list[Artifact]:
        """Return captured membership for a declared primary/type pair.

        Args:
            artifact: A primary input artifact or its exact artifact ID.
            assoc_type: Associated artifact type declared by the input spec.

        Returns:
            A copy of the captured member list, including an empty list when
            the declared pair had no associated artifacts at capture time.

        Raises:
            ValueError: If this primary/type pair was not captured.
        """
        artifact_id = artifact if isinstance(artifact, str) else artifact.artifact_id
        if artifact_id is None or (artifact_id, assoc_type) not in self.associations:
            msg = "Associated type was not captured for this curator input"
            raise ValueError(msg)
        return list(self.associations[(artifact_id, assoc_type)])


@dataclass(frozen=True)
class ExecuteInput:
    """Input container for execute phase.

    Contains prepared inputs and optional directories for file outputs.
    Fileless direct calls leave all directory and log paths unset.

    Attributes:
        execute_dir: Directory for writing output files, or None for a fileless
            direct call. Files are made available to postprocess.
        inputs: Prepared inputs from preprocess.
            Keys may differ from original roles after transformation.
        log_path: Optional path for automatic capture of external tool output.
            Unset for a fileless direct call. The
            filename ``tool_output.log`` is reserved by the framework
            — do not write a file by that name to ``<sandbox_root>``.
        metadata: Escape hatch for additional data from the engine.
        files_dir: Optional local directory for file-backed outputs. Write complete
            files here before drafting file references. Pipeline execution moves
            accepted files into managed storage and updates their locators.
            Direct calls with ``work_dir`` leave files beneath the caller's path
            until the caller removes them. Unset for fileless direct calls.
    """

    execute_dir: str | None
    inputs: dict[str, Any] = field(default_factory=dict)
    log_path: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    files_dir: str | None = None


@dataclass
class PostprocessInput(_InputArtifactsMixin):
    """Input container for postprocess phase.

    The postprocess method creates draft Artifacts from execute outputs
    and returns them in an ArtifactResult.

    Attributes:
        step_number: Current pipeline step number, or None for a standalone call.
        postprocess_dir: Directory for postprocess files, or None for a fileless
            direct call. Most postprocessing can use in-memory results.
        file_outputs: Files collected from execute directories. Empty for
            fileless direct calls. Read their content to create artifact drafts.
        memory_outputs: Whatever execute returned (Any type).
            Could be a subprocess result, dict, library object, None, etc.
        input_artifacts: Full input context with metadata.
            Artifacts have materialized_path set when materialization was requested.
            Use for output naming and explicit parent declarations.
        metadata: Escape hatch for additional data from the engine.
        files_dir: Optional local directory for file-backed outputs. Copy selected
            complete outputs here before drafting references; use distinct paths
            for files with the same basename. Pipeline execution relocates accepted
            files and updates their locators. Direct-call files remain beneath
            the caller's ``work_dir`` until caller cleanup. Unset for fileless calls.
    """

    step_number: int | None
    postprocess_dir: str | None
    file_outputs: list[str] = field(default_factory=list)
    memory_outputs: Any = None
    input_artifacts: dict[str, list[Artifact]] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    _associated: Mapping[tuple[str, str], Sequence[Artifact]] = field(
        default_factory=dict, repr=False
    )
    _references: dict[str, dict[str, Artifact]] = field(
        default_factory=dict, repr=False
    )
    files_dir: str | None = None
    _context: FrozenInputContext | None = field(default=None, repr=False)

"""Input containers for operation lifecycle phases.

Structured containers for creator lifecycle phases and curator execution.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

import polars as pl

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact


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

    def __post_init__(self) -> None:
        self._associated = _immutable_associations(self._associated)

    def referenced_artifacts(
        self, config: ExecutionConfigArtifact
    ) -> Mapping[str, Artifact]:
        """Return the prepared direct dependencies of one declared config.

        Args:
            config: Declared config input whose spec enables reference resolution.

        Returns:
            Read-only artifact-ID mapping. No additional store lookup occurs.

        Raises:
            ValueError: If the config is not a resolved, finalized input.
        """
        if config.artifact_id is None or config.artifact_id not in self._references:
            msg = "Config is not a declared input with resolved references"
            raise ValueError(msg)
        return MappingProxyType(self._references[config.artifact_id])

    def associated_artifacts(
        self, artifact: Artifact, assoc_type: str
    ) -> list[Artifact]:
        """Get associated artifacts for a primary artifact.

        Args:
            artifact: The primary artifact to find associations for.
            assoc_type: The associated artifact type string
                (e.g. "structure_annotation").

        Returns:
            List of associated artifacts, or empty list if none.
        """
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
        preprocess_dir: Directory for writing generated files (configs, conversions).
        input_artifacts: Artifacts keyed by role. Access via artifact.materialized_path
            (for materialized inputs) or artifact.content (for non-materialized).
        metadata: Escape hatch for additional data from the engine.

    Example:
        >>> for artifact in preprocess_input.input_artifacts["data"]:
        ...     process(artifact.materialized_path)
    """

    preprocess_dir: str
    input_artifacts: dict[str, list[Artifact]] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    _associated: Mapping[tuple[str, str], Sequence[Artifact]] = field(
        default_factory=dict, repr=False
    )
    _references: dict[str, dict[str, Artifact]] = field(
        default_factory=dict, repr=False
    )


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

    Contains the prepared inputs from preprocess and the directory
    where output files should be written.

    Attributes:
        execute_dir: Directory for writing output files.
            Files are made available to postprocess, which returns artifact drafts.
        inputs: Prepared inputs from preprocess.
            Keys may differ from original roles after transformation.
        log_path: Path where external tool output should be written.
            Provided by the framework for automatic capture. The
            filename ``tool_output.log`` is reserved by the framework
            — do not write a file by that name to ``<sandbox_root>``.
        metadata: Escape hatch for additional data from the engine.
        files_dir: Local sandbox directory for external file output.
            Always a local filesystem path, never a cloud URI. Operations
            write here using stdlib I/O; the framework uploads the
            contents to ``files_root`` on postprocess and rewrites each
            artifact's ``external_path`` to the destination (cloud URI
            on cloud backends; sharded path under ``files_root`` on
            local). Set when the pipeline has ``files_root`` configured;
            None otherwise.
    """

    execute_dir: str
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
        step_number: Current pipeline step number.
            Required for constructing draft artifacts.
        postprocess_dir: Directory for any postprocess artifacts.
            Rarely needed; most postprocessing is in-memory.
        file_outputs: All files in execute_dir after execute completes.
            Use these to create artifact drafts by reading content.
        memory_outputs: Whatever execute returned (Any type).
            Could be a subprocess result, dict, library object, None, etc.
        input_artifacts: Full input context with metadata.
            Artifacts have materialized_path set for reference.
            Use for output naming and explicit parent declarations.
        metadata: Escape hatch for additional data from the engine.
        files_dir: Local staging directory for this lifecycle unit's managed
            external files. Set when ``files_root`` is configured; None
            otherwise. Copy selected complete outputs here before drafting
            file references to their staged paths. Use distinct subdirectories
            for outputs with the same basename. The framework relocates
            referenced files and updates their locators before successful
            sandbox cleanup. This path is temporary: do not retain it as a
            permanent locator or write to it after postprocess returns.
    """

    step_number: int
    postprocess_dir: str
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

"""Input containers for operation lifecycle phases.

Provides ``PreprocessInput``, ``ExecuteInput``, and
``PostprocessInput`` -- structured containers passed to each phase
of the preprocess/execute/postprocess lifecycle.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

from artisan.schemas.artifact.base import Artifact


class _InputArtifactsMixin:
    """Shared methods for containers that hold input_artifacts and _associated."""

    input_artifacts: dict[str, list[Artifact]]
    _associated: dict[tuple[str, str], list[Artifact]]

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
        return self._associated.get((artifact.artifact_id, assoc_type), [])

    def grouped(self) -> Iterator[dict[str, Artifact]]:
        """Yield per-index snapshots across positionally-aligned input roles.

        For multi-input operations with group_by set, all input lists are
        positionally aligned after framework pairing. This method yields
        one dict per index: {"data": artifact_i, "config": artifact_j}.

        Single-input operations should not use this method.
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
    _associated: dict[tuple[str, str], list[Artifact]] = field(
        default_factory=dict, repr=False
    )


@dataclass(frozen=True)
class ExecuteInput:
    """Input container for execute phase.

    Contains the prepared inputs from preprocess and the directory
    where output files should be written.

    Attributes:
        execute_dir: Directory for writing output files.
            All files written here will be captured as artifacts.
        inputs: Prepared inputs from preprocess.
            Keys may differ from original roles after transformation.
        log_path: Path where external tool output should be written.
            Provided by the framework for automatic capture.
        metadata: Escape hatch for additional data from the engine.
        files_dir: Directory within files_root for external file output.
            Set when the pipeline has files_root configured. Operations
            that produce Artisan-managed external files write here instead
            of execute_dir. None when files_root is not configured.
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
            Use for output naming and lineage inference.
        metadata: Escape hatch for additional data from the engine.
    """

    step_number: int
    postprocess_dir: str
    file_outputs: list[str] = field(default_factory=list)
    memory_outputs: Any = None
    input_artifacts: dict[str, list[Artifact]] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    _associated: dict[tuple[str, str], list[Artifact]] = field(
        default_factory=dict, repr=False
    )

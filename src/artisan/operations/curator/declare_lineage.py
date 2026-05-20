"""Curator operation that declares directed lineage edges between two streams.

DeclareLineage emits parent → child provenance edges without producing
new artifacts. Pairings are computed per a chosen strategy (ZIP, NAME,
CROSS_PRODUCT), the children stream is passed through unchanged, and
candidate edges already present in the table are deduped against the
full ``(source_id, target_id, source_role, target_role, group_id)``
tuple — so re-running with the same inputs is a no-op.
"""

from __future__ import annotations

from enum import StrEnum, auto
from typing import TYPE_CHECKING, ClassVar

import polars as pl
from pydantic import BaseModel

from artisan.execution.inputs.grouping import (
    _match_by_name,
    _match_cross_product,
    _match_zip,
)
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.provenance import ArtifactProvenanceEdge
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.execution.curator_result import PassthroughResult
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

if TYPE_CHECKING:
    from artisan.storage.core.artifact_store import ArtifactStore


# Sentinel run_id stamped onto every edge before the executor injects
# the real execution_run_id from the ExecutionContext (see
# ``_handle_passthrough_result`` in execution/executors/curator.py).
# The value is never persisted; it only satisfies
# ``ArtifactProvenanceEdge``'s 32-char constraint at construction time.
_SENTINEL_RUN_ID = "0" * 32

# v1: edges always use these fixed role names on the artifact_edges row.
_SOURCE_ROLE = "parents"
_TARGET_ROLE = "children"


class DeclareLineage(OperationDefinition):
    """Declare directed lineage edges between two finalized artifact streams.

    Takes two input roles (``parents`` and ``children``) and a pairing
    strategy, computes ``(parent_id, child_id)`` pairs, and emits one
    provenance edge per pair. Both sides are already finalized; the
    children flow through unchanged (passthrough semantics) and the
    op's only effect is new rows in ``provenance/artifact_edges``.

    Pairings via ``Params.pairing``:

    - ``GroupByStrategy.ZIP``: 1:1 positional; lengths must match.
    - ``GroupByStrategy.NAME``: 1:1 by ``original_name`` stem.
    - ``GroupByStrategy.CROSS_PRODUCT``: every parent x every child.

    ``GroupByStrategy.LINEAGE`` is not supported as a pairing strategy
    — DeclareLineage *creates* lineage edges, so matching against
    existing edges to declare new ones is circular.

    Idempotent: candidate edges whose full
    ``(source_id, target_id, source_role, target_role, group_id)``
    tuple already exists in the table are skipped. Re-runs against the
    same inputs produce zero new edges.

    Example:
        >>> pipeline.run(
        ...     operation=DeclareLineage,
        ...     inputs={"parents": designs, "children": anchored_msas},
        ...     params=DeclareLineage.Params(pairing=GroupByStrategy.ZIP),
        ...     name="link_anchored_msas_to_designs",
        ... )
    """

    # ---------- Metadata ----------
    name = "declare_lineage"
    description = (
        "Declare directed parent → child lineage edges between two "
        "artifact streams without producing new artifacts"
    )

    # ---------- Inputs ----------
    class InputRole(StrEnum):
        parents = auto()
        children = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.parents: InputSpec(
            artifact_type=ArtifactTypes.ANY,
            required=True,
            description="Source artifacts for the declared edges",
        ),
        InputRole.children: InputSpec(
            artifact_type=ArtifactTypes.ANY,
            required=True,
            description="Target artifacts for the declared edges (passthrough)",
        ),
    }

    # ---------- Outputs ----------
    class OutputRole(StrEnum):
        children = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.children: OutputSpec(
            artifact_type=ArtifactTypes.ANY,
            required=False,
            description="Children stream passthrough (artifact IDs unchanged)",
        ),
    }

    # ---------- Behavior ----------
    hydrate_inputs: ClassVar[bool] = False
    independent_input_streams: ClassVar[bool] = True

    # ---------- Parameters ----------
    class Params(BaseModel):
        """DeclareLineage parameters.

        Attributes:
            pairing: Strategy for matching parents to children. Required;
                no default. ``LINEAGE`` is rejected at execute time.
        """

        pairing: GroupByStrategy

    params: Params

    # ---------- Lifecycle ----------
    def execute_curator(
        self,
        inputs: dict[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> PassthroughResult:
        """Validate inputs, compute pairings, dedup, and emit edges.

        Args:
            inputs: Role names to DataFrames with an ``artifact_id``
                column. Must contain both ``parents`` and ``children``.
            step_number: This step's number (unused; kept for the
                curator-op signature).
            artifact_store: Store for type lookups and edge dedup.

        Returns:
            ``PassthroughResult`` routing ``children`` IDs unchanged, with
            new edges in ``lineage_edges`` (deduped against existing
            rows in ``provenance/artifact_edges``). ``lineage_edges``
            is ``None`` when no new edges remain after dedup.

        Raises:
            ValueError: When a required input role is missing, the
                pairing strategy is unsupported, a parent or child ID
                is absent from ``artifact_index``, or a pairing would
                emit a self-loop.
        """
        parent_ids = _extract_ids(inputs, role=self.InputRole.parents)
        child_ids = _extract_ids(inputs, role=self.InputRole.children)

        self._validate_pairing_supported()
        _validate_sources_exist(parent_ids, child_ids, artifact_store)

        pairs = self._build_pairs(parent_ids, child_ids, artifact_store)
        _validate_no_self_loops(pairs)

        new_pairs = _dedup_against_existing(
            pairs, parent_ids, child_ids, artifact_store
        )

        type_map = artifact_store.provenance.load_type_map(
            list({*parent_ids, *child_ids})
        )
        edges = [
            ArtifactProvenanceEdge(
                execution_run_id=_SENTINEL_RUN_ID,
                source_artifact_id=parent_id,
                target_artifact_id=child_id,
                source_artifact_type=type_map.get(parent_id, "UNKNOWN"),
                target_artifact_type=type_map.get(child_id, "UNKNOWN"),
                source_role=_SOURCE_ROLE,
                target_role=_TARGET_ROLE,
                group_id=None,
            )
            for parent_id, child_id in new_pairs
        ]

        return PassthroughResult(
            success=True,
            passthrough={self.OutputRole.children: child_ids},
            lineage_edges=edges if edges else None,
        )

    # ---------- Private helpers ----------
    def _validate_pairing_supported(self) -> None:
        """Reject LINEAGE-as-pairing; DeclareLineage creates lineage."""
        if self.params.pairing == GroupByStrategy.LINEAGE:
            msg = (
                "DeclareLineage does not support pairing=LINEAGE. The op's "
                "purpose is to create lineage edges; pairing against "
                "existing edges to declare new ones is circular. Use "
                "ZIP, NAME, or CROSS_PRODUCT."
            )
            raise ValueError(msg)

    def _build_pairs(
        self,
        parent_ids: list[str],
        child_ids: list[str],
        artifact_store: ArtifactStore,
    ) -> list[tuple[str, str]]:
        """Invoke the chosen pairing helper and return (parent, child) tuples."""
        matcher_inputs = {
            _SOURCE_ROLE: parent_ids,
            _TARGET_ROLE: child_ids,
        }
        match self.params.pairing:
            case GroupByStrategy.ZIP:
                matched = _match_zip(matcher_inputs)
            case GroupByStrategy.CROSS_PRODUCT:
                matched = _match_cross_product(matcher_inputs)
            case GroupByStrategy.NAME:
                matched = _match_by_name(matcher_inputs, artifact_store)
            case _:  # LINEAGE rejected by _validate_pairing_supported
                msg = f"Unreachable: unsupported pairing {self.params.pairing!r}"
                raise AssertionError(msg)
        return [(m[_SOURCE_ROLE], m[_TARGET_ROLE]) for m in matched]


# ---------------------------------------------------------------------------
# Module-level helpers (kept private; reused only by DeclareLineage itself)
# ---------------------------------------------------------------------------


def _extract_ids(inputs: dict[str, pl.DataFrame], *, role: str) -> list[str]:
    """Return the artifact_id column for the named input role."""
    if role not in inputs:
        msg = f"DeclareLineage requires '{role}' input role; got: {list(inputs)}"
        raise ValueError(msg)
    return inputs[role]["artifact_id"].to_list()


def _validate_sources_exist(
    parent_ids: list[str],
    child_ids: list[str],
    artifact_store: ArtifactStore,
) -> None:
    """Raise if any parent or child ID is missing from artifact_index."""
    all_ids = list({*parent_ids, *child_ids})
    if not all_ids:
        return
    type_map = artifact_store.provenance.load_type_map(all_ids)
    missing = sorted(aid for aid in all_ids if aid not in type_map)
    if missing:
        msg = (
            f"DeclareLineage: {len(missing)} input ID(s) not found in "
            f"artifact_index (sample: {missing[:3]}). This usually means "
            f"a typo or a reference to a different pipeline's store."
        )
        raise ValueError(msg)


def _validate_no_self_loops(pairs: list[tuple[str, str]]) -> None:
    """Raise if any (parent, child) pair would emit a self-edge."""
    loops = [(p, c) for p, c in pairs if p == c]
    if loops:
        msg = (
            f"DeclareLineage: pairing would emit {len(loops)} self-loop "
            f"edge(s) (source_artifact_id == target_artifact_id). Under the "
            f"directional matcher's depth-0 self-match rule, a self-loop "
            f"would make the candidate match itself — almost always a "
            f"pipeline-construction bug. First offender: {loops[0]!r}"
        )
        raise ValueError(msg)


def _dedup_against_existing(
    pairs: list[tuple[str, str]],
    parent_ids: list[str],
    child_ids: list[str],
    artifact_store: ArtifactStore,
) -> list[tuple[str, str]]:
    """Drop pairs whose edge tuple already exists in artifact_edges.

    Dedup key: ``(source_id, target_id, source_role, target_role,
    group_id)`` — matching the storage-level edge identity. v1 fixes
    ``source_role=parents``, ``target_role=children``, ``group_id=NULL``,
    so the filter narrows to those rows.
    """
    if not pairs:
        return []
    all_ids = pl.Series(list({*parent_ids, *child_ids}))
    step_range = artifact_store.provenance.get_step_range(all_ids)
    if step_range is None:
        # No artifacts known → no existing edges → nothing to dedup against.
        return pairs
    step_min, step_max = step_range
    existing = artifact_store.provenance.load_edges_df(
        step_min, step_max, include_roles=True
    )
    if existing.is_empty():
        return pairs
    matching = existing.filter(
        (pl.col("source_role") == _SOURCE_ROLE)
        & (pl.col("target_role") == _TARGET_ROLE)
        & pl.col("group_id").is_null()
    )
    already = {
        (row["source_artifact_id"], row["target_artifact_id"])
        for row in matching.iter_rows(named=True)
    }
    return [(p, c) for p, c in pairs if (p, c) not in already]

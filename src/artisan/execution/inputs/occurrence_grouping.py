"""Outer pairing over ordered selections without merging equal artifact content."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass

import polars as pl

from artisan.execution.inputs.grouping import validate_stem_match_uniqueness
from artisan.execution.inputs.lineage_matching import match_by_ancestry
from artisan.execution.inputs.partitioning import partition_local_inputs
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.composites.input_contracts import split_shared_inputs
from artisan.schemas.composites.plan import InvocationPolicy
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.provenance.occurrence import OccurrenceAddress
from artisan.schemas.provenance.projection import AcceptedProducer
from artisan.utils.filename import strip_extensions

type SelectionKey = AcceptedProducer | OccurrenceAddress


@dataclass(frozen=True)
class InputSelection:
    """One supplied position; an unmanaged artifact has no inferred producer."""

    artifact: Artifact
    key: SelectionKey

    @property
    def producer(self) -> AcceptedProducer | None:
        return None if isinstance(self.key, OccurrenceAddress) else self.key

    def accepted_producer(self) -> AcceptedProducer:
        """Require qualified provenance for ancestry pairing."""
        producer = self.producer
        if producer is None:
            msg = "LINEAGE grouping requires accepted producer scope for every input"
            raise ValueError(msg)
        return producer


@dataclass(frozen=True)
class LineageSnapshot:
    """Verified producer aliases and ancestry used only for outer pairing."""

    canonical: Mapping[AcceptedProducer, AcceptedProducer]
    parents: Mapping[AcceptedProducer, tuple[AcceptedProducer, ...]]
    step_numbers: Mapping[AcceptedProducer, int]


def partition_occurrence_inputs(
    inputs: Mapping[str, Sequence[InputSelection]],
    policy: InvocationPolicy,
    *,
    lineage: LineageSnapshot | None = None,
) -> Iterator[dict[str, tuple[InputSelection, ...]]]:
    """Pair once at the boundary and reuse local scientific partition semantics."""
    paired, shared = split_shared_inputs(inputs, policy.shared_input_roles)
    if shared:
        if not any(paired.values()):
            return
        policy = policy.model_copy(update={"shared_input_roles": ()})
        for partition in partition_occurrence_inputs(paired, policy, lineage=lineage):
            yield {
                role: shared[role] if role in shared else partition[role]
                for role in inputs
            }
        return
    if not inputs or not any(inputs.values()) or policy.independent_input_streams:
        yield from partition_local_inputs(inputs, policy)
        return
    if policy.group_by == GroupByStrategy.NAME:
        inputs = _by_name(inputs)
        policy = policy.model_copy(update={"group_by": GroupByStrategy.ZIP})
    elif policy.group_by == GroupByStrategy.LINEAGE:
        if lineage is None:
            msg = "LINEAGE grouping requires verified accepted occurrence ancestry"
            raise ValueError(msg)
        inputs = _by_lineage(inputs, lineage)
        policy = policy.model_copy(update={"group_by": GroupByStrategy.ZIP})
    yield from partition_local_inputs(inputs, policy)


def _by_name(
    inputs: Mapping[str, Sequence[InputSelection]],
) -> dict[str, tuple[InputSelection, ...]]:
    names = {}
    for role, selections in inputs.items():
        named = []
        for item in selections:
            name = getattr(item.artifact, "original_name", None)
            if name is not None:
                if not isinstance(name, str):
                    msg = "NAME grouping requires string original names"
                    raise ValueError(msg)
                named.append((strip_extensions(name, strip_all=True), item))
        validate_stem_match_uniqueness(role, [name for name, _ in named])
        names[role] = dict(named)
    common = sorted(set.intersection(*(set(items) for items in names.values())))
    return {
        role: tuple(items[name] for name in common) for role, items in names.items()
    }


def _by_lineage(
    inputs: Mapping[str, Sequence[InputSelection]], snapshot: LineageSnapshot
) -> dict[str, tuple[InputSelection, ...]]:
    if len(inputs) != 2:
        msg = "LINEAGE grouping requires exactly two input roles"
        raise ValueError(msg)
    if any(item.producer is None for items in inputs.values() for item in items):
        msg = "LINEAGE grouping requires accepted producer scope for every input"
        raise ValueError(msg)
    if any(not items for items in inputs.values()):
        return dict.fromkeys(inputs, ())
    roles = tuple(inputs)
    first = [snapshot.canonical[inputs[role][0].accepted_producer()] for role in roles]
    target, candidate = (
        roles
        if snapshot.step_numbers[first[0]] <= snapshot.step_numbers[first[1]]
        else tuple(reversed(roles))
    )
    targets = [snapshot.canonical[item.accepted_producer()] for item in inputs[target]]
    candidates = [
        snapshot.canonical[item.accepted_producer()] for item in inputs[candidate]
    ]
    encode = {item: item.model_dump_json() for item in snapshot.parents}
    edges = pl.DataFrame(
        [
            (encode[parent], encode[child])
            for child, parents in snapshot.parents.items()
            for parent in parents
        ],
        schema={"source_artifact_id": pl.String, "target_artifact_id": pl.String},
        orient="row",
    )
    matches = match_by_ancestry(
        {encode[item] for item in targets},
        {candidate: [encode[item] for item in candidates]},
        edges,
    )
    paired: dict[str, list[InputSelection]] = {role: [] for role in inputs}
    for selected, producer in zip(inputs[target], targets, strict=True):
        found = set(matches.get(encode[producer], {}).get(candidate, ()))
        for other, ancestor in zip(inputs[candidate], candidates, strict=True):
            if encode[ancestor] in found:
                paired[target].append(selected)
                paired[candidate].append(other)
    return {role: tuple(items) for role, items in paired.items()}

"""Occurrence-aware derivation and retention prototype using real artifacts."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from pydantic import Field, model_validator

from artisan.execution.lineage.validation import (
    validate_artifacts_match_specs,
    validate_lineage_completeness,
)
from artisan.schemas import Artifact, ArtifactResult, LineageMapping, OutputSpec

from .projection import BoundaryProjection, FrozenContext, Occurrence, Relation


class PreciseLineageMapping(LineageMapping):
    """Experimental input position added without changing production schemas."""

    source_input_index: int | None = Field(default=None, ge=0, strict=True)
    source_invocation: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def validate_position(self) -> PreciseLineageMapping:
        """Require position and owner together, only for an input parent."""
        if (self.source_input_index is None) != (self.source_invocation is None):
            msg = "Precise input lineage requires both position and invocation"
            raise ValueError(msg)
        if self.source_input_index is not None and self.source_artifact_id is None:
            msg = "Precise input lineage requires an expected input artifact ID"
            raise ValueError(msg)
        return self


@dataclass(frozen=True)
class Node:
    """One transient occurrence with its actual declared parents."""

    key: str
    artifact: Artifact
    parents: tuple[str, ...] = ()
    alias_of: str | None = None


@dataclass(frozen=True)
class RequiredReference:
    """A type policy's exact referent and expected immutable identity."""

    key: str
    artifact_id: str
    artifact_type: str


ReferencePolicy = Callable[[Node, Mapping[str, Node]], Sequence[RequiredReference]]


def occurrence_key(invocation: str, role: str, index: int) -> str:
    """Encode an unambiguous owner/role/position key without content identity."""
    if not invocation or not role or index < 0:
        msg = "Occurrence keys require an owner, role, and nonnegative position"
        raise ValueError(msg)
    return json.dumps([invocation, role, index], separators=(",", ":"))


class OccurrenceGraph:
    """Construct a validated transient DAG, then retain only its boundary."""

    def __init__(
        self, reference_policies: Mapping[str, ReferencePolicy | None]
    ) -> None:
        self.nodes: dict[str, Node] = {}
        self.boundary_inputs: dict[str, tuple[str, ...]] = {}
        self.reference_policies = dict(reference_policies)
        self._invocations: set[str] = set()

    def add_input(self, role: str, artifact: Artifact) -> str:
        """Admit one finalized input occurrence in caller order."""
        values = self.boundary_inputs.get(role, ())
        key = occurrence_key("boundary", role, len(values))
        self._insert(Node(key, artifact))
        self.boundary_inputs[role] = (*values, key)
        return key

    def add_alias(self, invocation: str, role: str, source: str) -> str:
        """Preserve an explicit identity-routing occurrence and its original origin."""
        source_node = self.nodes[source]
        key = occurrence_key(invocation, role, 0)
        self._insert(Node(key, source_node.artifact, (source,), alias_of=source))
        return key

    def add_context_input(self, key: str, artifact: Artifact) -> str:
        """Admit captured external content without adding an operation input role."""
        self._insert(Node(key, artifact))
        return key

    def record_result(
        self,
        invocation: str,
        inputs: Mapping[str, Sequence[str]],
        result: ArtifactResult,
        output_specs: dict[str, OutputSpec],
    ) -> dict[str, tuple[str, ...]]:
        """Finalize and record an ordinary operation's explicit result lineage."""
        if invocation in self._invocations:
            msg = "Invocation identities must be unique"
            raise ValueError(msg)
        if not result.success:
            msg = "Cannot record a failed operation result"
            raise ValueError(msg)
        validate_artifacts_match_specs(result.artifacts, output_specs)
        validate_lineage_completeness(result.artifacts, output_specs, result.lineage)
        if set(result.lineage) != set(result.artifacts):
            msg = "Lineage roles must exactly match artifact roles"
            raise ValueError(msg)
        outputs = {
            role: tuple(occurrence_key(invocation, role, i) for i in range(len(items)))
            for role, items in result.artifacts.items()
        }
        parents = self._resolve_lineage(
            invocation, inputs, outputs, result, output_specs
        )
        pending = {
            key: Node(key, artifact.finalize(), tuple(parents.get(key, ())))
            for role, artifacts in result.artifacts.items()
            for key, artifact in zip(outputs[role], artifacts, strict=True)
        }
        self._validate_pending(pending)
        self.nodes.update(pending)
        self._invocations.add(invocation)
        return outputs

    def _resolve_lineage(
        self,
        invocation: str,
        inputs: Mapping[str, Sequence[str]],
        outputs: Mapping[str, tuple[str, ...]],
        result: ArtifactResult,
        specs: Mapping[str, OutputSpec],
    ) -> dict[str, list[str]]:
        parents: dict[str, list[str]] = {}
        for role, mappings in result.lineage.items():
            contract = specs[role].derives_from
            if contract is None:
                msg = "Artifact-producing operations require a lineage contract"
                raise ValueError(msg)
            kind, allowed_roles = next(iter(contract.items()))
            for mapping in mappings:
                if mapping.source_role not in allowed_roles:
                    msg = "Lineage source role is outside the declared contract"
                    raise ValueError(msg)
                target = self._indexed(outputs, role, mapping.draft_index)
                source = self._resolve_parent(
                    invocation, inputs, outputs, mapping, kind
                )
                values = parents.setdefault(target, [])
                if source in values:
                    msg = "Duplicate exact parent occurrence"
                    raise ValueError(msg)
                values.append(source)
        return parents

    def _resolve_parent(
        self,
        invocation: str,
        inputs: Mapping[str, Sequence[str]],
        outputs: Mapping[str, tuple[str, ...]],
        mapping: LineageMapping,
        kind: str,
    ) -> str:
        if kind == "outputs":
            if mapping.source_output_index is None:
                msg = "Sibling lineage requires an output index"
                raise ValueError(msg)
            return self._indexed(
                outputs, mapping.source_role, mapping.source_output_index
            )
        if mapping.source_artifact_id is None:
            msg = "Input lineage requires an expected input artifact ID"
            raise ValueError(msg)
        values = inputs.get(mapping.source_role, ())
        if (
            isinstance(mapping, PreciseLineageMapping)
            and mapping.source_input_index is not None
        ):
            if mapping.source_invocation != invocation:
                msg = "Input lineage belongs to another invocation"
                raise ValueError(msg)
            key = self._indexed(inputs, mapping.source_role, mapping.source_input_index)
            if self.nodes[key].artifact.artifact_id != mapping.source_artifact_id:
                msg = "Input lineage expected artifact identity does not match"
                raise ValueError(msg)
            return key
        matches = [
            key
            for key in values
            if self.nodes[key].artifact.artifact_id == mapping.source_artifact_id
        ]
        if len(matches) != 1:
            msg = "Input ID lineage is missing or ambiguous; use an exact occurrence"
            raise ValueError(msg)
        return matches[0]

    @staticmethod
    def _indexed(streams: Mapping[str, Sequence[str]], role: str, index: int) -> str:
        values = streams.get(role, ())
        if not 0 <= index < len(values):
            msg = "Lineage index is outside the referenced role"
            raise ValueError(msg)
        return values[index]

    def _insert(self, node: Node) -> None:
        self._validate_pending({node.key: node})
        self.nodes[node.key] = node

    def _validate_pending(self, pending: Mapping[str, Node]) -> None:
        if self.nodes.keys() & pending.keys():
            msg = "Occurrence identities must be unique"
            raise ValueError(msg)
        all_nodes = self.nodes | dict(pending)
        for node in pending.values():
            if node.artifact.artifact_id is None:
                msg = "Only finalized artifacts may enter the occurrence graph"
                raise ValueError(msg)
            node.artifact.finalize()
            if any(parent not in all_nodes for parent in node.parents):
                msg = "A parent occurrence is not admitted to this assignment"
                raise ValueError(msg)
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(key: str) -> None:
            if key in visiting:
                msg = "Derivation must be acyclic"
                raise ValueError(msg)
            if key in visited:
                return
            visiting.add(key)
            for parent in all_nodes[key].parents:
                visit(parent)
            visiting.remove(key)
            visited.add(key)

        for key in pending:
            visit(key)

    def project(
        self,
        exports: Mapping[str, Sequence[str]],
        output_specs: dict[str, OutputSpec],
        *,
        frozen_context: FrozenContext | None = None,
    ) -> BoundaryProjection:
        """Project declared parents while preserving direct associations separately."""
        context = frozen_context or FrozenContext()
        boundary = {key for values in self.boundary_inputs.values() for key in values}
        boundary |= context.occurrence_keys()
        if not boundary <= self.nodes.keys():
            msg = "Frozen context references an unadmitted occurrence"
            raise ValueError(msg)
        exported = {
            role: tuple(self._original(key) for key in keys)
            for role, keys in exports.items()
        }
        artifacts = {
            role: [self.nodes[key].artifact for key in keys]
            for role, keys in exported.items()
        }
        validate_artifacts_match_specs(artifacts, output_specs)
        retained, references = self._retain(exported, boundary)
        kept = boundary | retained
        parents = self._project_parents(retained, kept)
        public = {key for values in exported.values() for key in values}
        return BoundaryProjection(
            boundary_inputs=dict(self.boundary_inputs),
            occurrences=tuple(
                Occurrence(
                    key=key,
                    artifact_id=node.artifact.artifact_id,
                    artifact_type=node.artifact.artifact_type,
                )
                for key, node in self.nodes.items()
                if key in kept
            ),
            exports=exported,
            support=tuple(
                key for key in self.nodes if key in retained - public - boundary
            ),
            parents=parents,
            associations=self._direct_associations(kept),
            references=references,
            frozen_context=context,
        )

    def _retain(
        self, exports: Mapping[str, tuple[str, ...]], boundary: set[str]
    ) -> tuple[set[str], tuple[Relation, ...]]:
        retained = {key for values in exports.values() for key in values}
        pending = list(retained)
        inspected: set[str] = set()
        references: set[tuple[str, str]] = set()
        while pending:
            key = pending.pop()
            if key in inspected:
                continue
            inspected.add(key)
            node = self.nodes[self._original(key)]
            if node.artifact.artifact_type not in self.reference_policies:
                msg = "Artifact type has no declared required-reference policy"
                raise ValueError(msg)
            policy = self.reference_policies[node.artifact.artifact_type]
            for reference in () if policy is None else policy(node, self.nodes):
                self._validate_reference(reference)
                referent = self._original(reference.key)
                references.add((key, referent))
                if referent not in boundary:
                    retained.add(referent)
                    pending.append(referent)
        return retained, tuple(
            Relation(source=a, target=b) for a, b in sorted(references)
        )

    def _validate_reference(self, reference: RequiredReference) -> None:
        target = self.nodes.get(reference.key)
        if target is None:
            msg = "Required referent is missing or belongs to an unadmitted result"
            raise ValueError(msg)
        if (target.artifact.artifact_id, target.artifact.artifact_type) != (
            reference.artifact_id,
            reference.artifact_type,
        ):
            msg = "Required referent has the wrong identity or artifact type"
            raise ValueError(msg)

    def _project_parents(
        self, retained: set[str], kept: set[str]
    ) -> tuple[Relation, ...]:
        pairs: set[tuple[str, str]] = set()
        for target in retained:
            pending = list(self.nodes[target].parents)
            visited: set[str] = set()
            while pending:
                parent = pending.pop()
                if parent in visited:
                    continue
                visited.add(parent)
                if parent in kept:
                    pairs.add((parent, target))
                else:
                    pending.extend(self.nodes[parent].parents)
        return tuple(Relation(source=a, target=b) for a, b in sorted(pairs))

    def _original(self, key: str) -> str:
        while self.nodes[key].alias_of is not None:
            key = self.nodes[key].alias_of
        return key

    def _direct_associations(self, kept: set[str]) -> tuple[Relation, ...]:
        representatives: dict[str, list[str]] = {}
        for key in kept:
            representatives.setdefault(self._original(key), []).append(key)
        pairs: set[tuple[str, str]] = set()
        for node in self.nodes.values():
            if node.alias_of is not None:
                continue
            for parent in node.parents:
                for source in representatives.get(self._original(parent), ()):
                    for target in representatives.get(node.key, ()):
                        if source != target:
                            pairs.add((source, target))
        return tuple(Relation(source=a, target=b) for a, b in sorted(pairs))

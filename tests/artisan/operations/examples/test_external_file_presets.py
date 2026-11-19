"""Explicit checkpoint selection and installed command execution."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest
from fixtures.commands import command_router

from artisan.composites.base.compiler import compile_definition
from artisan.execution import run_operation
from artisan.execution.executor_runtime import ExecutorRuntime
from artisan.operations.examples.external_files import (
    ScaleWithExternalWeights,
    default_scale_weights,
    scale_with_weights,
)
from artisan.schemas import MetricArtifact
from artisan.schemas.artifact.external_descriptor import PreparedExternalFiles
from artisan.schemas.external_artifacts import (
    ExternalFile,
    ExternalFilesDescriptor,
    ModelWeightsArtifact,
)
from artisan.storage.external_access import prepare_external_files
from artisan.utils.hashing import compute_content_digest

pytestmark = pytest.mark.usefixtures("external_artifact_types")


@pytest.fixture
def command_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise command execution without claiming prepared software qualification."""
    monkeypatch.setattr(
        ExecutorRuntime, "_software", lambda *_args: lambda *_call: command_router()
    )


def _values() -> tuple[MetricArtifact, ...]:
    return tuple(
        cast(
            MetricArtifact,
            MetricArtifact.draft(
                content={"value": value},
                original_name=f"value-{value}.json",
            ).finalize(),
        )
        for value in (3, 7)
    )


def _weights(content: bytes) -> ModelWeightsArtifact:
    return cast(
        ModelWeightsArtifact,
        ModelWeightsArtifact.draft_weights(
            ExternalFilesDescriptor(
                files=(
                    ExternalFile(
                        path="scale.txt",
                        content_hash=compute_content_digest(content),
                        size_bytes=len(content),
                    ),
                )
            ),
            model_family="example-scalar",
        ).finalize(),
    )


def _prepare(
    root: Path, artifact: ModelWeightsArtifact, content: bytes
) -> PreparedExternalFiles:
    root.mkdir()
    (root / "scale.txt").write_bytes(content)
    return prepare_external_files(root, artifact.external_descriptor())


def test_preset_is_inspectable_and_performs_no_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values = _values()

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Selecting the preset performed filesystem I/O")

    monkeypatch.setattr(Path, "open", forbidden)
    selection = scale_with_weights(values)
    assert selection.origin == "preset"
    assert selection.operation is ScaleWithExternalWeights
    assert selection.weights.artifact_id == default_scale_weights().artifact_id
    assert selection.inputs == {"value": values, "weights": (selection.weights,)}


def test_equal_override_has_identical_scientific_inputs() -> None:
    values = _values()
    selected = scale_with_weights(values)
    explicit = scale_with_weights(values, weights=default_scale_weights())
    assert (selected.origin, explicit.origin) == ("preset", "override")
    assert selected.inputs == explicit.inputs
    assert (
        compile_definition(selected.operation).recipe
        == compile_definition(explicit.operation).recipe
    )
    assert not selected.weights.model_dump().get("origin")


def test_override_replaces_checkpoint_instead_of_adding_one() -> None:
    override = _weights(b"4\n")
    selection = scale_with_weights(_values(), weights=override)
    assert selection.origin == "override"
    assert len(selection.inputs["weights"]) == 1
    assert selection.weights.artifact_id == override.artifact_id
    assert selection.weights.artifact_id != default_scale_weights().artifact_id


def test_preset_rejects_implicit_checkpoint_sweep() -> None:
    with pytest.raises(TypeError, match="declare sweeps explicitly"):
        scale_with_weights(
            _values(), weights=[default_scale_weights(), _weights(b"4\n")]
        )


def test_installed_command_shares_default_and_preserves_exact_parents(
    tmp_path: Path,
    command_execution: None,
) -> None:
    selection = scale_with_weights(_values())
    prepared = _prepare(tmp_path / "weights", selection.weights, b"2\n")
    results = []
    for selected in (
        selection,
        scale_with_weights(selection.values, weights=selection.weights),
    ):
        result = run_operation(
            selected.operation,
            inputs=selected.inputs,
            external_files={cast(str, selected.weights.artifact_id): prepared},
            work_dir=tmp_path / "calls",
        )
        assert result.success
        outputs = cast(list[MetricArtifact], result.artifacts["result"])
        assert [item.values["value"] for item in outputs] == [6.0, 14.0]
        assert len(result.lineage["result"]) == 4
        assert {mapping.source_artifact_id for mapping in result.lineage["result"]} == {
            selection.weights.artifact_id,
            *(item.artifact_id for item in selection.values),
        }
        results.append([item.artifact_id for item in outputs])
    assert results[0] == results[1]
    assert (prepared.root / "scale.txt").read_bytes() == b"2\n"
    assert not list((tmp_path / "calls").rglob("scale.txt"))


def test_multiple_checkpoints_use_an_explicit_cross_product(
    tmp_path: Path,
    command_execution: None,
) -> None:
    weights = (default_scale_weights(), _weights(b"4\n"))
    bindings = {
        cast(str, artifact.artifact_id): _prepare(
            tmp_path / f"weights-{index}", artifact, content
        )
        for index, (artifact, content) in enumerate(
            zip(weights, (b"2\n", b"4\n"), strict=True)
        )
    }
    result = run_operation(
        ScaleWithExternalWeights,
        inputs={"value": _values(), "weights": weights},
        external_files=bindings,
        work_dir=tmp_path / "calls",
    )
    assert result.success
    assert [
        cast(MetricArtifact, item).values["value"]
        for item in result.artifacts["result"]
    ] == [6.0, 12.0, 14.0, 28.0]
    assert len(result.lineage["result"]) == 8

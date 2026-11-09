"""Association snapshots participate in caching and survive concurrent additions."""

from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.frozen_association_ops import AssociationCountCurator

from artisan.execution.inputs.instantiation import instantiate_inputs
from artisan.operations.examples import DataGenerator, MetricCalculator
from artisan.orchestration.engine.inputs import prepare_inputs
from artisan.schemas import InputSpec
from artisan.schemas.execution.replay import ReplaySnapshot, captured_associations
from artisan.schemas.orchestration.step_result import StepDisposition
from artisan.storage import ArtifactStore
from artisan.utils.hashing import compute_execution_spec_id, compute_step_spec_id

from .test_multi_input import AssociatedMetricConsumer
from .test_replay_inputs import _execution, _manager, _runtime

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "consumer", [AssociatedMetricConsumer, AssociationCountCurator]
)
def test_new_association_invalidates_creator_and_curator_cache(
    tmp_path: Path, consumer: type
) -> None:
    runtime = _runtime(tmp_path)
    with _manager(tmp_path) as manager:
        generated = manager.run(
            DataGenerator, params={"count": 1, "seed": 91}, compact=False
        )
        request = {"primary": generated.output("datasets")}
        first = manager.run(consumer, inputs=request, compact=False)
        original = _execution(runtime, first)
        empty = ReplaySnapshot.model_validate_json(original["replay_snapshot"])
        assert empty.associated_complete
        assert empty.associated[0].artifact_ids == []
        cached = manager.run(consumer, inputs=request, compact=False)
        assert cached.disposition == StepDisposition.CACHE_HIT

        manager.run(
            MetricCalculator,
            inputs={"dataset": generated.output("datasets")},
            compact=False,
        )
        refreshed = manager.run(consumer, inputs=request, compact=False)
        current = _execution(runtime, refreshed)
        populated = ReplaySnapshot.model_validate_json(current["replay_snapshot"])
        assert len(populated.associated[0].artifact_ids) == 1
        assert current["execution_spec_id"] != original["execution_spec_id"]
        assert compute_step_spec_id(
            consumer.name, 1, {}, empty.inputs
        ) != compute_step_spec_id(consumer.name, 1, {}, populated.inputs)


def test_prepared_membership_is_not_rediscovered_in_worker(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    with _manager(tmp_path) as manager:
        generated = manager.run(
            DataGenerator, params={"count": 1, "seed": 93}, compact=False
        )
        specs = {"primary": InputSpec(artifact_type="csv", with_associated=("metric",))}
        prepared = prepare_inputs(
            {"primary": generated.output("datasets")},
            runtime.delta_root,
            runtime.storage.filesystem(),
            input_specs=specs,
        )
        captured = captured_associations(prepared.cache_inputs)
        assert captured[0].artifact_ids == []
        before = compute_execution_spec_id("consumer", prepared.cache_inputs)
        manager.run(
            MetricCalculator,
            inputs={"dataset": generated.output("datasets")},
            compact=False,
        )
        store = ArtifactStore(runtime.delta_root)
        _, associated = instantiate_inputs(
            prepared.inputs, store, specs, recorded_associated=captured
        )
        assert list(associated.values()) == [[]]
        assert compute_execution_spec_id("consumer", prepared.cache_inputs) == before


def test_association_capture_uses_one_visibility_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import artisan.storage.core.artifact_store as module

    runtime = _runtime(tmp_path)
    with _manager(tmp_path) as manager:
        generated = manager.run(
            DataGenerator, params={"count": 1, "seed": 95}, compact=False
        )
        prepared = prepare_inputs(
            {"primary": generated.output("datasets")},
            runtime.delta_root,
            runtime.storage.filesystem(),
        )
        primary = prepared.inputs["primary"][0]
        scan = module.scan_committed
        added = False

        def commit_after_capture(*args: object, **kwargs: object):
            nonlocal added
            if not added and kwargs.get("controls") is not None:
                added = True
                manager.run(
                    MetricCalculator,
                    inputs={"dataset": generated.output("datasets")},
                    compact=False,
                )
            return scan(*args, **kwargs)

        monkeypatch.setattr(module, "scan_committed", commit_after_capture)
        store = ArtifactStore(runtime.delta_root)
        captured = store.capture_associated({primary: ("metric", "config")})
        assert added
        assert captured == {(primary, "config"): (), (primary, "metric"): ()}
        assert (
            len(store.capture_associated({primary: ("metric",)})[(primary, "metric")])
            == 1
        )

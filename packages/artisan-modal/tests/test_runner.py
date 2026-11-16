"""Runner construction/lookup remain separate from explicit provisioning."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest
from artisan_modal import ModalRunner, runner
from artisan_modal.bindings import PreparedModalImage
from artisan_modal.image_store import publish_image
from artisan_modal.pack_owner import ModalPackOwner
from artisan_modal.software import ModalSoftware

from artisan.operations.curator.merge import Merge
from artisan.operations.examples.data_generator import DataGenerator
from artisan.operations.examples.data_transformer import DataTransformer
from artisan.orchestration.runner_api import (
    AssignmentLaunchError,
    CancellationStatus,
    Environment,
    OwnershipState,
    RunnerResources,
    compile_definition,
)


def test_import_and_constructor_do_not_import_optional_sdk(tmp_path: Path) -> None:
    program = """
import importlib.abc
import sys
from pathlib import Path
class BlockModal(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "modal" or fullname.startswith("modal."):
            raise AssertionError("SDK imported before explicit provider use")
sys.meta_path.insert(0, BlockModal())
from artisan_modal import ModalRunner
from artisan.orchestration.runner_api import Environment
runner = ModalRunner(Environment.from_pixi(Path(sys.argv[1]), name="worker"), "sample")
assert "modal" not in sys.modules
"""
    manifest = tmp_path / "pixi.toml"
    result = subprocess.run(
        [sys.executable, "-c", program, str(manifest)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_resolve_creator_and_curator_only_read_prepared_requirements(
    prepared: tuple[PreparedModalImage, Environment, Environment],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    image, worker, _ = prepared
    publish_image(image, cache_root=tmp_path)
    names = tuple(
        package.name for package in image.startup.worker.compatibility.packages
    )
    monkeypatch.setattr(ModalRunner, "_providers", lambda _self: ())
    monkeypatch.setattr(runner, "requirements", lambda *_args: (names, ()))
    monkeypatch.setattr(
        runner,
        "controller_compatibility",
        lambda *_args: image.startup.worker.compatibility,
    )
    provision = Mock(side_effect=AssertionError("lookup provisioned"))
    monkeypatch.setattr(runner, "prepare_image", provision)
    selected = ModalRunner(worker, "sample", cache_root=tmp_path)
    resources = RunnerResources(cpus=1, memory_gb=1, gpus=0, time_limit="00:01:00")
    first = selected.resolve(compile_definition(DataTransformer), resources)
    changed = selected.resolve(
        compile_definition(DataTransformer, params={"scale_factor": 7.0}), resources
    )
    curator = selected.resolve_curator(Merge(), resources)
    assert isinstance(first, ModalSoftware)
    assert isinstance(changed, ModalSoftware)
    assert isinstance(curator, ModalSoftware)
    assert first.image == changed.image == curator.image == image
    assert first.software_digest == changed.software_digest == curator.software_digest
    provision.assert_not_called()


def test_prepare_reuses_completed_image_after_failed_deployment(
    prepared: tuple[PreparedModalImage, Environment, Environment],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import modal

    image, worker, _ = prepared
    monkeypatch.setattr(ModalRunner, "_providers", lambda _self: ())
    monkeypatch.setattr(runner, "requirements", lambda *_args: ((), ()))
    monkeypatch.setattr(
        runner,
        "controller_compatibility",
        lambda *_args: image.startup.worker.compatibility,
    )
    build = Mock(return_value=image)
    monkeypatch.setattr(runner, "prepare_image", build)
    monkeypatch.setattr(ModalRunner, "_deployment_exists", lambda *_args: False)
    monkeypatch.setattr(runner, "_function", Mock())
    application = Mock()
    monkeypatch.setattr(modal, "App", Mock(return_value=application))
    selected = ModalRunner(worker, "sample", cache_root=tmp_path)
    application.deploy.side_effect = RuntimeError("failed deployment")
    with pytest.raises(RuntimeError, match="failed deployment"):
        selected.prepare(DataTransformer)
    assert len(tuple(tmp_path.glob("modal/images-v2/*/binding.json"))) == 1
    application.deploy.side_effect = None
    selected.prepare(DataTransformer)
    assert len(tuple(tmp_path.glob("modal/images-v2/*/binding.json"))) == 1
    assert application.deploy.call_args.kwargs["name"].startswith("sample-")
    build.assert_called_once()


def test_repeated_preparation_and_another_operation_reuse_image_and_deployment(
    prepared: tuple[PreparedModalImage, Environment, Environment],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import modal

    image, worker, tool = prepared
    names = tuple(
        package.name for package in image.startup.worker.compatibility.packages
    )
    monkeypatch.setattr(ModalRunner, "_providers", lambda _self: ())
    monkeypatch.setattr(runner, "requirements", lambda *_args: (names, (tool, tool)))
    monkeypatch.setattr(
        runner,
        "controller_compatibility",
        lambda *_args: image.startup.worker.compatibility,
    )
    build = Mock(return_value=image)
    monkeypatch.setattr(runner, "prepare_image", build)
    monkeypatch.setattr(runner, "_function", Mock())
    application = Mock()
    monkeypatch.setattr(modal, "App", Mock(return_value=application))
    deployed = Mock(side_effect=(False, True, True))
    monkeypatch.setattr(ModalRunner, "_deployment_exists", deployed)
    first = ModalRunner(worker, "sample", cache_root=tmp_path)
    second = ModalRunner(worker, "sample", cache_root=tmp_path)
    first.prepare(DataTransformer)
    first.prepare(DataTransformer)
    second.prepare(DataGenerator)
    build.assert_called_once()
    application.deploy.assert_called_once()
    assert deployed.call_count == 3


@pytest.mark.parametrize("error", [None, "missing", "inaccessible"])
def test_deployment_lookup_only_classifies_not_found_as_absent(
    prepared: tuple[PreparedModalImage, Environment, Environment],
    monkeypatch: pytest.MonkeyPatch,
    error: str | None,
) -> None:
    import modal

    image, worker, _ = prepared
    profile = runner.function_profile(
        image.image_id, RunnerResources(cpus=1, memory_gb=1, gpus=0)
    )
    endpoint = Mock()
    if error == "missing":
        endpoint.hydrate.side_effect = modal.exception.NotFoundError("absent")
    elif error == "inaccessible":
        endpoint.hydrate.side_effect = PermissionError("service unavailable")
    lookup = Mock(return_value=endpoint)
    monkeypatch.setattr(modal.Function, "from_name", lookup)
    selected = ModalRunner(worker, "sample")
    if error == "inaccessible":
        with pytest.raises(PermissionError, match="service unavailable"):
            selected._deployment_exists(image, profile)
    else:
        assert selected._deployment_exists(image, profile) is (error is None)
    lookup.assert_called_once_with(
        runner._deployment_name("sample", image, profile), "assignment"
    )
    endpoint.hydrate.assert_called_once_with()


def test_inaccessible_or_ambiguous_preparation_does_not_build_or_deploy(
    prepared: tuple[PreparedModalImage, Environment, Environment],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import modal

    image, worker, _ = prepared
    publish_image(image, cache_root=tmp_path)
    monkeypatch.setattr(ModalRunner, "_providers", lambda _self: ())
    monkeypatch.setattr(runner, "requirements", lambda *_args: ((), ()))
    monkeypatch.setattr(
        runner,
        "controller_compatibility",
        lambda *_args: image.startup.worker.compatibility,
    )
    build = Mock(side_effect=AssertionError("unexpected image build"))
    app = Mock(side_effect=AssertionError("unexpected deployment"))
    monkeypatch.setattr(runner, "prepare_image", build)
    monkeypatch.setattr(modal, "App", app)
    monkeypatch.setattr(
        ModalRunner,
        "_deployment_exists",
        Mock(side_effect=PermissionError("service unavailable")),
    )
    selected = ModalRunner(worker, "sample", cache_root=tmp_path)
    with pytest.raises(PermissionError, match="service unavailable"):
        selected.prepare(DataTransformer)
    monkeypatch.setattr(
        runner, "lookup_image", Mock(side_effect=ValueError("Ambiguous bindings"))
    )
    with pytest.raises(ValueError, match="Ambiguous"):
        selected.prepare(DataTransformer)
    build.assert_not_called()
    app.assert_not_called()


def test_empty_pack_fails_before_any_owner_starts(
    prepared: tuple[PreparedModalImage, Environment, Environment],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, worker, _ = prepared
    owner = Mock()
    monkeypatch.setattr(ModalRunner, "_owner", owner)
    selected = ModalRunner(worker, "sample")
    with pytest.raises(ValueError, match="at least one"):
        selected.launch((), software=Mock(), dispatch=Mock())
    owner.assert_not_called()


def test_thread_start_failure_retains_handle_and_worker_owner(
    prepared: tuple[PreparedModalImage, Environment, Environment],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, worker, _ = prepared
    owner = ModalPackOwner(Mock(), Mock(), count=1)
    monkeypatch.setattr(
        owner._thread, "start", Mock(side_effect=RuntimeError("start failed"))
    )
    monkeypatch.setattr(ModalRunner, "_owner", lambda *_args: owner)
    monkeypatch.setattr(runner, "sensitive_values", lambda _dispatch: ())
    assignment = Mock()
    selected = ModalRunner(worker, "sample")
    with pytest.raises(AssignmentLaunchError) as raised:
        selected.launch((assignment,), software=Mock(), dispatch=Mock())
    error = raised.value
    assert error.handles[0].attempt is assignment.attempt
    assert error.handles[0].worker_owner is owner
    assert error.worker_owners == (owner,)
    assert error.cancellation.status == CancellationStatus.UNKNOWN
    assert owner.ownership_state() == OwnershipState.UNKNOWN


def test_creator_and_curator_handles_share_the_ordered_owner(
    prepared: tuple[PreparedModalImage, Environment, Environment],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, worker, _ = prepared
    owner = Mock(spec=ModalPackOwner)
    captured = []

    def make_owner(
        _self: ModalRunner, admitted: tuple, _software: object, _dispatch: object
    ) -> Mock:
        captured.append(admitted)
        return owner

    monkeypatch.setattr(ModalRunner, "_owner", make_owner)
    monkeypatch.setattr(runner, "sensitive_values", lambda _dispatch: ())
    selected = ModalRunner(worker, "sample")
    assignments = (Mock(), Mock())
    handles = selected.launch(assignments, software=Mock(), dispatch=Mock())
    curator = Mock()
    curator_handle = selected.launch_curator(curator, software=Mock(), dispatch=Mock())
    assert captured == [assignments, (curator,)]
    assert [handle.index for handle in handles] == [0, 1]
    assert all(handle.worker_owner is owner for handle in handles)
    assert curator_handle.worker_owner is owner
    assert curator_handle.index == 0
    assert owner.start.call_count == 2

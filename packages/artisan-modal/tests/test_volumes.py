"""External bundles keep exact volume access separate from scientific identity."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from artisan_modal import ModalRunner, ModalVolumeBinding, runner, runtime
from artisan_modal.bindings import (
    ModalFunctionProfile,
    ModalFunctionRuntime,
    ModalFunctionRuntimeWithVolumes,
    PreparedModalImage,
)
from artisan_modal.image_store import ImageSelection, publish_image
from artisan_modal.pack_control import ModalPack
from artisan_modal.resolution import bind_software, function_profile
from artisan_modal.transport import ModalCall
from artisan_modal.volumes import (
    ModalVolumeMount,
    resolve_volume_mounts,
    validate_volume_bindings,
    validate_volume_mounts,
)
from pydantic import TypeAdapter, ValidationError

from artisan.operations.examples.data_transformer import DataTransformer
from artisan.orchestration.runner_api import (
    Environment,
    PreparedExternalFiles,
    RunnerResources,
    compile_definition,
)
from artisan.schemas.artifact.external_descriptor import (
    ExternalFile,
    ExternalFilesDescriptor,
    expected_receipt_digest,
)

MOUNT = Path("/mnt/artisan-external/references")


def _files(root: Path = MOUNT / "v1") -> PreparedExternalFiles:
    descriptor = ExternalFilesDescriptor(
        files=(ExternalFile(path="reference.bin", content_hash="a" * 32, size_bytes=3),)
    )
    return PreparedExternalFiles(
        root=root,
        descriptor=descriptor,
        receipt_digest=expected_receipt_digest(descriptor),
    )


def _volume(volume_id: str = "vo-selected", mount: Path = MOUNT) -> ModalVolumeMount:
    return ModalVolumeMount(volume_id=volume_id, mount_path=mount)


def _resources() -> RunnerResources:
    return RunnerResources(cpus=1, memory_gb=1, gpus=0, time_limit="00:01:00")


def test_legacy_profile_encoding_stays_exact_and_mounts_are_versioned() -> None:
    profile = function_profile("im-example", _resources())
    expected = {
        "image_id": "im-example",
        "milli_cpu": 1000,
        "memory_mb": 1024,
        "gpu_count": 0,
        "gpu_type": "",
        "max_inputs": 1,
        "timeout_seconds": 60,
    }
    assert type(profile) is ModalFunctionRuntime
    assert profile.model_dump_json() == json.dumps(expected, separators=(",", ":"))
    mounted = function_profile("im-example", _resources(), volumes=(_volume(),))
    assert isinstance(mounted, ModalFunctionRuntimeWithVolumes)
    assert mounted.schema_version == 2
    assert mounted.scientific_profile() == expected
    adapter = TypeAdapter(ModalFunctionProfile)
    assert adapter.validate_json(profile.model_dump_json()) == profile
    assert adapter.validate_json(mounted.model_dump_json()) == mounted
    changed = mounted.model_dump(mode="json")
    changed["schema_version"] = 3
    with pytest.raises(ValidationError):
        adapter.validate_python(changed)


@pytest.mark.parametrize(
    "path",
    ["relative", "/", "/tmp/models", "/opt/artisan/model", str(MOUNT / "../escape")],
)
def test_volume_declarations_reject_unsafe_mount_paths(path: str) -> None:
    with pytest.raises(ValidationError, match="beneath"):
        ModalVolumeBinding(name="files", environment_name="main", mount_path=path)


def test_volume_inventory_rejects_duplicate_sources_and_overlapping_roots() -> None:
    first = ModalVolumeBinding(name="files", environment_name="main", mount_path=MOUNT)
    with pytest.raises(ValueError, match="exactly one declared"):
        validate_volume_bindings(
            (first, first.model_copy(update={"mount_path": MOUNT.parent / "other"}))
        )
    with pytest.raises(ValueError, match="overlap"):
        validate_volume_bindings(
            (
                first,
                first.model_copy(
                    update={"name": "other", "mount_path": MOUNT / "nested"}
                ),
            )
        )
    with pytest.raises(ValueError, match="exactly one resolved"):
        validate_volume_mounts((_volume(), _volume(mount=MOUNT.parent / "other")))


def test_volume_lookup_selects_existing_named_object_without_file_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import modal

    volume = Mock(object_id="vo-selected")
    volume.hydrate.return_value = volume
    lookup = Mock(return_value=volume)
    monkeypatch.setattr(modal.Volume, "from_name", lookup)
    monkeypatch.setattr(Path, "open", Mock(side_effect=AssertionError("payload read")))
    declaration = ModalVolumeBinding(
        name="files", environment_name="science", mount_path=MOUNT
    )
    assert resolve_volume_mounts((declaration,)) == (_volume(),)
    lookup.assert_called_once_with(
        "files", environment_name="science", create_if_missing=False
    )
    volume.hydrate.assert_called_once_with()


def test_runner_freezes_remote_descriptors_without_reading_payloads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    prepared = _files()
    supplied = {"c" * 32: prepared}
    monkeypatch.setattr(Path, "open", Mock(side_effect=AssertionError("payload read")))
    instance = ModalRunner(
        Environment.from_pixi(tmp_path / "pixi.toml"),
        "example",
        volumes=(
            ModalVolumeBinding(name="files", environment_name="main", mount_path=MOUNT),
        ),
        external_files=supplied,
    )
    supplied.clear()
    assert instance.external_files == {"c" * 32: prepared}
    with pytest.raises(TypeError):
        instance.external_files["d" * 32] = prepared
    for root in (MOUNT, Path("/elsewhere/v1")):
        with pytest.raises(ValueError, match="version directory"):
            replace(instance, external_files={"c" * 32: _files(root)})


def test_deployment_identity_changes_but_scientific_digest_ignores_placement(
    prepared: tuple[PreparedModalImage, Environment, Environment],
) -> None:
    image, _, _ = prepared
    compatibility = image.startup.worker.compatibility
    assert compatibility is not None
    first = bind_software(
        compile_definition(DataTransformer),
        ImageSelection(image, {}),
        compatibility,
        _resources(),
        (),
        external_files={"c" * 32: _files()},
        volumes=(_volume(),),
    )
    target = MOUNT.parent / "replica"
    second = bind_software(
        compile_definition(DataTransformer),
        ImageSelection(image, {}),
        compatibility,
        _resources(),
        (),
        external_files={"c" * 32: _files(target / "v1")},
        volumes=(_volume("vo-replica", target),),
    )
    assert first.software_digest == second.software_digest
    assert runner._deployment_name(
        "example", image, first.function
    ) != runner._deployment_name("example", image, second.function)
    assert first.external_provider == "modal"
    assert first.external_mounts[0].source_id == "vo-selected"
    assert first.external_mounts[0].target == MOUNT
    assert first.external_files["c" * 32].root == MOUNT / "v1"


def test_function_mounts_exact_resolved_volume_read_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import modal

    application = modal.App()
    options = []
    volume, read_only = Mock(), Mock()
    volume.read_only.return_value = read_only
    lookup = Mock(return_value=volume)
    monkeypatch.setattr(modal.Volume, "from_id", lookup)
    monkeypatch.setattr(modal.Image, "from_id", Mock(return_value=Mock()))
    monkeypatch.setattr(
        modal.App,
        "function",
        lambda _self, **kwargs: options.append(kwargs) or (lambda fn: fn),
    )
    profile = function_profile("im-example", _resources(), volumes=(_volume(),))
    runner._function(application, profile)
    lookup.assert_called_once_with("vo-selected")
    volume.read_only.assert_called_once_with()
    assert options[0]["volumes"] == {str(MOUNT): read_only}


def _observed_definition() -> SimpleNamespace:
    return SimpleNamespace(
        image_id="im-example",
        resources=SimpleNamespace(
            milli_cpu=1000,
            memory_mb=1024,
            gpu_config=SimpleNamespace(count=0, gpu_type=""),
        ),
        max_concurrent_inputs=1,
        timeout_secs=60,
        volume_mounts=[
            SimpleNamespace(
                volume_id="vo-selected",
                mount_path=str(MOUNT),
                read_only=True,
                allow_background_commits=True,
            )
        ],
    )


def test_runtime_attests_full_actual_mount_inventory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from modal._runtime.container_io_manager import _ContainerIOManager

    definition = _observed_definition()
    monkeypatch.setattr(
        _ContainerIOManager, "_singleton", SimpleNamespace(function_def=definition)
    )
    expected = function_profile("im-example", _resources(), volumes=(_volume(),))
    assert runtime.capture_modal_function() == expected
    runtime.verify_modal_function(expected)
    definition.volume_mounts[0].volume_id = "vo-substitute"
    with pytest.raises(ValueError, match="volume mounts"):
        runtime.verify_modal_function(expected)
    definition.volume_mounts = []
    with pytest.raises(ValueError, match="volume mounts"):
        runtime.verify_modal_function(expected)
    definition.volume_mounts = _observed_definition().volume_mounts
    definition.volume_mounts.append(
        SimpleNamespace(
            volume_id="vo-other",
            mount_path=str(MOUNT.parent / "other"),
            read_only=True,
            allow_background_commits=True,
        )
    )
    with pytest.raises(ValueError, match="volume mounts"):
        runtime.verify_modal_function(expected)


@pytest.mark.parametrize("field", ["read_only", "allow_background_commits"])
def test_runtime_rejects_unqualified_volume_flags(
    field: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from modal._runtime.container_io_manager import _ContainerIOManager

    definition = _observed_definition()
    setattr(definition.volume_mounts[0], field, False)
    monkeypatch.setattr(
        _ContainerIOManager, "_singleton", SimpleNamespace(function_def=definition)
    )
    with pytest.raises(ValidationError, match="read-only SDK mount"):
        runtime.capture_modal_function()


def test_transport_preserves_versioned_mount_profile(pack_request: ModalPack) -> None:
    original = pack_request.calls[0]
    profile = function_profile(
        original.function.image_id, _resources(), volumes=(_volume(),)
    )
    request = original.model_copy(update={"function": profile})
    restored = ModalCall.model_validate_json(request.model_dump_json())
    assert isinstance(restored.function, ModalFunctionRuntimeWithVolumes)
    assert restored.function.mount_inventory() == (_volume(),)
    assert restored.inert().header == request.inert().header


def test_missing_volume_endpoint_does_not_deploy_or_read_payloads(
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
    monkeypatch.setattr(runner, "resolve_volume_mounts", lambda _bindings: (_volume(),))
    monkeypatch.setattr(ModalRunner, "_deployment_exists", lambda *_args: False)
    provision = Mock(side_effect=AssertionError("unexpected provisioning"))
    monkeypatch.setattr(ModalRunner, "_prepare", provision)
    instance = ModalRunner(
        worker,
        "example",
        cache_root=tmp_path,
        volumes=(
            ModalVolumeBinding(name="files", environment_name="main", mount_path=MOUNT),
        ),
        external_files={"c" * 32: _files()},
    )
    with pytest.raises(ValueError, match="selected volumes is not prepared"):
        instance.resolve(compile_definition(DataTransformer), _resources())
    provision.assert_not_called()

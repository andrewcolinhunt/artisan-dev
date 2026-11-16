"""GPU evidence is measured on the worker and never freezes physical placement."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock
from uuid import uuid4

import pytest
from artisan_modal import gpu, pack_worker, runner, software, transport
from artisan_modal.bindings import PreparedModalImage
from artisan_modal.gpu import GPUProbe, GPUProbeReply
from artisan_modal.image_store import publish_image
from artisan_modal.resolution import function_profile
from artisan_modal.software import ModalSoftware
from test_software import binding

from artisan.operations.curator.merge import Merge
from artisan.operations.examples.data_transformer import DataTransformer
from artisan.orchestration.runner_api import (
    AssignmentLaunchError,
    CommandOwnershipError,
    CuratorLaunchError,
    NativeGPUAllocation,
    OwnershipState,
    PreparedLocalBindings,
    RunnerResources,
    compile_definition,
)


def allocation(
    device: str = "00000000-0000-0000-0000-000000000001",
) -> NativeGPUAllocation:
    return NativeGPUAllocation.model_validate(
        {
            "devices": ["GPU-" + device],
            "driver": {
                "driver_version": "test-driver",
                "device_models": ["test-model"],
                "libcuda_sha256": "a" * 64,
                "nvidia_smi_sha256": "b" * 64,
            },
            "libraries": [
                {
                    "name": "libcuda.so.1",
                    "path": "/driver/libcuda.so.1",
                    "sha256": "a" * 64,
                },
                {
                    "name": "libnvidia-ptxjitcompiler.so.1",
                    "path": "/driver/libnvidia-ptxjitcompiler.so.1",
                    "sha256": "c" * 64,
                },
            ],
            "shell_sha256": "d" * 64,
        }
    )


def selected(image: PreparedModalImage) -> ModalSoftware:
    current = binding(image)
    return replace(
        current,
        function=current.function.model_copy(
            update={"gpu_count": 1, "gpu_type": "A10G"}
        ),
        gpu=allocation().software_profile(),
    )


def request(image: PreparedModalImage) -> GPUProbe:
    current = selected(image)
    return GPUProbe(
        nonce=uuid4(),
        function=current.function,
        runtime=image.startup.runtime,
        compatibility=current.compatibility,
    )


@pytest.mark.parametrize(
    ("extra", "count"),
    [
        ({}, 1),
        ({"gpu_type": "A10G"}, 0),
        ({"gpu_type": "A10G:2"}, 1),
        ({"gpu_type": "a10g"}, 1),
        ({"gpu_type": "A10G", "other": True}, 1),
    ],
)
def test_resource_validation_rejects_implicit_or_ambiguous_gpu(
    extra: dict, count: int
) -> None:
    with pytest.raises(ValueError, match="Modal"):
        function_profile("im-qualified", RunnerResources(gpus=count, extra=extra))


def test_resource_configuration_matches_pinned_sdk() -> None:
    from modal._utils.function_utils import parse_gpu_config

    profile = function_profile(
        "im-qualified", RunnerResources(gpus=2, extra={"gpu_type": "A10G"})
    )
    actual = parse_gpu_config(f"{profile.gpu_type}:{profile.gpu_count}")
    assert (actual.count, actual.gpu_type) == (2, "A10G")


def test_live_allocation_is_not_in_scientific_identity(
    prepared: tuple, monkeypatch: pytest.MonkeyPatch
) -> None:
    current = selected(prepared[0])
    first = allocation()
    second = allocation("00000000-0000-0000-0000-000000000002")
    assert first != second
    assert first.software_profile() == second.software_profile()
    assert (
        current.software_digest
        == replace(current, gpu=second.software_profile()).software_digest
    )
    seen = []
    monkeypatch.setattr(software, "capture_worker_gpu", lambda *_: second)
    monkeypatch.setattr(
        PreparedLocalBindings,
        "create_adapter",
        lambda local, *_args, **_kwargs: seen.append(local),
    )
    current.create_adapter(Mock(), working_root=Path("/worker"), runtime_env={})
    assert seen[0].native_gpu == second
    assert current.local.native_gpu is None
    changed = current.gpu.model_copy(update={"shell_sha256": "e" * 64})
    assert current.software_digest != replace(current, gpu=changed).software_digest


def test_gpu_probe_verifies_qualified_code_before_measurement(
    prepared: tuple, monkeypatch: pytest.MonkeyPatch
) -> None:
    probe = request(prepared[0])
    seen = []
    for name in (
        "verify_worker_activation",
        "verify_modal_runtime",
        "verify_modal_function",
        "verify_runtime_compatibility",
    ):
        monkeypatch.setattr(gpu, name, lambda *_args, phase=name: seen.append(phase))
    monkeypatch.setattr(
        gpu, "capture_worker_gpu", lambda *_: (seen.append("measure"), allocation())[1]
    )
    reply = gpu.probe_gpu(probe)
    reply.verify(probe)
    assert reply.software == allocation().software_profile()
    assert seen == [
        "verify_worker_activation",
        "verify_modal_runtime",
        "verify_modal_function",
        "verify_runtime_compatibility",
        "measure",
    ]
    assert "GPU-" not in reply.model_dump_json()
    assert "/driver/" not in reply.model_dump_json()
    with pytest.raises(ValueError, match="fresh request"):
        reply.verify(probe.model_copy(update={"nonce": uuid4()}))


def test_probe_compatibility_failure_never_measures_gpu(
    prepared: tuple, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name in (
        "verify_worker_activation",
        "verify_modal_runtime",
        "verify_modal_function",
    ):
        monkeypatch.setattr(gpu, name, Mock())
    monkeypatch.setattr(
        gpu,
        "verify_runtime_compatibility",
        Mock(side_effect=ValueError("code changed")),
    )
    measure = Mock()
    monkeypatch.setattr(gpu, "capture_worker_gpu", measure)
    reply = gpu.probe_gpu(request(prepared[0]))
    assert reply.software is None
    assert "code changed" in reply.error
    measure.assert_not_called()


def test_worker_rejects_driver_drift_and_invalid_loader(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = allocation()
    monkeypatch.setattr(gpu, "capture_native_gpu", lambda _: current)
    loader = Mock()
    monkeypatch.setattr(gpu, "verify_native_gpu_loader", loader)
    changed = current.software_profile().model_copy(update={"shell_sha256": "e" * 64})
    with pytest.raises(ValueError, match="changed after"):
        gpu.capture_worker_gpu(1, changed)
    loader.assert_not_called()
    loader.side_effect = ValueError("foreign libcuda")
    with pytest.raises(ValueError, match="foreign libcuda"):
        gpu.capture_worker_gpu(1, current.software_profile())


@pytest.mark.usefixtures("pack_runtime")
def test_probe_and_science_share_poisoned_worker_lease(
    prepared: tuple, monkeypatch: pytest.MonkeyPatch
) -> None:
    probe = request(prepared[0])
    monkeypatch.setattr(
        pack_worker, "probe_gpu", Mock(side_effect=CommandOwnershipError("unknown"))
    )
    with pytest.raises(CommandOwnershipError, match="unknown"):
        pack_worker.invoke_pack(probe.model_dump_json().encode())
    with pytest.raises(CommandOwnershipError, match="unresolved pack"):
        pack_worker.invoke_pack(probe.model_dump_json().encode())


@pytest.mark.usefixtures("pack_runtime")
def test_completed_probe_releases_lease_without_authored_entry(
    prepared: tuple, monkeypatch: pytest.MonkeyPatch
) -> None:
    probe = request(prepared[0])
    reply = GPUProbeReply(
        nonce=probe.nonce,
        request_sha256=probe.digest,
        entry_id=uuid4(),
        software=allocation().software_profile(),
    )
    monkeypatch.setattr(pack_worker, "probe_gpu", lambda _: reply)
    science = Mock(side_effect=AssertionError("authored science"))
    monkeypatch.setattr(pack_worker, "execute_call", science)
    assert (
        pack_worker.invoke_pack(probe.model_dump_json().encode())
        == reply.model_dump_json().encode()
    )
    assert (
        pack_worker.invoke_pack(probe.model_dump_json().encode())
        == reply.model_dump_json().encode()
    )
    science.assert_not_called()


@pytest.mark.parametrize("curator", [False, True])
def test_resolve_remeasures_before_each_cache_decision(
    prepared: tuple, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, curator: bool
) -> None:
    image, worker, _ = prepared
    publish_image(image, cache_root=tmp_path)
    names = tuple(
        package.name for package in image.startup.worker.compatibility.packages
    )
    monkeypatch.setattr(runner.ModalRunner, "_providers", lambda _: ())
    monkeypatch.setattr(runner, "requirements", lambda *_: (names, ()))
    monkeypatch.setattr(
        runner,
        "controller_compatibility",
        lambda *_: image.startup.worker.compatibility,
    )
    seen = []

    def spawn(encoded: bytes) -> Mock:
        probe = GPUProbe.model_validate_json(encoded)
        seen.append(probe)
        reply = GPUProbeReply(
            nonce=probe.nonce,
            request_sha256=probe.digest,
            entry_id=uuid4(),
            software=allocation().software_profile(),
        )
        return Mock(get=lambda: reply.model_dump_json().encode())

    monkeypatch.setattr(
        runner.ModalRunner, "_lookup_profile", lambda *_: Mock(spawn=spawn)
    )
    provider = runner.ModalRunner(worker, "sample", cache_root=tmp_path)
    resources = RunnerResources(gpus=1, extra={"gpu_type": "A10G"})

    def resolve() -> object:
        if curator:
            return provider.resolve_curator(Merge(), resources)
        return provider.resolve(compile_definition(DataTransformer), resources)

    assert resolve().software_digest == resolve().software_digest
    assert len(seen) == 2
    assert seen[0].nonce != seen[1].nonce


@pytest.mark.parametrize("curator", [False, True])
def test_resolution_failure_retains_unknown_probe_owner(
    prepared: tuple, monkeypatch: pytest.MonkeyPatch, curator: bool
) -> None:
    image, worker, _ = prepared
    function = Mock(spawn=Mock(side_effect=OSError("submission ambiguous")))
    monkeypatch.setattr(runner.ModalRunner, "_lookup_profile", lambda *_: function)
    provider = runner.ModalRunner(worker, "sample")
    error_type = CuratorLaunchError if curator else AssignmentLaunchError
    with pytest.raises(error_type) as caught:
        provider._measure_gpu(image, selected(image).function, curator=curator)
    assert caught.value.handles == ()
    assert len(caught.value.worker_owners) == 1
    assert caught.value.worker_owners[0].ownership_state() == OwnershipState.UNKNOWN


@pytest.mark.parametrize("phase", ["code", "driver"])
def test_gpu_preflight_rejects_before_any_unpickle(
    pack_request: object, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    from artisan_modal.transport import ModalCall

    from artisan.orchestration.runner_api import InertCallControl

    original = pack_request.calls[0]
    call = ModalCall.model_validate(
        {
            **original.model_dump(),
            "function": original.function.model_copy(
                update={"gpu_count": 1, "gpu_type": "A10G"}
            ),
            "gpu": allocation().software_profile(),
        }
    )
    for name in (
        "verify_worker_activation",
        "verify_modal_runtime",
        "verify_modal_function",
    ):
        monkeypatch.setattr(transport, name, Mock())
    code = Mock(side_effect=ValueError("changed code") if phase == "code" else None)
    driver = Mock(side_effect=ValueError("changed driver"))
    monkeypatch.setattr(transport, "verify_runtime_compatibility", code)
    monkeypatch.setattr(transport, "capture_worker_gpu", driver)
    execute = Mock()
    monkeypatch.setattr(transport, "execute_inert_call", execute)
    with pytest.raises(ValueError, match="changed"):
        transport.execute_call(call, InertCallControl())
    execute.assert_not_called()
    if phase == "code":
        driver.assert_not_called()
    else:
        driver.assert_called_once_with(1, call.gpu)


def test_gpu_software_rechecks_actual_driver_before_curator_or_creator(
    prepared: tuple, monkeypatch: pytest.MonkeyPatch
) -> None:
    selected_software = selected(prepared[0])
    monkeypatch.setattr(software, "verify_modal_runtime", Mock())
    monkeypatch.setattr(software, "verify_modal_function", Mock())
    monkeypatch.setattr(
        software,
        "capture_worker_gpu",
        Mock(side_effect=ValueError("current driver differs")),
    )
    verify_creator, verify_curator = Mock(), Mock()
    monkeypatch.setattr(PreparedLocalBindings, "verify", verify_creator)
    monkeypatch.setattr(PreparedLocalBindings, "verify_worker", verify_curator)
    with pytest.raises(ValueError, match="current driver"):
        selected_software.verify_worker()
    with pytest.raises(ValueError, match="current driver"):
        selected_software.verify(compile_definition(DataTransformer))
    verify_creator.assert_not_called()
    verify_curator.assert_not_called()


def test_assignment_function_uses_exact_gpu_resource_string(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import modal

    seen = []
    application = modal.App()

    def decorate(_self: object, **options: object) -> object:
        seen.append(options)
        return lambda function: function

    monkeypatch.setattr(modal.App, "function", decorate)
    monkeypatch.setattr(modal.Image, "from_id", Mock(return_value=Mock()))
    profile = function_profile(
        "im-qualified", RunnerResources(gpus=2, extra={"gpu_type": "A10G"})
    )
    assert runner._function(application, profile) is pack_worker.invoke_pack
    assert seen[0]["gpu"] == "A10G:2"
    assert seen[0]["retries"] == 0
    assert seen[0]["max_containers"] == 1

"""Curators share measured worker software without declaring command targets."""

from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path

import pytest

from artisan.composites.base.compiler import compile_definition
from artisan.execution.environments.curator_software import resolve_curator_software
from artisan.execution.environments.observed_worker import ObservedWorker
from artisan.execution.environments.worker_qualification import (
    prepare_worker_qualification,
)
from artisan.execution.local_assignment import (
    PreparedToolBinding,
    bind_local_software,
    bind_local_worker,
)
from artisan.execution.local_profile import LocalLauncherIdentity
from artisan.operations.curator.merge import Merge
from artisan.operations.examples import DataGenerator
from artisan.orchestration.runners.local import LocalRunner
from artisan.schemas.execution.container_environment import PreparedOCIEnvironment
from artisan.schemas.execution.gpu import (
    GPUDriverIdentity,
    GPUDriverLibrary,
    NativeGPUAllocation,
)
from artisan.schemas.execution.prepared_environment import PreparedNativeEnvironment
from artisan.schemas.execution.worker_transport import (
    TRANSPORT_PACKAGES,
    DistributionMetadata,
    RuntimeCompatibility,
)
from artisan.schemas.operation_config.environment import Environment
from artisan.schemas.operation_config.runner_resources import RunnerResources

MODULE = "artisan.execution.environments.curator_software"
LOCAL = "artisan.execution.local_assignment"


@pytest.fixture
def worker(oci: PreparedOCIEnvironment) -> PreparedNativeEnvironment:
    compatibility = RuntimeCompatibility(
        python_implementation="CPython",
        python_version="3.12.12",
        cache_tag="cpython-312",
        packages=tuple(
            DistributionMetadata(
                name=name,
                version="1",
            )
            for name in sorted(TRANSPORT_PACKAGES)
        ),
    )
    return oci.native.model_copy(update={"compatibility": compatibility})


def _observed(worker: PreparedNativeEnvironment) -> ObservedWorker:
    assert worker.compatibility is not None
    return ObservedWorker(
        Path(sys.executable),
        Path(sys.prefix),
        Path(sys.base_prefix),
        "a" * 64,
        worker.compatibility,
        {"SYNTHETIC_ACTIVATION": "literal $VALUE"},
    )


def _gpu() -> NativeGPUAllocation:
    return NativeGPUAllocation(
        devices=("GPU-00000000-0000-0000-0000-000000000001",),
        driver=GPUDriverIdentity(
            driver_version="synthetic-driver",
            device_models=("synthetic-device",),
            libcuda_sha256="a" * 64,
            nvidia_smi_sha256="b" * 64,
        ),
        libraries=tuple(
            GPUDriverLibrary(name=name, path=Path("/driver") / name, sha256="a" * 64)
            for name in ("libcuda.so.1", "libnvidia-ptxjitcompiler.so.1")
        ),
        shell_sha256="c" * 64,
    )


def test_prepared_worker_binding_uses_same_verification_and_identity_as_native_plan(
    worker: PreparedNativeEnvironment, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    identity = LocalLauncherIdentity("pixi 0.83.0", "b" * 64, "c" * 64, ("glibc", "2"))
    verified = []
    monkeypatch.setattr(LocalLauncherIdentity, "capture", lambda *args: identity)
    monkeypatch.setattr(f"{LOCAL}.verify_native_environment", verified.append)
    arguments = {"pixi": tmp_path / "pixi", "system_env": Path("/usr/bin/env")}
    bound = bind_local_worker(worker, **arguments)
    creator = bind_local_software(
        compile_definition(DataGenerator), worker, cache_root=tmp_path, **arguments
    )
    assert verified == [worker, worker]
    assert bound.software_digest == creator.software_digest
    assert bound.tools == ()
    assert bound.software_cacheable
    changed = replace(identity, pixi_sha256="d" * 64)
    monkeypatch.setattr(LocalLauncherIdentity, "capture", lambda *args: changed)
    with pytest.raises(ValueError, match="launcher identity changed"):
        bound.verify_worker()


def test_observed_worker_binding_retains_gpu_integrity_and_disables_cache(
    worker: PreparedNativeEnvironment, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    observed, gpu = _observed(worker), _gpu()
    checked: list[object] = []
    monkeypatch.setattr(ObservedWorker, "verify", lambda self: checked.append(self))
    monkeypatch.setattr(f"{LOCAL}.verify_native_gpu", checked.append)
    monkeypatch.setattr(
        LocalLauncherIdentity, "capture", lambda *args: pytest.fail("Inspected Pixi")
    )
    bound = bind_local_worker(
        observed,
        pixi=tmp_path / "unused",
        system_env=tmp_path / "unused",
        native_gpu=gpu,
    )
    assert checked == [gpu, observed]
    assert bound.native_gpu == gpu
    assert not bound.software_cacheable
    assert len(bound.software_digest) == 64
    assert bound.launcher is None
    assert replace(bound, native_gpu=None).software_digest != bound.software_digest
    assert (
        replace(bound, application_modules=("json",)).software_digest
        != bound.software_digest
    )
    with pytest.raises(ValueError, match="cannot contain command"):
        replace(bound, tools=(PreparedToolBinding((), observed),)).verify_worker()


def test_observed_curator_resolution_qualifies_collected_roots_without_preparation(
    worker: PreparedNativeEnvironment, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    observed = _observed(worker)
    names = tuple(package.name for package in observed.compatibility.packages)
    sources = {"synthetic-application": {"module.py": tmp_path / "module.py"}}
    calls: list[object] = []

    def collect(
        definitions: tuple[type, ...],
        providers: tuple[type, ...],
        *,
        application_modules: tuple[str, ...] = (),
    ) -> tuple[tuple[str, ...], dict[str, dict[str, Path]]]:
        assert definitions == (Merge,)
        assert providers == (Merge,)
        return names, sources

    def qualify(selected: tuple[str, ...], **kwargs: object) -> ObservedWorker:
        calls.append((selected, kwargs))
        return observed

    monkeypatch.setattr(f"{MODULE}.observed_definition_sources", collect)
    monkeypatch.setattr(f"{MODULE}.qualify_observed_worker", qualify)
    monkeypatch.setattr(ObservedWorker, "verify", lambda _self: None)
    monkeypatch.setattr(
        f"{MODULE}.lookup_native_environment",
        lambda *a, **k: pytest.fail("Looked up Pixi"),
    )
    bound = resolve_curator_software(
        Merge(),
        (Merge,),
        environment=None,
        cache_root=tmp_path,
        pixi=tmp_path / "unused",
        system_env=tmp_path / "unused",
    )
    assert calls == [(names, {})]
    assert bound.worker == observed
    assert bound.tools == ()
    assert not bound.software_cacheable


def test_prepared_curator_resolution_only_looks_up_existing_qualification(
    worker: PreparedNativeEnvironment, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    declaration = Environment.from_pixi(tmp_path / "pixi.toml", name="default")
    identity = LocalLauncherIdentity("pixi 0.83.0", "b" * 64, "c" * 64, ("glibc", "2"))
    names = tuple(sorted(TRANSPORT_PACKAGES))
    monkeypatch.setattr(f"{MODULE}.lookup_native_environment", lambda *a, **k: worker)
    monkeypatch.setattr(
        f"{MODULE}.controller_definition_sources", lambda *a, **k: (names, {})
    )
    monkeypatch.setattr(LocalLauncherIdentity, "capture", lambda *a: identity)
    monkeypatch.setattr(f"{LOCAL}.verify_native_environment", lambda target: None)
    monkeypatch.setattr(
        f"{MODULE}.qualify_observed_worker",
        lambda *a, **k: pytest.fail("Measured child"),
    )
    with pytest.raises(ValueError, match="prepare explicitly"):
        resolve_curator_software(
            Merge(),
            (Merge,),
            environment=declaration,
            cache_root=tmp_path,
            pixi=tmp_path / "pixi",
            system_env=Path("/usr/bin/env"),
        )
    qualification = "artisan.execution.environments.worker_qualification"
    monkeypatch.setattr(f"{qualification}.qualify_local_worker", lambda *a, **k: worker)
    monkeypatch.setattr(
        f"{qualification}.verify_runtime_compatibility", lambda *a, **k: None
    )
    prepare_worker_qualification(
        worker,
        names,
        cache_root=tmp_path,
        pixi=tmp_path / "pixi",
        system_env=Path("/usr/bin/env"),
    )
    monkeypatch.setattr(
        "subprocess.Popen", lambda *a, **k: pytest.fail("Lookup launched a process")
    )
    bound = resolve_curator_software(
        Merge(),
        (Merge,),
        environment=declaration,
        cache_root=tmp_path,
        pixi=tmp_path / "pixi",
        system_env=Path("/usr/bin/env"),
    )
    assert bound.worker == worker
    assert bound.software_cacheable
    assert bound.tools == ()


def test_noncurator_rejects_before_software_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        f"{MODULE}.observed_definition_sources",
        lambda *a: pytest.fail("Collected code"),
    )
    with pytest.raises(TypeError, match="curator"):
        resolve_curator_software(
            DataGenerator(),
            (Merge,),
            environment=None,
            cache_root=tmp_path,
            pixi=tmp_path / "unused",
            system_env=tmp_path / "unused",
        )


@pytest.mark.parametrize("prepared", [False, True])
def test_private_runner_prepares_only_curator_worker(
    worker: PreparedNativeEnvironment,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    prepared: bool,
) -> None:
    module = "artisan.orchestration.runners.local"
    declaration = Environment.from_pixi(tmp_path / "pixi.toml") if prepared else None
    runner = LocalRunner(
        environment=declaration, cache_root=tmp_path, tool_backend="docker"
    )
    calls: list[str] = []

    def prepare(*args: object, **kwargs: object) -> PreparedNativeEnvironment:
        calls.append("prepare")
        return worker

    def qualify(*args: object, **kwargs: object) -> PreparedNativeEnvironment:
        calls.append("qualify")
        return worker

    def resolve(
        self: LocalRunner,
        operation: Merge,
        resources: RunnerResources,
        *,
        application_modules: tuple[str, ...] = (),
    ) -> None:
        assert self is runner
        assert type(operation) is Merge
        assert resources.gpus == 0
        calls.append("resolve")

    monkeypatch.setattr(f"{module}.prepare_native_environment", prepare)
    monkeypatch.setattr(f"{module}.prepare_worker_qualification", qualify)
    monkeypatch.setattr(
        f"{module}.controller_definition_sources",
        lambda *a, **kw: (tuple(TRANSPORT_PACKAGES), {}),
    )
    monkeypatch.setattr(LocalRunner, "resolve_curator", resolve)
    monkeypatch.setattr(
        f"{module}.prepare_command_witness",
        lambda *a: pytest.fail("Prepared a command"),
    )
    monkeypatch.setattr(
        f"{module}.prepare_oci_environment",
        lambda *a, **k: pytest.fail("Prepared a tool"),
    )
    runner.prepare_curator(Merge())
    assert calls == (["prepare", "qualify", "resolve"] if prepared else ["resolve"])


def test_private_runner_forwards_exact_native_gpu_allocation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = "artisan.orchestration.runners.local"
    gpu = _gpu()
    monkeypatch.setattr(f"{module}.capture_native_gpu", lambda count: gpu)
    selected: list[object] = []

    def resolve(*args: object, **kwargs: object) -> None:
        selected.append(kwargs["native_gpu"])

    monkeypatch.setattr(f"{module}.resolve_curator_software", resolve)
    runner = LocalRunner(cache_root=tmp_path)
    runner.resolve_curator(Merge(), RunnerResources(gpus=1))
    assert selected == [gpu]

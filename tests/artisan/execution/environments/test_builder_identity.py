"""Prepared containers retain build recipe and exact local-wheel identities."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock

import pytest

from artisan.execution.environments.container_runtime import (
    ApptainerRuntime,
    DockerRuntime,
)
from artisan.execution.environments.container_source import ContainerBuildInputs
from artisan.execution.environments.native import wheel_builder_digest
from artisan.execution.environments.prepared_oci import (
    _reuse_oci_environment,
    lookup_oci_environment,
    prepare_oci_environment,
)
from artisan.execution.environments.prepared_sif import lookup_sif_source_environment
from artisan.execution.environments.source import CapturedEnvironment
from artisan.schemas.execution.container_environment import (
    PreparedOCIEnvironment,
    PreparedSIFEnvironment,
)
from artisan.schemas.execution.prepared_environment import (
    DerivedWheel,
    EnvironmentFile,
    LocalPackageSource,
    SourcePayload,
)
from artisan.schemas.operation_config.environment import Environment


@pytest.fixture
def local_oci(oci: PreparedOCIEnvironment) -> PreparedOCIEnvironment:
    package = LocalPackageSource(
        root=".",
        name="probe",
        version="1.0",
        files=("pyproject.toml", "src/probe.py"),
        payloads=(SourcePayload(path="probe.py", source="src/probe.py"),),
        scan_roots=("src",),
        membership_sha256="1" * 64,
    )
    source = oci.native.source.model_copy(
        update={
            "packages": (package,),
            "files": (
                *oci.native.source.files,
                EnvironmentFile(path="pyproject.toml", sha256="2" * 64),
                EnvironmentFile(path="src/probe.py", sha256="3" * 64),
            ),
        }
    )
    wheel = DerivedWheel(
        name="probe",
        version="1.0",
        source_root=".",
        source_digest="4" * 64,
        builder_digest=wheel_builder_digest("linux-64"),
        filename="probe-1.0-py3-none-any.whl",
        sha256="5" * 64,
    )
    return oci.model_copy(
        update={
            "native": oci.native.model_copy(
                update={"source": source, "wheels": (wheel,)}
            )
        }
    )


@pytest.mark.parametrize("change", ["backend", "profile"])
def test_container_lookup_rejects_changed_builder_without_builder_cache(
    tmp_path: Path,
    local_oci: PreparedOCIEnvironment,
    sif: PreparedSIFEnvironment,
    docker: DockerRuntime,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
) -> None:
    selected_sif = sif.model_copy(update={"oci": local_oci})
    for kind, target in (("oci", local_oci), ("sif", selected_sif)):
        binding = tmp_path / f"{kind}-v2/prepared/binding.json"
        binding.parent.mkdir(parents=True)
        binding.write_text(target.model_dump_json())
        module = f"artisan.execution.environments.prepared_{kind}"
        monkeypatch.setattr(
            f"{module}.read_environment_inputs", lambda *a, **k: object()
        )
        monkeypatch.setattr(
            f"{module}._capture_current_source",
            lambda *a: CapturedEnvironment(local_oci.native.source, ()),
        )
        monkeypatch.setattr(f"{module}.verify_{kind}_environment", lambda *a, **k: None)
    declaration = Environment.from_pixi(tmp_path / "pixi.toml")
    runtime = ApptainerRuntime(docker.executable)
    assert (
        lookup_oci_environment(declaration, cache_root=tmp_path, docker=docker)
        == local_oci
    )
    assert (
        lookup_sif_source_environment(
            declaration, cache_root=tmp_path, apptainer=runtime
        )
        == selected_sif
    )
    monkeypatch.setattr(
        "artisan.execution.environments.prepared_oci.pixi_version", lambda: None
    )
    monkeypatch.setattr(
        "artisan.execution.environments.prepared_oci.pixi_executable",
        lambda: str(docker.executable),
    )
    monkeypatch.setattr(
        "artisan.execution.environments.prepared_oci.file_digest",
        lambda _: local_oci.pixi_sha256,
    )
    if change == "backend":
        backend = tmp_path / "backend.py"
        backend.write_text("# changed static build backend\n")
        monkeypatch.setattr(
            "artisan.execution.environments.native.BACKEND_SCRIPT", backend
        )
    else:
        monkeypatch.setattr(
            "artisan.execution.environments.native.HATCH_VERSION", "99.0"
        )
    assert (
        _reuse_oci_environment(
            declaration,
            tmp_path,
            docker,
            local_oci.base_image_id,
            local_oci.build_runtime,
        )
        is None
    )
    with pytest.raises(ValueError, match="prepare it explicitly"):
        lookup_oci_environment(declaration, cache_root=tmp_path, docker=docker)
    with pytest.raises(ValueError, match="prepare an unambiguous target explicitly"):
        lookup_sif_source_environment(
            declaration, cache_root=tmp_path, apptainer=runtime
        )


def test_oci_cache_key_includes_same_filename_wheel_bytes(
    tmp_path: Path,
    local_oci: PreparedOCIEnvironment,
    docker: DockerRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = "artisan.execution.environments.prepared_oci"
    inputs = ContainerBuildInputs(
        CapturedEnvironment(local_oci.native.source, ()),
        local_oci.native.wheels,
        (),
        None,
        local_oci.pixi_sha256,
    )
    changed = ContainerBuildInputs(
        inputs.captured,
        (inputs.wheels[0].model_copy(update={"sha256": "6" * 64}),),
        (),
        None,
        inputs.pixi_sha256,
    )
    capture = Mock(side_effect=(inputs, changed, changed))
    monkeypatch.setattr(f"{module}.capture_container_inputs", capture)
    monkeypatch.setattr(f"{module}._reuse_oci_environment", lambda *a: None)
    monkeypatch.setattr(f"{module}._bind_base", lambda *a: "base")
    monkeypatch.setattr(
        f"{module}.write_container_recipe", lambda *a, **k: local_oci.recipe_sha256
    )
    monkeypatch.setattr(f"{module}._verify_current_source", lambda *a: None)
    monkeypatch.setattr(f"{module}.verify_oci_environment", lambda *a, **k: None)
    monkeypatch.setattr(f"{module}._seal_image_binding", lambda target, **k: target)
    monkeypatch.setattr(DockerRuntime, "identity", lambda _: local_oci.build_runtime)
    metadata = {
        "Id": local_oci.base_image_id,
        "Os": "linux",
        "Architecture": "amd64",
        "Config": {},
        "RootFS": {"Layers": ["base-layer"]},
    }
    monkeypatch.setattr(DockerRuntime, "inspect_image", lambda *a: metadata)

    def build(command: list[str], **_: object) -> None:
        Path(command[command.index("--iidfile") + 1]).write_text(local_oci.image_id)

    builder = Mock(side_effect=build)
    monkeypatch.setattr(f"{module}.subprocess.run", builder)
    declaration = Environment.from_pixi(tmp_path / "pixi.toml")
    first = prepare_oci_environment(
        declaration, cache_root=tmp_path, docker=docker, base_image="base"
    )
    second = prepare_oci_environment(
        declaration, cache_root=tmp_path, docker=docker, base_image="base"
    )
    reused = prepare_oci_environment(
        declaration, cache_root=tmp_path, docker=docker, base_image="base"
    )
    assert first.native.wheels == inputs.wheels
    assert second.native.wheels == changed.wheels
    assert reused == second
    assert builder.call_count == 2
    assert len(tuple(tmp_path.glob("oci-v2/*/binding.json"))) == 2

"""External directory binds preserve ordinary artifact identity and command ownership."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import pytest

from artisan.execution.assignment import Assignment
from artisan.execution.environments.command_witness import WitnessInvocation
from artisan.execution.environments.container_launch import (
    PreparedContainerCommandEnvironment,
)
from artisan.execution.environments.container_runtime import (
    ApptainerRuntime,
    DockerRuntime,
)
from artisan.execution.environments.external_preflight import (
    MAGIC,
    encode_external_preflight,
)
from artisan.execution.environments.observed_worker import observe_current_worker
from artisan.execution.local_assignment import (
    PreparedLocalBindings,
    PreparedToolBinding,
    container_external_files,
    container_input_files,
)
from artisan.schemas.artifact.external_descriptor import (
    ExternalFile,
    ExternalFilesDescriptor,
    PreparedExternalFiles,
)
from artisan.schemas.execution.command_witness import PreparedCommandWitness
from artisan.schemas.execution.container_environment import (
    PreparedOCIEnvironment,
    PreparedSIFEnvironment,
)
from artisan.schemas.execution.worker_transport import TRANSPORT_PACKAGES
from artisan.schemas.external_artifacts import ExternalFilesArtifact
from artisan.storage.external_access import prepare_external_files
from artisan.utils.hashing import compute_content_digest

pytestmark = pytest.mark.usefixtures("external_artifact_types")


def _installation(root: Path, content: bytes = b"external") -> PreparedExternalFiles:
    root.mkdir(parents=True)
    (root / "data.bin").write_bytes(content)
    descriptor = ExternalFilesDescriptor(
        files=(
            ExternalFile(
                path="data.bin",
                content_hash=compute_content_digest(content),
                size_bytes=len(content),
            ),
        )
    )
    return prepare_external_files(root, descriptor)


def _artifact(prepared: PreparedExternalFiles) -> ExternalFilesArtifact:
    artifact = ExternalFilesArtifact.draft(prepared.descriptor)
    artifact.finalize()
    artifact.bind_external_root(prepared.root)
    return artifact


def _witness(sandbox: Path) -> WitnessInvocation:
    return WitnessInvocation(
        PreparedCommandWitness(
            executable=sandbox.parent / "witness",
            architecture="x86_64",
            source_sha256="a" * 64,
            binary_sha256="b" * 64,
            recipe_sha256="c" * 64,
            builder_digest="d" * 64,
            compiler_version="synthetic",
            compiler_files=(),
        ),
        sandbox / "command-runtime/exits",
    )


def test_container_selects_shared_external_inputs_without_extra_configured_roots(
    tmp_path: Path,
) -> None:
    selected = _installation(tmp_path / "selected")
    unused = _installation(tmp_path / "unused", b"unused")
    artifact = _artifact(selected)
    other = _artifact(unused)
    assert artifact.artifact_id is not None
    assert other.artifact_id is not None
    configured = {artifact.artifact_id: selected, other.artifact_id: unused}
    assert container_external_files((artifact, artifact), configured) == (selected,)
    assert container_input_files((artifact,)) == ()
    with pytest.raises(ValueError, match="admitted artifact binding"):
        container_external_files((artifact,), {artifact.artifact_id: unused})


def test_docker_external_roots_are_readonly_with_private_static_preflight(
    tmp_path: Path,
    oci: PreparedOCIEnvironment,
    docker: DockerRuntime,
) -> None:
    prepared = _installation(tmp_path / "installation")
    sandbox = tmp_path / "session/invocation"
    execute = sandbox / "execute"
    execute.mkdir(parents=True)
    witness = _witness(sandbox)
    environment = PreparedContainerCommandEnvironment(
        oci,
        docker,
        sandbox,
        witness=witness,
        external_files=(prepared,),
    )
    command = ["python", "-c", "print('tool')"]
    argv = environment.wrap_command(command, str(execute))
    assert (
        f"type=bind,source={prepared.root},target={prepared.root},readonly,bind-propagation=rprivate,bind-recursive=readonly"
        in argv
    )
    descriptor = Path(argv[argv.index("--external-access") + 1])
    assert descriptor.parent == sandbox / "external-access"
    assert descriptor.read_bytes() == encode_external_preflight((prepared,))
    assert descriptor.read_bytes().startswith(MAGIC)
    assert argv[argv.index("--external-access") + 2 :] == [
        str(oci.native.prefix / "bin/python"),
        *command[1:],
    ]
    assert environment._managed_container_command(argv) == [
        "docker",
        "create",
        *argv[3:],
    ]
    assert not list(sandbox.rglob("data.bin"))


@pytest.mark.parametrize(
    "collision", ["parent", "scratch", "managed_file", "nested_root", "software"]
)
def test_docker_rejects_external_mount_overlap(
    tmp_path: Path,
    oci: PreparedOCIEnvironment,
    docker: DockerRuntime,
    collision: str,
) -> None:
    sandbox = tmp_path / "session/invocation"
    execute = sandbox / "execute"
    execute.mkdir(parents=True)
    prepared = _installation(tmp_path / "installation")
    inputs = ()
    roots = (prepared,)
    if collision == "parent":
        roots = (prepared.model_copy(update={"root": tmp_path}),)
    elif collision == "scratch":
        roots = (prepared.model_copy(update={"root": sandbox}),)
    elif collision == "managed_file":
        inputs = (prepared.root / "data.bin",)
    elif collision == "nested_root":
        nested = prepared.root / "child"
        nested.mkdir()
        roots = (prepared, prepared.model_copy(update={"root": nested}))
    else:
        native = oci.native.model_copy(
            update={"workspace": prepared.root, "prefix": prepared.root / "prefix"}
        )
        oci = oci.model_copy(update={"native": native})
    environment = PreparedContainerCommandEnvironment(
        oci,
        docker,
        sandbox,
        input_files=inputs,
        witness=_witness(sandbox),
        external_files=roots,
    )
    with pytest.raises(ValueError, match="overlap"):
        environment.wrap_command(["python"], str(execute))


def test_docker_rejects_symlink_installation_root(
    tmp_path: Path,
    oci: PreparedOCIEnvironment,
    docker: DockerRuntime,
) -> None:
    prepared = _installation(tmp_path / "installation")
    alias = tmp_path / "alias"
    alias.symlink_to(prepared.root, target_is_directory=True)
    sandbox = tmp_path / "session/invocation"
    (sandbox / "execute").mkdir(parents=True)
    environment = PreparedContainerCommandEnvironment(
        oci,
        docker,
        sandbox,
        witness=_witness(sandbox),
        external_files=(prepared.model_copy(update={"root": alias}),),
    )
    with pytest.raises(ValueError, match="canonical installation"):
        environment.wrap_command(["python"], str(sandbox / "execute"))


def test_apptainer_external_access_is_explicitly_unsupported(
    tmp_path: Path,
    sif: PreparedSIFEnvironment,
    docker: DockerRuntime,
) -> None:
    prepared = _installation(tmp_path / "installation")
    with pytest.raises(ValueError, match="only by the Docker"):
        PreparedContainerCommandEnvironment(
            sif,
            ApptainerRuntime(docker.executable),
            tmp_path / "invocation",
            external_files=(prepared,),
        )


@pytest.mark.parametrize(
    "root", ["/usr", "/usr/local", "/usr/bin", "/etc", "/dev", "/proc"]
)
def test_docker_rejects_external_roots_that_shadow_image_support(
    tmp_path: Path, oci: PreparedOCIEnvironment, docker: DockerRuntime, root: str
) -> None:
    prepared = _installation(tmp_path / "installation")
    sandbox = tmp_path / "session/invocation"
    (sandbox / "execute").mkdir(parents=True)
    environment = PreparedContainerCommandEnvironment(
        oci,
        docker,
        sandbox,
        witness=_witness(sandbox),
        external_files=(prepared.model_copy(update={"root": Path(root)}),),
    )
    with pytest.raises(ValueError, match="overlap"):
        environment.wrap_command(["python"], str(sandbox / "execute"))


def test_assignment_adapter_includes_captured_context_and_direct_external_inputs(
    tmp_path: Path,
    oci: PreparedOCIEnvironment,
    docker: DockerRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    direct = _installation(tmp_path / "direct")
    context = _installation(tmp_path / "context", b"context")
    unused = _installation(tmp_path / "unused", b"unused")
    artifacts = [_artifact(item) for item in (direct, context, unused)]
    configured = {
        item.artifact_id: prepared
        for item, prepared in zip(artifacts, (direct, context, unused), strict=True)
    }
    bindings = PreparedLocalBindings(
        worker=observe_current_worker(TRANSPORT_PACKAGES),
        tools=(PreparedToolBinding((), oci),),
        pixi=tmp_path / "pixi",
        system_env=Path("/usr/bin/env"),
        launcher=None,
        container_runtime=docker,
        container_launcher=oci.build_runtime,
        external_files=configured,
    )
    route = Mock()
    monkeypatch.setattr(PreparedLocalBindings, "adapter", route)
    assignment = cast(
        Assignment,
        SimpleNamespace(
            inputs={"source": [artifacts[0]]},
            context_artifacts={"captured": artifacts[1]},
        ),
    )
    adapter = bindings.create_adapter(
        assignment, working_root=tmp_path / "calls", runtime_env={}
    )
    operation = Mock()
    adapter(operation, ())
    assert route.call_args.kwargs["external_files"] == (context, direct)
    assert route.call_args.kwargs["input_files"] == ()

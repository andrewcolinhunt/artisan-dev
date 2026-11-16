"""Container launch preserves exact software, explicit mounts and literal values."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pytest

from artisan.execution.environments.command_witness import WitnessInvocation
from artisan.execution.environments.container_launch import (
    PreparedContainerCommandEnvironment,
    container_executable,
    validate_container_bind_path,
)
from artisan.execution.environments.container_runtime import (
    ApptainerRuntime,
    DockerRuntime,
    capture_gpu_driver,
)
from artisan.schemas.execution.command_witness import PreparedCommandWitness
from artisan.schemas.execution.container_environment import (
    PreparedOCIEnvironment,
    PreparedSIFEnvironment,
)


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
        sandbox / "command-runtime",
    )


def test_container_executable_stays_in_trusted_prepared_prefix(
    oci: PreparedOCIEnvironment,
) -> None:
    for name in ("python", "missing-tool"):
        assert container_executable(oci.native, name) == str(
            oci.native.prefix / "bin" / name
        )
    for name in (
        "/usr/bin/python",
        "../../../../usr/bin/python",
        str(oci.native.prefix),
    ):
        with pytest.raises(ValueError, match="inside the prepared"):
            container_executable(oci.native, name)


def test_docker_launch_preserves_literal_values_and_exact_container_owner(
    tmp_path: Path,
    oci: PreparedOCIEnvironment,
    docker: DockerRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sandbox = tmp_path / "session/invocation"
    execute = sandbox / "execute"
    execute.mkdir(parents=True)
    carrier = tmp_path / "carrier.bin"
    carrier.write_bytes(b"input")
    literal = 'literal $value, spaces and "quotes"\nsecond line'
    environment = PreparedContainerCommandEnvironment(
        oci,
        docker,
        sandbox,
        {"PROBE_LITERAL": literal, "PROBE_CREDENTIAL": "synthetic"},
        (carrier,),
        witness=_witness(sandbox),
    )
    monkeypatch.setenv("PYTHONPATH", "/worker-only")
    monkeypatch.setenv("LD_LIBRARY_PATH", "/worker-only/lib")
    argv = environment.wrap_command(["python", "-c", "pass"], str(execute))
    host_env = environment.prepare_env()
    assert "--pull=never" in argv
    assert oci.image_id in argv
    assert f"PROBE_LITERAL={literal}" in argv
    assert "PROBE_CREDENTIAL=synthetic" in argv
    assert argv[argv.index("--prefix") :] == [
        "--prefix",
        str(oci.native.prefix),
        str(oci.native.prefix / "bin/python"),
        "-c",
        "pass",
    ]
    assert "PYTHONPATH" not in host_env
    assert "LD_LIBRARY_PATH" not in host_env
    assert host_env["DOCKER_HOST"] == docker.endpoint
    assert f"type=bind,source={carrier},target={carrier},readonly" in argv
    assert f"type=bind,source={sandbox.parent},target={sandbox.parent},readonly" in argv
    assert f"type=bind,source={execute},target={execute}" in argv
    assert environment._managed_container_command(argv) == [
        "docker",
        "create",
        *argv[3:],
    ]
    assert environment.cancellation_scope == "external"


def test_apptainer_launch_has_explicit_extraction_clean_environment_and_no_engine(
    sif: PreparedSIFEnvironment,
    docker: DockerRuntime,
) -> None:
    runtime = ApptainerRuntime(docker.executable)
    # This profile rejects /dev binds even when pytest uses /dev/shm for scratch.
    with TemporaryDirectory(prefix="artisan-sif-launch-", dir="/tmp") as directory:
        sandbox = Path(directory) / "session/invocation"
        execute = sandbox / "execute"
        execute.mkdir(parents=True)
        environment = PreparedContainerCommandEnvironment(
            sif, runtime, sandbox, witness=_witness(sandbox)
        )
        argv = environment.wrap_command(["python", "-c", "pass"], str(execute))
        assert {"--unsquash", "--containall", "--cleanenv", "--no-eval"} <= set(argv)
        assert str(sif.sif) in argv
        assert argv[argv.index("--prefix") + 1] == str(sif.oci.native.prefix)
        assert environment._managed_container_command(argv) is None
        assert environment.cancellation_scope == "process_group"
        assert str(runtime.executable.parent) not in environment.prepare_env()["PATH"]
    with pytest.raises(ValueError, match="ordinary filesystem"):
        validate_container_bind_path(Path("/dev/shm/scratch"), runtime)


def test_container_rejects_ownership_and_software_injections(
    tmp_path: Path,
    oci: PreparedOCIEnvironment,
    docker: DockerRuntime,
) -> None:
    for key in ("PYTHONPATH", "LD_LIBRARY_PATH", "PATH", "_ARTISAN_COMMAND_SECRET"):
        with pytest.raises(ValueError, match="cannot replace"):
            PreparedContainerCommandEnvironment(
                oci, docker, tmp_path, {key: "untrusted"}
            )


@pytest.mark.parametrize("devices", [None, (), ("0",)])
def test_gpu_allocation_preserves_unset_empty_and_exact_device(
    tmp_path: Path,
    oci: PreparedOCIEnvironment,
    docker: DockerRuntime,
    devices: tuple[str, ...] | None,
) -> None:
    runtime = DockerRuntime(docker.executable, gpu_devices=devices)
    sandbox = tmp_path / "invocation"
    (sandbox / "execute").mkdir(parents=True)
    argv = PreparedContainerCommandEnvironment(
        oci, runtime, sandbox, witness=_witness(sandbox)
    ).wrap_command(["python"], str(sandbox / "execute"))
    assert ("--gpus" in argv) == bool(devices)
    if devices is None:
        assert not any(item.startswith("CUDA_VISIBLE_DEVICES=") for item in argv)
    else:
        assert "CUDA_VISIBLE_DEVICES=" + ("0" if devices else "") in argv


def test_container_commands_require_prefix_checking_witness(
    tmp_path: Path, oci: PreparedOCIEnvironment, docker: DockerRuntime
) -> None:
    environment = PreparedContainerCommandEnvironment(oci, docker, tmp_path)
    with pytest.raises(ValueError, match="require a command witness"):
        environment.wrap_command(["python"], str(tmp_path / "execute"))


def test_gpu_driver_identity_excludes_placement_and_measures_actual_profile() -> None:
    observed = "0, GPU-synthetic, NVIDIA A10, 570.195.03\n"
    with (
        patch("subprocess.check_output", return_value=observed),
        patch(
            "artisan.execution.environments.container_runtime.file_digest",
            return_value="a" * 64,
        ),
    ):
        assert capture_gpu_driver(("0",)) == capture_gpu_driver(("GPU-synthetic",))
        for devices in (("1",), ("all",), ("0", "GPU-synthetic")):
            with pytest.raises(ValueError, match="GPU allocation"):
                capture_gpu_driver(devices)
    with (
        patch(
            "subprocess.check_output",
            return_value=observed.replace("570.195.03", "0.0"),
        ),
        patch(
            "artisan.execution.environments.container_runtime.file_digest",
            return_value="b" * 64,
        ),
    ):
        measured = capture_gpu_driver(("0",))
        assert measured is not None
        assert measured.driver_version == "0.0"
        assert measured.libcuda_sha256 == "b" * 64

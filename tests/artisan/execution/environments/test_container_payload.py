"""Provider image layouts retain one canonical source and final-path installs."""

from __future__ import annotations

import hashlib
import json
import tarfile
from pathlib import Path

import pytest

from artisan.execution.environments.container_source import (
    NATIVE_PENDING_FILENAME,
    ContainerBuildInputs,
    write_container_payload,
    write_container_recipe,
)
from artisan.execution.environments.source import CapturedEnvironment
from artisan.schemas.execution.prepared_environment import (
    NATIVE_BINDING_FILENAME,
    EnvironmentFile,
    EnvironmentSource,
)


def _inputs(context: Path) -> ContainerBuildInputs:
    contents = {"pixi.lock": b"locked packages", "pixi.toml": b"canonical declaration"}
    source = EnvironmentSource(
        manifest_name="pixi.toml",
        name="selected",
        files=tuple(
            EnvironmentFile(path=name, sha256=hashlib.sha256(data).hexdigest())
            for name, data in contents.items()
        ),
    )
    captured = CapturedEnvironment(source, tuple(contents.values()))
    captured.write(context / "workspace")
    return ContainerBuildInputs(captured, (), (), None, "a" * 64)


@pytest.mark.parametrize(
    "location", ["/opt/artisan/worker", "/opt/artisan/tools/first"]
)
def test_payload_installs_at_each_distinct_final_path_without_rewriting_source(
    tmp_path: Path, location: str
) -> None:
    inputs = _inputs(tmp_path)
    commands = write_container_payload(inputs, tmp_path, workspace=Path(location))
    command, publish = commands
    assert command[1:4] == ("install", "--no-config", "--locked")
    assert command[command.index("--manifest-path") + 1] == location + "/pixi.toml"
    assert command[command.index("--environment") + 1] == "selected"
    with tarfile.open(tmp_path / "payload.tar.xz") as archive:
        members = {item.name: item for item in archive.getmembers() if item.isfile()}
        assert set(members) == {
            location.lstrip("/") + "/" + item.path
            for item in inputs.captured.source.files
        } | {location.lstrip("/") + "/" + NATIVE_PENDING_FILENAME}
        stream = archive.extractfile(
            location.lstrip("/") + "/" + NATIVE_PENDING_FILENAME
        )
        assert stream is not None
        completion = json.loads(stream.read())
        assert completion["schema_version"] == 2
        assert "files" not in completion
        assert completion["prefix"] == location + "/.pixi/envs/selected"
        assert publish == (
            "/bin/mv",
            location + "/" + NATIVE_PENDING_FILENAME,
            location + "/" + NATIVE_BINDING_FILENAME,
        )
        for record, data in zip(
            inputs.captured.source.files, inputs.captured.contents, strict=True
        ):
            stream = archive.extractfile(
                members[location.lstrip("/") + "/" + record.path]
            )
            assert stream is not None
            assert stream.read() == data
            assert (tmp_path / "workspace" / record.path).read_bytes() == data


@pytest.mark.parametrize(
    "workspace",
    [
        Path("relative"),
        Path("/opt/../other"),
        Path("/opt/artisan/builder/child"),
        Path("/opt/artisan"),
    ],
)
def test_payload_rejects_ambiguous_or_overlapping_install_locations(
    tmp_path: Path, workspace: Path
) -> None:
    with pytest.raises(ValueError, match="canonical paths|disjoint"):
        write_container_payload(_inputs(tmp_path), tmp_path, workspace=workspace)
    assert not (tmp_path / "payload.tar.xz").exists()


def test_oci_recipe_keeps_existing_final_prefix_and_locked_command(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    digest = write_container_recipe(
        inputs, tmp_path, base_image="example@sha256:" + "b" * 64
    )
    recipe = (tmp_path / "Dockerfile").read_bytes()
    assert hashlib.sha256(recipe).hexdigest() == digest
    commands = [
        json.loads(line.removeprefix("RUN "))
        for line in recipe.decode().splitlines()
        if line.startswith("RUN ")
    ]
    assert commands == [
        [
            "/usr/local/bin/artisan-pixi",
            "install",
            "--no-config",
            "--locked",
            "--manifest-path",
            "/opt/artisan/environment/pixi.toml",
            "--environment",
            "selected",
        ],
        [
            "/bin/mv",
            "/opt/artisan/environment/" + NATIVE_PENDING_FILENAME,
            "/opt/artisan/environment/" + NATIVE_BINDING_FILENAME,
        ],
    ]


def test_payload_uses_nondefault_builder_and_worker_paths_without_copying_prefixes(
    tmp_path: Path,
) -> None:
    from dataclasses import replace

    from artisan.schemas.execution.prepared_environment import (
        DerivedWheel,
        LocalPackageSource,
        PreparedNativeEnvironment,
        SourcePayload,
    )

    inputs = _inputs(tmp_path)
    builder_root = tmp_path / "builder-source"
    builder_root.mkdir()
    for record, data in zip(
        inputs.captured.source.files, inputs.captured.contents, strict=True
    ):
        (builder_root / record.path).write_bytes(data)
    prefix = builder_root / ".pixi/envs/default"
    prefix.mkdir(parents=True)
    (prefix / "installed-only.txt").write_text("must not be copied")
    builder = PreparedNativeEnvironment.model_validate(
        {
            "source": inputs.captured.source.model_copy(update={"name": "default"}),
            "platform": "linux-64",
            "workspace": builder_root,
            "prefix": prefix,
        }
    )
    wheel = DerivedWheel(
        name="fixture",
        version="1",
        source_root=".",
        source_digest="b" * 64,
        builder_digest="c" * 64,
        filename="fixture-1-py3-none-any.whl",
        sha256="d" * 64,
    )
    package = LocalPackageSource(
        root=".",
        name="fixture",
        version="1",
        files=("pixi.toml",),
        payloads=(SourcePayload(path="fixture.py", source="pixi.toml"),),
        scan_roots=("pixi.toml",),
        membership_sha256="e" * 64,
    )
    captured = CapturedEnvironment(
        inputs.captured.source.model_copy(update={"packages": (package,)}),
        inputs.captured.contents,
    )
    commands = write_container_payload(
        replace(inputs, builder=builder, wheels=(wheel,), captured=captured),
        tmp_path,
        workspace=Path("/opt/custom/worker"),
        builder_workspace=Path("/opt/custom/builder"),
    )
    assert commands[0][-2:] == ("--skip", "fixture")
    assert commands[1][-1] == "/opt/custom/builder/pixi.toml"
    assert commands[2][2] == "/opt/custom/builder/.pixi/envs/default/bin/python"
    assert commands[2][commands[2].index("--python") + 1] == (
        "/opt/custom/worker/.pixi/envs/selected/bin/python"
    )
    with tarfile.open(tmp_path / "payload.tar.xz") as archive:
        names = {item.name for item in archive.getmembers() if item.isfile()}
        assert names == {
            f"opt/custom/{kind}/{record.path}"
            for kind in ("worker", "builder")
            for record in inputs.captured.source.files
        } | {"opt/custom/worker/" + NATIVE_PENDING_FILENAME}
        for record, data in zip(
            inputs.captured.source.files, inputs.captured.contents, strict=True
        ):
            stream = archive.extractfile(f"opt/custom/builder/{record.path}")
            assert stream is not None
            assert stream.read() == data


@pytest.mark.parametrize(
    "builder",
    [
        Path("relative"),
        Path("/opt/../builder"),
        Path("/opt/worker/child"),
        Path("/opt"),
    ],
)
def test_payload_rejects_noncanonical_or_overlapping_builder_path(
    tmp_path: Path, builder: Path
) -> None:
    with pytest.raises(ValueError, match="canonical paths|disjoint"):
        write_container_payload(
            _inputs(tmp_path),
            tmp_path,
            workspace=Path("/opt/worker"),
            builder_workspace=builder,
        )
    assert not (tmp_path / "payload.tar.xz").exists()

"""Immutable container metadata binds source evidence without implicit preparation."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from artisan.execution.environments.container_runtime import (
    ApptainerRuntime,
    DockerRuntime,
)
from artisan.execution.environments.container_source import (
    ContainerBuildInputs,
    container_recipe_digest,
)
from artisan.execution.environments.prepared_oci import (
    lookup_oci_environment,
    prepare_oci_environment,
    verify_oci_environment,
)
from artisan.execution.environments.prepared_sif import (
    SIF_CONVERSION_PROFILE,
    prepare_sif_environment,
    verify_sif_environment,
)
from artisan.execution.environments.source import (
    CapturedEnvironment,
    capture_environment_source,
    read_environment_inputs,
)
from artisan.schemas.execution.container_environment import (
    PreparedOCIEnvironment,
    PreparedSIFEnvironment,
)
from artisan.schemas.operation_config.environment import Environment
from artisan.utils.hashing import canonical_json_bytes


def test_oci_requires_immutable_binding_label(
    oci: PreparedOCIEnvironment, docker: DockerRuntime
) -> None:
    observed = {
        "Id": oci.image_id,
        "Os": "linux",
        "Config": {"Labels": {"org.artisan.prepared.payload": oci.payload_digest}},
    }
    with patch.object(DockerRuntime, "inspect_image", return_value=observed):
        verify_oci_environment(oci, docker=docker)
        changed = oci.model_copy(update={"recipe_sha256": "9" * 64})
        with pytest.raises(ValueError, match="identity changed"):
            verify_oci_environment(changed, docker=docker)


def test_sif_checks_completion_and_labels_without_hashing_bytes(
    sif: PreparedSIFEnvironment, docker: DockerRuntime
) -> None:
    runtime = ApptainerRuntime(docker.executable)
    metadata = {
        "data": {
            "attributes": {
                "labels": {"org.artisan.prepared.payload": sif.oci.payload_digest}
            }
        }
    }
    metadata["data"]["attributes"]["labels"].update(
        {
            "org.artisan.oci.source": sif.derivation.source_image_id,
            "org.artisan.oci.config": sif.derivation.source_config_sha256,
            "org.artisan.oci.rootfs": sif.derivation.rootfs_sha256,
        }
    )
    with patch("subprocess.check_output", return_value=json.dumps(metadata)) as inspect:
        verify_sif_environment(sif, apptainer=runtime)
        assert inspect.call_args.args[0][1:3] == ["inspect", "--json"]
        changed = sif.model_copy(
            update={"oci": sif.oci.model_copy(update={"recipe_sha256": "9" * 64})}
        )
        with pytest.raises(ValueError, match="completion record differs"):
            verify_sif_environment(changed, apptainer=runtime)
        changed = sif.model_copy(
            update={
                "oci": sif.oci.model_copy(update={"image_id": "sha256:" + "9" * 64}),
                "derivation": sif.derivation.model_copy(
                    update={"source_image_id": "sha256:" + "9" * 64}
                ),
            }
        )
        with pytest.raises(ValueError, match="completion record differs"):
            verify_sif_environment(changed, apptainer=runtime)
        sif.sif.write_bytes(b"changed image")
        with patch(
            "artisan.execution.environments.prepared_sif.file_digest",
            side_effect=AssertionError("installed SIF bytes read"),
        ):
            verify_sif_environment(sif, apptainer=runtime)


def test_oci_lookup_rejects_changed_source_without_building(
    tmp_path: Path,
    oci: PreparedOCIEnvironment,
    docker: DockerRuntime,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "pixi.toml").write_text(
        '[workspace]\nname="lookup"\nplatforms=["linux-64"]\n'
    )
    (source / "pixi.lock").write_text(
        json.dumps(
            {
                "version": 7,
                "environments": {"default": {"packages": {"linux-64": []}}},
                "packages": [],
            }
        )
    )
    declaration = Environment.from_pixi(source / "pixi.toml")
    captured = capture_environment_source(
        read_environment_inputs(declaration, platform="linux-64")
    )
    target = oci.model_copy(
        update={"native": oci.native.model_copy(update={"source": captured.source})}
    )
    cache = tmp_path / "cache"
    binding = cache / "oci-v2/synthetic/binding.json"
    binding.parent.mkdir(parents=True)
    binding.write_text(target.model_dump_json())
    observed = {
        "Id": target.image_id,
        "Os": "linux",
        "Config": {"Labels": {"org.artisan.prepared.payload": target.payload_digest}},
    }
    with (
        patch.object(DockerRuntime, "inspect_image", return_value=observed),
        patch("subprocess.run", side_effect=AssertionError("provisioning")),
    ):
        assert (
            lookup_oci_environment(declaration, cache_root=cache, docker=docker)
            == target
        )
        (source / "pixi.toml").write_text(
            (source / "pixi.toml").read_text() + "# changed canonical bytes\n"
        )
        with pytest.raises(ValueError, match="prepare it explicitly"):
            lookup_oci_environment(declaration, cache_root=cache, docker=docker)


def test_repeated_oci_prepare_reuses_before_building_packages(
    tmp_path: Path, oci: PreparedOCIEnvironment, docker: DockerRuntime
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "pixi.toml").write_text(
        '[workspace]\nname="reuse"\nplatforms=["linux-64"]\n'
    )
    (source / "pixi.lock").write_text(
        json.dumps(
            {
                "version": 7,
                "environments": {"default": {"packages": {"linux-64": []}}},
                "packages": [],
            }
        )
    )
    declaration = Environment.from_pixi(source / "pixi.toml")
    captured = capture_environment_source(
        read_environment_inputs(declaration, platform="linux-64")
    )
    target = oci.model_copy(
        update={
            "native": oci.native.model_copy(update={"source": captured.source}),
            "pixi_sha256": hashlib.sha256(docker.executable.read_bytes()).hexdigest(),
            "recipe_sha256": container_recipe_digest(
                captured.source,
                (),
                base_image="artisan-environment-base:"
                + oci.base_image_id.removeprefix("sha256:"),
            ),
        }
    )
    binding = tmp_path / "oci-v2/prepared/binding.json"
    binding.parent.mkdir(parents=True)
    binding.write_text(target.model_dump_json())
    base = {
        "Id": target.base_image_id,
        "Os": "linux",
        "Architecture": "amd64",
        "Config": {},
    }
    image = {
        "Id": target.image_id,
        "Os": "linux",
        "Config": {"Labels": {"org.artisan.prepared.payload": target.payload_digest}},
    }
    with (
        patch.object(DockerRuntime, "identity", return_value=target.build_runtime),
        patch.object(
            DockerRuntime,
            "inspect_image",
            side_effect=lambda selected: base if selected == "base" else image,
        ),
        patch("artisan.execution.environments.prepared_oci.pixi_version"),
        patch(
            "artisan.execution.environments.prepared_oci.pixi_executable",
            return_value=str(docker.executable),
        ),
        patch(
            "artisan.execution.environments.prepared_oci.capture_container_inputs",
            side_effect=AssertionError("package build"),
        ),
        patch("subprocess.run", side_effect=AssertionError("image build")),
    ):
        for _ in range(2):
            assert (
                prepare_oci_environment(
                    declaration, cache_root=tmp_path, docker=docker, base_image="base"
                )
                == target
            )


@pytest.mark.parametrize(
    "field", ["base_image_id", "recipe_sha256", "pixi_sha256", "build_runtime"]
)
def test_oci_prepare_cache_hit_must_match_requested_inputs(
    tmp_path: Path,
    oci: PreparedOCIEnvironment,
    docker: DockerRuntime,
    field: str,
) -> None:
    inputs = ContainerBuildInputs(
        CapturedEnvironment(oci.native.source, ()), (), (), None, oci.pixi_sha256
    )
    key = hashlib.sha256(
        canonical_json_bytes(
            {
                "source": oci.native.source.digest,
                "wheels": [],
                "base": oci.base_image_id,
                "recipe": oci.recipe_sha256,
                "pixi": oci.pixi_sha256,
                "runtime": oci.build_runtime.model_dump(mode="json"),
            }
        )
    ).hexdigest()
    changed = oci.model_copy(
        update={
            field: oci.build_runtime.model_copy(update={"version": "other"})
            if field == "build_runtime"
            else ("sha256:" if field == "base_image_id" else "") + "9" * 64
        }
    )
    binding = tmp_path / "oci-v2" / key / "binding.json"
    binding.parent.mkdir(parents=True)
    binding.write_text(changed.model_dump_json())
    base = {
        "Id": oci.base_image_id,
        "Os": "linux",
        "Architecture": "amd64",
        "Config": {},
    }
    with (
        patch.object(DockerRuntime, "identity", return_value=oci.build_runtime),
        patch.object(DockerRuntime, "inspect_image", return_value=base),
        patch(
            "artisan.execution.environments.prepared_oci._reuse_oci_environment",
            return_value=None,
        ),
        patch(
            "artisan.execution.environments.prepared_oci.capture_container_inputs",
            return_value=inputs,
        ),
        patch(
            "artisan.execution.environments.prepared_oci._bind_base",
            return_value="synthetic-base",
        ),
        patch(
            "artisan.execution.environments.prepared_oci.write_container_recipe",
            return_value=oci.recipe_sha256,
        ),
        patch("artisan.execution.environments.prepared_oci.verify_oci_environment"),
        patch("subprocess.run", side_effect=AssertionError("build")),
        pytest.raises(ValueError, match="requested preparation inputs"),
    ):
        prepare_oci_environment(
            Environment.from_pixi(tmp_path / "pixi.toml"),
            cache_root=tmp_path,
            docker=docker,
            base_image="base",
        )


@pytest.mark.parametrize("field", ["oci", "build_runtime", "sif"])
def test_sif_prepare_cache_hit_must_match_requested_inputs(
    tmp_path: Path,
    sif: PreparedSIFEnvironment,
    docker: DockerRuntime,
    field: str,
) -> None:
    runtime = ApptainerRuntime(docker.executable)
    key = hashlib.sha256(
        canonical_json_bytes(
            {
                "oci": sif.oci.digest,
                "builder": sif.build_runtime.model_dump(mode="json"),
                "conversion": SIF_CONVERSION_PROFILE,
            }
        )
    ).hexdigest()
    binding = tmp_path / "sif-v2" / key / "binding.json"
    binding.parent.mkdir(parents=True)
    target = sif.model_copy(update={"sif": binding.parent / "environment.sif"})
    changed = {
        "oci": target.oci.model_copy(update={"recipe_sha256": "9" * 64}),
        "build_runtime": target.build_runtime.model_copy(update={"version": "other"}),
        "sif": tmp_path / "other.sif",
    }[field]
    binding.write_text(target.model_copy(update={field: changed}).model_dump_json())
    with (
        patch.object(ApptainerRuntime, "identity", return_value=sif.build_runtime),
        patch("artisan.execution.environments.prepared_sif.verify_oci_environment"),
        patch("subprocess.run", side_effect=AssertionError("conversion")),
        pytest.raises(ValueError, match="requested source, builder or locator"),
    ):
        prepare_sif_environment(
            sif.oci, cache_root=tmp_path, docker=docker, apptainer=runtime
        )

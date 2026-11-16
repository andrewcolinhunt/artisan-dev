"""Canonical image lookup is pure and rejects changed or uncovered software."""

from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from artisan_modal import image_store, resolution
from artisan_modal.bindings import (
    ImageEnvironment,
    ImageStartup,
    PreparedModalImage,
)
from artisan_modal.image_store import MissingPreparedImage, lookup_image, publish_image
from artisan_modal.preparation import BASE_IMAGE

from artisan.operations.examples.data_transformer import DataTransformer
from artisan.orchestration.runner_api import (
    DerivedWheel,
    Environment,
    EnvironmentSource,
    PreparedNativeEnvironment,
    RunnerResources,
    compile_definition,
)


def test_lookup_is_pure_and_call_count_does_not_require_another_image(
    prepared: tuple[PreparedModalImage, Environment, Environment],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binding, worker, tool = prepared
    publish_image(binding, cache_root=tmp_path)

    def forbidden(*_args: object, **_kwargs: object) -> None:
        message = "lookup attempted a subprocess"
        raise AssertionError(message)

    monkeypatch.setattr(subprocess, "run", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    for tools in ((tool,), (tool, tool), ()):
        assert (
            lookup_image(
                worker, tools, package_names=("cloudpickle",), cache_root=tmp_path
            ).image
            == binding
        )


@pytest.mark.parametrize(
    "change", ["manifest", "lock", "tool", "package", "worker", "base"]
)
def test_lookup_rejects_changed_or_uncovered_requirements(
    prepared: tuple[PreparedModalImage, Environment, Environment],
    tmp_path: Path,
    change: str,
) -> None:
    binding, worker, tool = prepared
    publish_image(binding, cache_root=tmp_path)
    names = ("cloudpickle",)
    base = BASE_IMAGE
    if change in {"manifest", "lock"}:
        path = tmp_path / ("pixi.toml" if change == "manifest" else "pixi.lock")
        path.write_text(path.read_text() + "\n")
    elif change == "tool":
        tool = Environment.from_pixi(tmp_path / "pixi.toml", name="other")
    elif change == "package":
        names = ("uncovered-application",)
    elif change == "worker":
        worker = tool
    else:
        base = "example@sha256:" + "f" * 64
    with pytest.raises(ValueError, match="prepare explicitly"):
        lookup_image(
            worker, (tool,), package_names=names, cache_root=tmp_path, base_image=base
        )


def test_explicit_reprepare_replaces_same_request_binding(
    prepared: tuple[PreparedModalImage, Environment, Environment], tmp_path: Path
) -> None:
    first, worker, tool = prepared
    second = first.model_copy(update={"image_id": "im-reprepared"})
    assert publish_image(first, cache_root=tmp_path) == publish_image(
        second, cache_root=tmp_path
    )
    assert (
        lookup_image(
            worker, (tool,), package_names=("cloudpickle",), cache_root=tmp_path
        ).image
        == second
    )


def test_shared_declarations_resolve_once_then_bind_without_authored_files(
    prepared: tuple[PreparedModalImage, Environment, Environment],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    image, worker, tool = prepared
    publish_image(image, cache_root=tmp_path)
    verify = image_store.verify_container_source
    observed = []

    def capture(declaration: Environment, source: EnvironmentSource) -> None:
        observed.append(declaration)
        verify(declaration, source)

    monkeypatch.setattr(image_store, "verify_container_source", capture)
    selected = lookup_image(
        worker, (worker, tool, tool), package_names=(), cache_root=tmp_path
    )
    assert observed == [worker, tool]
    for name in ("pixi.toml", "pixi.lock"):
        (tmp_path / name).unlink()
    monkeypatch.setattr(
        resolution,
        "select_tool_environments",
        lambda _plan: (
            SimpleNamespace(call_path=("a",), declaration=worker),
            SimpleNamespace(call_path=("b",), declaration=worker),
            SimpleNamespace(call_path=("c",), declaration=tool),
            SimpleNamespace(call_path=("d",), declaration=tool),
        ),
    )
    compatibility = image.startup.worker.compatibility
    assert compatibility is not None
    software = resolution.bind_software(
        compile_definition(DataTransformer),
        selected,
        compatibility,
        RunnerResources(cpus=1, memory_gb=1, gpus=0),
        (),
    )
    targets = tuple(binding.target for binding in software.local.tools)
    assert targets[0] is targets[1] is software.local.worker
    assert targets[2] is targets[3] is selected.targets[tool]
    assert targets[2] is not targets[0]
    assert observed == [worker, tool]
    with pytest.raises(MissingPreparedImage, match="prepare explicitly"):
        lookup_image(worker, (tool,), package_names=(), cache_root=tmp_path)


def test_equivalent_authored_locations_share_one_image_target(
    prepared: tuple[PreparedModalImage, Environment, Environment], tmp_path: Path
) -> None:
    image, worker, tool = prepared
    publish_image(image, cache_root=tmp_path)
    other = tmp_path / "another-operation"
    other.mkdir()
    for name in ("pixi.toml", "pixi.lock"):
        (other / name).write_bytes((tmp_path / name).read_bytes())
    equivalent = Environment.from_pixi(other / "pixi.toml", name=tool.name)
    selected = lookup_image(
        worker, (tool, equivalent), package_names=(), cache_root=tmp_path
    )
    assert selected.targets[tool] is selected.targets[equivalent]


def test_ambiguous_and_corrupt_bindings_are_errors_not_missing_images(
    prepared: tuple[PreparedModalImage, Environment, Environment], tmp_path: Path
) -> None:
    image, worker, tool = prepared
    original = publish_image(image, cache_root=tmp_path)
    duplicate = original.parent.with_name("duplicate") / "binding.json"
    duplicate.parent.mkdir()
    duplicate.write_bytes(original.read_bytes())
    with pytest.raises(ValueError, match="Ambiguous") as ambiguous:
        lookup_image(worker, (tool,), package_names=(), cache_root=tmp_path)
    assert not isinstance(ambiguous.value, MissingPreparedImage)
    duplicate.write_text("not a completed image binding")
    with pytest.raises(ValueError, match="Invalid JSON") as corrupt:
        lookup_image(worker, (tool,), package_names=(), cache_root=tmp_path)
    assert not isinstance(corrupt.value, MissingPreparedImage)


def test_old_binding_namespace_requires_explicit_preparation(
    prepared: tuple[PreparedModalImage, Environment, Environment], tmp_path: Path
) -> None:
    image, worker, tool = prepared
    old = tmp_path / "modal/images/old/binding.json"
    old.parent.mkdir(parents=True)
    old.write_text(image.model_dump_json())
    with pytest.raises(MissingPreparedImage, match="prepare explicitly"):
        lookup_image(worker, (tool,), package_names=(), cache_root=tmp_path)
    assert old.is_file()


def _image_with_builder(
    image: PreparedModalImage, builder_digest: str
) -> PreparedModalImage:
    source = EnvironmentSource.model_validate(
        {
            **image.startup.worker.source.model_dump(),
            "packages": [
                {
                    "root": ".",
                    "name": "example",
                    "version": "1.0",
                    "files": ["pixi.toml"],
                    "payloads": [{"path": "example.py", "source": "pixi.toml"}],
                    "scan_roots": ["pixi.toml"],
                    "membership_sha256": "a" * 64,
                }
            ],
        }
    )
    environment = ImageEnvironment(
        source=source,
        wheels=(
            DerivedWheel(
                name="example",
                version="1.0",
                source_root=".",
                source_digest=source.digest,
                builder_digest=builder_digest,
                filename="example-1.0-py3-none-any.whl",
                sha256="b" * 64,
            ),
        ),
    )
    layout = image.startup.layout.model_copy(
        update={"worker_source": source.digest, "environments": (environment,)}
    )
    startup = ImageStartup(
        layout=layout,
        targets=(
            PreparedNativeEnvironment(
                source=source,
                wheels=environment.wheels,
                platform="linux-64",
                workspace=environment.workspace,
                prefix=environment.prefix,
                compatibility=image.startup.worker.compatibility,
            ),
        ),
        launcher=image.startup.launcher,
        runtime=image.startup.runtime,
        activation_sha256=hashlib.sha256(layout.activation_config()).hexdigest(),
    )
    return image.model_copy(update={"startup": startup})


def test_builder_recipe_changes_require_new_image_without_overwriting_old_target(
    prepared: tuple[PreparedModalImage, Environment, Environment],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    image, worker, _ = prepared
    first = _image_with_builder(image, "c" * 64)
    old_record = publish_image(first, cache_root=tmp_path)
    monkeypatch.setattr(image_store, "verify_container_source", lambda *_args: None)
    monkeypatch.setattr(image_store, "wheel_builder_digest", lambda _platform: "c" * 64)
    assert (
        lookup_image(worker, (), package_names=(), cache_root=tmp_path).image == first
    )

    monkeypatch.setattr(image_store, "wheel_builder_digest", lambda _platform: "d" * 64)
    with pytest.raises(MissingPreparedImage, match="prepare explicitly"):
        lookup_image(worker, (), package_names=(), cache_root=tmp_path)

    second = _image_with_builder(image, "d" * 64)
    assert publish_image(second, cache_root=tmp_path) != old_record
    assert (
        lookup_image(worker, (), package_names=(), cache_root=tmp_path).image == second
    )
    assert PreparedModalImage.model_validate_json(old_record.read_bytes()) == first

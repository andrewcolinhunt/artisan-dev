"""Worker qualification is explicit and read-only resolution never launches Python."""

from __future__ import annotations

from pathlib import Path

import pytest

from artisan.execution.environments.worker_qualification import (
    lookup_worker_qualification,
    prepare_worker_qualification,
)
from artisan.schemas.execution.container_environment import PreparedOCIEnvironment
from artisan.schemas.execution.prepared_environment import PreparedNativeEnvironment
from artisan.schemas.execution.worker_transport import (
    TRANSPORT_PACKAGES,
    DistributionMetadata,
    RuntimeCompatibility,
)


@pytest.fixture
def qualified(oci: PreparedOCIEnvironment) -> PreparedNativeEnvironment:
    compatibility = RuntimeCompatibility(
        python_implementation="CPython",
        python_version="3.12.12",
        cache_tag="cpython-312",
        packages=tuple(
            DistributionMetadata(
                name=name,
                version="1",
            )
            for name in sorted((*TRANSPORT_PACKAGES, "later-application"))
        ),
    )
    return oci.native.model_copy(update={"compatibility": compatibility})


def test_explicit_qualification_publishes_outside_target_then_lookup_is_pure(
    tmp_path: Path,
    qualified: PreparedNativeEnvironment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    measured = []
    verified = []
    names = tuple(sorted(TRANSPORT_PACKAGES))
    worker = qualified.model_copy(update={"compatibility": None})

    def measure(*args: object, **kwargs: object) -> PreparedNativeEnvironment:
        measured.append(args)
        return qualified

    monkeypatch.setattr(
        "artisan.execution.environments.worker_qualification.qualify_local_worker",
        measure,
    )
    monkeypatch.setattr(
        "artisan.execution.environments.worker_qualification.verify_runtime_compatibility",
        lambda *args, **kwargs: verified.append(args),
    )
    cache = tmp_path / "cache"
    assert prepare_worker_qualification(
        worker,
        names,
        cache_root=cache,
        pixi=tmp_path / "pixi",
        system_env=Path("/usr/bin/env"),
    ) == qualified.model_copy(
        update={
            "compatibility": qualified.compatibility.model_copy(
                update={
                    "packages": tuple(
                        p for p in qualified.compatibility.packages if p.name in names
                    )
                }
            )
        }
    )
    assert len(measured) == 1
    assert not worker.workspace.exists()
    monkeypatch.setattr(
        "subprocess.run",
        lambda *_args, **_kwargs: pytest.fail("Lookup launched a process"),
    )
    monkeypatch.setattr(
        "subprocess.Popen",
        lambda *_args, **_kwargs: pytest.fail("Lookup launched a process"),
    )
    monkeypatch.setattr(
        "artisan.execution.environments.worker_qualification.qualify_local_worker",
        lambda *_args, **_kwargs: pytest.fail("Lookup qualified software"),
    )
    assert (
        tuple(
            package.name
            for package in lookup_worker_qualification(
                worker, names, cache_root=cache
            ).compatibility.packages
        )
        == names
    )
    assert prepare_worker_qualification(
        worker,
        names,
        cache_root=cache,
        pixi=tmp_path / "pixi",
        system_env=Path("/usr/bin/env"),
    ) == qualified.model_copy(
        update={
            "compatibility": qualified.compatibility.model_copy(
                update={
                    "packages": tuple(
                        p for p in qualified.compatibility.packages if p.name in names
                    )
                }
            )
        }
    )
    later = lookup_worker_qualification(
        worker, tuple(sorted((*names, "later-application"))), cache_root=cache
    )
    assert later == qualified
    assert len(measured) == 1
    assert len(verified) == 4


def test_missing_or_different_qualification_rejects_without_preparing(
    tmp_path: Path,
    qualified: PreparedNativeEnvironment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = qualified.model_copy(update={"compatibility": None})
    names = tuple(sorted(TRANSPORT_PACKAGES))
    monkeypatch.setattr(
        "artisan.execution.environments.worker_qualification.qualify_local_worker",
        lambda *_args, **_kwargs: qualified,
    )
    monkeypatch.setattr(
        "artisan.execution.environments.worker_qualification.verify_runtime_compatibility",
        lambda *_args, **_kwargs: None,
    )
    with pytest.raises(ValueError, match="prepare explicitly"):
        lookup_worker_qualification(worker, names, cache_root=tmp_path)
    prepare_worker_qualification(
        worker,
        names,
        cache_root=tmp_path,
        pixi=tmp_path / "pixi",
        system_env=Path("/usr/bin/env"),
    )
    path = next((tmp_path / "worker-qualification-v2").rglob("*.json"))
    workspace = tmp_path / "another-prepared-target"
    altered = qualified.model_copy(
        update={"workspace": workspace, "prefix": workspace / ".pixi/envs/default"}
    )
    path.write_text(altered.model_dump_json())
    with pytest.raises(ValueError, match="differs"):
        lookup_worker_qualification(worker, names, cache_root=tmp_path)


def test_callback_qualification_checks_actual_target_packages_without_controller_versions(
    tmp_path: Path,
    qualified: PreparedNativeEnvironment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = qualified.model_copy(update={"compatibility": None})
    monkeypatch.setattr(
        "artisan.execution.environments.worker_qualification.qualify_local_worker",
        lambda *args, **kwargs: qualified,
    )
    monkeypatch.setattr(
        "artisan.execution.environments.worker_qualification.verify_runtime_compatibility",
        lambda *args, **kwargs: pytest.fail(
            "Compared callback environment with controller"
        ),
    )
    prepare_worker_qualification(
        worker,
        TRANSPORT_PACKAGES,
        cache_root=tmp_path,
        pixi=tmp_path / "pixi",
        system_env=Path("/usr/bin/env"),
        verify_current=False,
    )
    monkeypatch.setattr(
        "artisan.execution.environments.worker_qualification.qualify_local_worker",
        lambda *args, **kwargs: pytest.fail("Submission launched qualification"),
    )
    with pytest.raises(ValueError, match="missing|cover|unavailable"):
        lookup_worker_qualification(
            worker,
            (*TRANSPORT_PACKAGES, "missing-application"),
            cache_root=tmp_path,
            verify_current=False,
        )


def test_callback_caller_collects_framework_and_selected_package_requirements(
    tmp_path: Path,
    qualified: PreparedNativeEnvironment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from artisan.composites.base.compiler import compile_definition
    from artisan.execution import CalledOperation
    from artisan.execution.environments.configured_calls import resolve_callback_caller
    from artisan.execution.worker_requirements import observed_controller_sources
    from artisan.operations.base import OperationDefinition
    from artisan.operations.examples import DataGenerator
    from artisan.schemas import Environment

    plan = compile_definition(
        DataGenerator, called_operations={"generate": CalledOperation(DataGenerator)}
    )
    names, sources = observed_controller_sources(plan, (OperationDefinition,))
    monkeypatch.setattr(
        "artisan.execution.environments.worker_qualification.qualified_controller_sources",
        lambda *args: sources,
    )
    selected = []

    def lookup(
        target: PreparedNativeEnvironment, required: tuple[str, ...], **kwargs: object
    ) -> PreparedNativeEnvironment:
        selected.append(required)
        return target

    monkeypatch.setattr(
        "artisan.execution.environments.configured_calls.lookup_worker_qualification",
        lookup,
    )
    resolve_callback_caller(
        plan,
        Environment.from_pixi(tmp_path / "pixi.toml"),
        qualified,
        cache_root=tmp_path,
        application_modules=(),
    )
    assert selected == [names]

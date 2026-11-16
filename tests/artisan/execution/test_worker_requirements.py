"""Transport coverage follows imported code, including nested application schemas."""

from __future__ import annotations

import importlib
import importlib.util
import json
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from unittest.mock import patch

import pytest

from artisan.composites.base.compiler import compile_definition
from artisan.execution.environments.source import (
    capture_environment_source,
    read_environment_inputs,
)
from artisan.execution.environments.source_membership import source_membership
from artisan.execution.runtime_compatibility import capture_runtime_compatibility
from artisan.execution.worker_bootstrap import (
    execute_worker_call,
    serialize_worker_call,
)
from artisan.execution.worker_requirements import (
    collect_definition_packages,
    collect_worker_packages,
    observed_definition_sources,
    qualified_controller_sources,
)
from artisan.operations.curator.merge import Merge
from artisan.operations.examples import DataGenerator
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.execution.prepared_environment import (
    DerivedWheel,
    LocalPackageSource,
    PreparedNativeEnvironment,
    SourcePayload,
)
from artisan.schemas.execution.worker_transport import TRANSPORT_PACKAGES
from artisan.schemas.operation_config.environment import Environment


@pytest.fixture
def core_sources() -> dict[str, dict[str, Path]]:
    import artisan

    root = Path(artisan.__file__).parent.parent
    return {
        "dexterity-artisan": {
            path.relative_to(root).as_posix(): path
            for path in (root / "artisan").rglob("*.py")
            if "__pycache__" not in path.parts
        }
    }


@pytest.fixture
def install_package(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Callable[[str, str], ModuleType]:
    monkeypatch.syspath_prepend(str(tmp_path))

    def install(name: str, source: str) -> ModuleType:
        root = tmp_path / name
        root.mkdir()
        (root / "__init__.py").write_text(source)
        metadata = tmp_path / f"{name}-1.0.dist-info"
        metadata.mkdir()
        (metadata / "METADATA").write_text(f"Name: {name}\nVersion: 1.0\n")
        (metadata / "RECORD").write_text(f"{name}/__init__.py,,\n")
        importlib.invalidate_caches()
        spec = importlib.util.spec_from_file_location(name, root / "__init__.py")
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, name, module)
        spec.loader.exec_module(module)
        return module

    return install


def test_collects_nested_definitions_parameter_schema_and_provider_without_imports(
    install_package: Callable[[str, str], ModuleType],
    core_sources: dict[str, dict[str, Path]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_package(
        "coverage_schema",
        "from enum import StrEnum\nclass Kind(StrEnum):\n    first='first'\n",
    )
    install_package(
        "coverage_child",
        """
from pydantic import Field
from artisan.operations.examples import DataGenerator
from coverage_schema import Kind
class Generate(DataGenerator):
    name='coverage_generate'
    class Params(DataGenerator.Params):
        kind: Kind=Field(default=Kind.first, description='Synthetic choice')
    params: Params=Params()
""",
    )
    composite = install_package(
        "coverage_composite",
        """
from enum import StrEnum
from typing import ClassVar
from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.schemas.specs.output_spec import OutputSpec
from coverage_child import Generate
class Workflow(CompositeDefinition):
    name='coverage_workflow'
    class OutputRole(StrEnum):
        datasets='datasets'
    outputs: ClassVar={'datasets': OutputSpec(artifact_type='csv')}
    def compose(self, ctx):
        generated=ctx.call(Generate)
        ctx.output('datasets', generated.output('datasets'))
""",
    )
    provider = install_package("coverage_provider", "class Provider: pass\n")
    plan = compile_definition(composite.Workflow)
    monkeypatch.setattr(
        importlib, "import_module", lambda *args: pytest.fail("Collector imported code")
    )
    packages = collect_worker_packages(
        plan, (provider.Provider,), editable_sources=core_sources
    )
    assert set(TRANSPORT_PACKAGES) <= set(packages)
    assert {
        "coverage-schema",
        "coverage-child",
        "coverage-composite",
        "coverage-provider",
    } <= set(packages)
    assert tuple(sorted(set(packages))) == packages


def test_rejects_uninstalled_definition(
    core_sources: dict[str, dict[str, Path]],
) -> None:
    class Unpackaged(DataGenerator):
        name = "unpackaged"

    with pytest.raises(ValueError, match="importable packaged"):
        collect_worker_packages(
            compile_definition(Unpackaged),
            (DataGenerator,),
            editable_sources=core_sources,
        )


def test_curator_collection_uses_live_params_schema_and_provider_without_a_plan(
    install_package: Callable[[str, str], ModuleType],
    core_sources: dict[str, dict[str, Path]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_package(
        "curator_schema",
        "from enum import StrEnum\nclass Kind(StrEnum):\n    first='first'\n",
    )
    curator = install_package(
        "curator_application",
        """
from pydantic import BaseModel, Field
from artisan.operations.curator.merge import Merge
from curator_schema import Kind
class Select(Merge):
    name='qualified_select'
    class Params(BaseModel):
        kind: Kind=Field(default=Kind.first, description='Synthetic selection')
    params: Params=Params()
""",
    )
    provider = install_package("curator_provider", "class Provider: pass\n")
    monkeypatch.setattr(
        importlib, "import_module", lambda *args: pytest.fail("Collector imported code")
    )
    monkeypatch.setattr(
        "artisan.execution.worker_requirements.observed_editable_sources",
        lambda _names: core_sources,
    )
    names = collect_definition_packages(
        (curator.Select,), (provider.Provider,), editable_sources=core_sources
    )
    assert {"curator-application", "curator-schema", "curator-provider"} <= set(names)
    observed, sources = observed_definition_sources(
        (curator.Select,), (provider.Provider,)
    )
    assert observed == names
    assert sources == core_sources


def test_curator_collection_rejects_unimportable_definition(
    core_sources: dict[str, dict[str, Path]],
) -> None:
    class Unpackaged(Merge):
        name = "unpackaged_curator"

    with pytest.raises(ValueError, match="importable packaged"):
        collect_definition_packages(
            (Unpackaged,), (Merge,), editable_sources=core_sources
        )


@pytest.mark.parametrize("curator", [False, True])
def test_provider_and_sdk_are_both_selected_from_trusted_imported_roots(
    install_package: Callable[[str, str], ModuleType],
    core_sources: dict[str, dict[str, Path]],
    monkeypatch: pytest.MonkeyPatch,
    curator: bool,
) -> None:
    provider = install_package("transport_provider", "class Provider: pass\n")
    sdk = install_package("transport_sdk", "class Executor: pass\n")
    roots = (provider.Provider, sdk.Executor)
    monkeypatch.setattr(
        importlib, "import_module", lambda *a: pytest.fail("Imported recorded code")
    )
    packages = (
        collect_definition_packages((Merge,), roots, editable_sources=core_sources)
        if curator
        else collect_worker_packages(
            compile_definition(DataGenerator), roots, editable_sources=core_sources
        )
    )
    assert {"transport-provider", "transport-sdk"} <= set(packages)
    monkeypatch.setattr(sdk, "__file__", "/unrelated/shadow.py")
    with pytest.raises(ValueError, match="unique qualified distribution"):
        collect_definition_packages((Merge,), roots, editable_sources=core_sources)


def test_provider_roots_require_imported_code_and_nonempty_coverage(
    core_sources: dict[str, dict[str, Path]],
) -> None:
    class UnpackagedSDK:
        pass

    for roots in ((), (Merge, UnpackagedSDK)):
        with pytest.raises(ValueError, match="provider code roots|importable packaged"):
            collect_definition_packages((Merge,), roots, editable_sources=core_sources)


@pytest.mark.parametrize("changed_root", ["provider", "sdk"])
def test_provider_and_sdk_version_mismatch_reject_before_unpickling(
    install_package: Callable[[str, str], ModuleType],
    core_sources: dict[str, dict[str, Path]],
    monkeypatch: pytest.MonkeyPatch,
    changed_root: str,
) -> None:
    provider = install_package("bound_provider", "class Provider: pass\n")
    sdk = install_package("bound_sdk", "class Executor: pass\n")
    names = collect_definition_packages(
        (Merge,), (provider.Provider, sdk.Executor), editable_sources=core_sources
    )
    compatibility = capture_runtime_compatibility(names)
    call = serialize_worker_call(str, compatibility=compatibility, args=(1,))
    selected = provider if changed_root == "provider" else sdk
    assert selected.__file__ is not None
    package_name = "bound-" + changed_root
    metadata = (
        Path(selected.__file__).parent.parent
        / f"bound_{changed_root}-1.0.dist-info/METADATA"
    )
    metadata.write_text(f"Name: {package_name}\nVersion: 2.0\n")
    monkeypatch.setattr(
        "cloudpickle.loads",
        lambda *a, **kw: pytest.fail("Loaded changed provider code"),
    )
    with pytest.raises(ValueError, match=f"bound-{changed_root}.*differs"):
        execute_worker_call(call)


def test_rejects_loaded_module_shadowing_registered_distribution(
    install_package: Callable[[str, str], ModuleType],
    core_sources: dict[str, dict[str, Path]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = install_package("coverage_shadow", "class Provider: pass\n")
    unrelated = tmp_path / "shadow.py"
    unrelated.write_text("class Provider: pass\n")
    monkeypatch.setattr(provider, "__file__", str(unrelated))
    with pytest.raises(ValueError, match="unique qualified distribution"):
        collect_worker_packages(
            compile_definition(DataGenerator),
            (provider.Provider,),
            editable_sources=core_sources,
        )


def test_rejects_editable_without_qualified_source_selection() -> None:
    with pytest.raises(ValueError, match="unique qualified distribution"):
        collect_worker_packages(compile_definition(DataGenerator), (DataGenerator,))


@pytest.mark.parametrize("observed", [False, True])
def test_plain_literal_editable_does_not_require_top_level_metadata(
    install_package: Callable[[str, str], ModuleType],
    core_sources: dict[str, dict[str, Path]],
    tmp_path: Path,
    observed: bool,
) -> None:
    provider = install_package("literal_provider", "class Provider: pass\n")
    metadata = tmp_path / "literal_provider-1.0.dist-info"
    pointer = tmp_path / "_literal_provider.pth"
    pointer.write_text(str(tmp_path) + "\n")
    (metadata / "RECORD").write_text(f"{pointer.name},,\n")
    (metadata / "direct_url.json").write_text(
        json.dumps({"url": tmp_path.as_uri(), "dir_info": {"editable": True}})
    )
    importlib.invalidate_caches()
    assert "literal_provider" not in importlib.metadata.packages_distributions()
    if observed:
        names, sources = observed_definition_sources((Merge,), (provider.Provider,))
    else:
        sources = {
            **core_sources,
            "literal-provider": {
                "literal_provider/__init__.py": tmp_path
                / "literal_provider/__init__.py"
            },
        }
        names = collect_definition_packages(
            (Merge,), (provider.Provider,), editable_sources=sources
        )
    assert "literal-provider" in names
    assert sources["literal-provider"] == {
        "literal_provider/__init__.py": tmp_path / "literal_provider/__init__.py"
    }
    if observed:
        duplicate = tmp_path / "second_owner-1.0.dist-info"
        duplicate.mkdir()
        (duplicate / "METADATA").write_text("Name: second-owner\nVersion: 1.0\n")
        (duplicate / "RECORD").write_text(f"{pointer.name},,\n")
        (duplicate / "direct_url.json").write_bytes(
            (metadata / "direct_url.json").read_bytes()
        )
        importlib.invalidate_caches()
        with pytest.raises(ValueError, match="unique qualified distribution"):
            observed_definition_sources((Merge,), (provider.Provider,))


def test_numeric_parameter_change_preserves_collected_packages(
    core_sources: dict[str, dict[str, Path]],
) -> None:
    first = collect_worker_packages(
        compile_definition(DataGenerator, params={"count": 1}),
        (DataGenerator,),
        editable_sources=core_sources,
    )
    changed = collect_worker_packages(
        compile_definition(DataGenerator, params={"count": 3}),
        (DataGenerator,),
        editable_sources=core_sources,
    )
    assert first == changed


def test_collects_registered_schema_and_separately_packaged_reference_policy(
    install_package: Callable[[str, str], ModuleType],
    core_sources: dict[str, dict[str, Path]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_package("coverage_policy", "def references(artifact, context): return ()\n")
    schema = install_package(
        "coverage_artifact",
        "from artisan.schemas.artifact.csv import CSVTypeDef\n"
        "from coverage_policy import references\n"
        "class ExternalCSV(CSVTypeDef):\n"
        "    required_references=staticmethod(references)\n",
    )
    monkeypatch.setitem(ArtifactTypeDef._registry, "csv", schema.ExternalCSV)
    packages = collect_worker_packages(
        compile_definition(DataGenerator),
        (DataGenerator,),
        editable_sources=core_sources,
    )
    assert {"coverage-artifact", "coverage-policy"} <= set(packages)


def test_rejects_ambiguous_distribution_ownership(
    install_package: Callable[[str, str], ModuleType],
    core_sources: dict[str, dict[str, Path]],
    tmp_path: Path,
) -> None:
    provider = install_package("coverage_shared", "class Provider: pass\n")
    metadata = tmp_path / "other_owner-1.0.dist-info"
    metadata.mkdir()
    (metadata / "METADATA").write_text("Name: other_owner\nVersion: 1.0\n")
    (metadata / "RECORD").write_text("coverage_shared/__init__.py,,\n")
    importlib.invalidate_caches()
    with pytest.raises(ValueError, match="unique qualified distribution"):
        collect_worker_packages(
            compile_definition(DataGenerator),
            (provider.Provider,),
            editable_sources=core_sources,
        )


def test_controller_editable_mapping_uses_already_resolved_source(
    install_package: Callable[[str, str], ModuleType],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_package("coverage_editable", "class Provider: pass\n")
    metadata = tmp_path / "coverage_editable-1.0.dist-info/direct_url.json"
    metadata.write_text(json.dumps({"dir_info": {"editable": True}}))
    manifest = tmp_path / "pyproject.toml"
    manifest.write_text(
        '[project]\nname="coverage-editable"\nversion="1.0"\n'
        '[build-system]\nrequires=["hatchling==1.31.0"]\nbuild-backend="hatchling.build"\n'
        '[tool.hatch.build.targets.wheel]\npackages=["coverage_editable"]\n'
        '[tool.pixi.workspace]\nname="coverage"\nplatforms=["linux-64"]\n'
    )
    (tmp_path / "pixi.lock").write_text(
        json.dumps(
            {
                "version": 7,
                "environments": {
                    "default": {"packages": {"linux-64": [{"pypi": "./"}]}}
                },
                "packages": [
                    {"pypi": "./", "name": "coverage-editable", "version": "1.0"}
                ],
            }
        )
    )
    declaration = Environment.from_pixi(manifest)
    package = LocalPackageSource(
        root=".",
        name="coverage-editable",
        version="1.0",
        files=("coverage_editable/__init__.py", "pyproject.toml"),
        payloads=(
            SourcePayload(
                path="coverage_editable/__init__.py",
                source="coverage_editable/__init__.py",
            ),
        ),
        scan_roots=("coverage_editable",),
        membership_sha256=source_membership(tmp_path, ("coverage_editable",)),
    )
    source = capture_environment_source(
        read_environment_inputs(declaration, platform="linux-64"), (package,)
    ).source
    target = PreparedNativeEnvironment(
        source=source,
        platform="linux-64",
        workspace=tmp_path / "prepared",
        prefix=tmp_path / "prepared/.pixi/envs/default",
        wheels=(
            DerivedWheel(
                name=package.name,
                version="1.0",
                source_root=".",
                source_digest="b" * 64,
                builder_digest="c" * 64,
                filename="coverage_editable-1.0-py3-none-any.whl",
                sha256="d" * 64,
            ),
        ),
    )
    monkeypatch.setattr(
        "subprocess.run",
        lambda *args, **kwargs: pytest.fail(
            "Controller qualification spawned a process"
        ),
    )
    monkeypatch.setattr(
        "subprocess.Popen",
        lambda *args, **kwargs: pytest.fail(
            "Controller qualification spawned a process"
        ),
    )
    assert qualified_controller_sources(declaration, target) == {
        "coverage-editable": {
            "coverage_editable/__init__.py": tmp_path / "coverage_editable/__init__.py"
        }
    }
    metadata.write_text("{}")
    assert qualified_controller_sources(declaration, target) == {}
    with patch(
        "artisan.execution.worker_requirements.importlib.metadata.distribution",
        side_effect=importlib.metadata.PackageNotFoundError("coverage-editable"),
    ):
        assert qualified_controller_sources(declaration, target) == {}
    metadata.write_text(json.dumps({"dir_info": {"editable": True}}))
    (tmp_path / "coverage_editable/__init__.py").write_text("changed = True\n")
    monkeypatch.setattr(
        Path, "read_bytes", lambda *_: pytest.fail("Resolved source was recaptured")
    )
    assert qualified_controller_sources(declaration, target) == {
        "coverage-editable": {
            "coverage_editable/__init__.py": tmp_path / "coverage_editable/__init__.py"
        }
    }


def test_application_initializer_is_qualified_even_without_registered_classes(
    install_package: Callable[[str, str], ModuleType],
    core_sources: dict[str, dict[str, Path]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    application = install_package("initializer_only", "SETTING = 1\n")
    packages = collect_worker_packages(
        compile_definition(DataGenerator),
        (DataGenerator,),
        editable_sources=core_sources,
        application_modules=("initializer_only",),
    )
    assert "initializer-only" in packages
    monkeypatch.delitem(sys.modules, application.__name__)
    monkeypatch.setattr(
        importlib, "import_module", lambda *args: pytest.fail("Imported an application")
    )
    with pytest.raises(ValueError, match="imported installed package"):
        collect_worker_packages(
            compile_definition(DataGenerator),
            (DataGenerator,),
            editable_sources=core_sources,
            application_modules=("initializer_only",),
        )

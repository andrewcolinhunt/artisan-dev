"""Explicit application initialization precedes custom row hydration in fresh workers."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from textwrap import dedent

import pytest

from artisan.execution.application_bootstrap import (
    validate_worker_applications,
    worker_application_scope,
)

MODEL_SOURCE = """
from dataclasses import dataclass
from enum import StrEnum
from typing import ClassVar
from pydantic import Field
from artisan.operations.base import OperationDefinition
from artisan.schemas import ArtifactResult, InputSpec, MetricArtifact, OutputSpec
from artisan.execution.compute.local import LocalExecuteRouter
from artisan.execution.runtime_compatibility import verify_runtime_compatibility
from artisan.execution.environments.observed_sources import observed_editable_sources

class Note(MetricArtifact):
    artifact_type: str = Field(default="bootstrap_note", frozen=True)

class CopyNote(OperationDefinition):
    name = "bootstrap_copy_note"
    class InputRole(StrEnum):
        notes = "notes"
    class OutputRole(StrEnum):
        notes = "notes"
    inputs: ClassVar = {"notes": InputSpec(artifact_type="any", materialize=False)}
    outputs: ClassVar = {"notes": OutputSpec(artifact_type="any", derives_from={"inputs": ["notes"]})}
    per_artifact_dispatch = False
    def preprocess(self, inputs):
        import os
        from pathlib import Path
        if marker := os.environ.get("OPERATION_ENTRY_MARKER"):
            Path(marker).write_text("entered")
        assert isinstance(inputs.input_artifacts["notes"][0], Note)
        return {}
    def execute_function(self, inputs):
        return {}
    def postprocess(self, inputs):
        result = ArtifactResult()
        result.add_artifact("notes", Note.draft({"value": 2}, "copied"), sources={"notes": [inputs.ref("notes", index=0)]})
        return result

@dataclass(frozen=True)
class Software:
    compatibility: object
    application_modules: tuple[str, ...]
    software_digest: str = "c" * 64
    software_cacheable: bool = False
    def verify_worker(self):
        verify_runtime_compatibility(self.compatibility)
    def verify(self, plan):
        self.verify_worker()
        plan.instantiate()
    def create_adapter(self, assignment, *, working_root, runtime_env):
        return lambda operation, path: LocalExecuteRouter()
"""
APPLICATION_SOURCE = """
import os
from pathlib import Path
from artisan.schemas import ArtifactTypeDef
from bootstrap_models import Note
if marker := os.environ.get("APPLICATION_IMPORT_MARKER"):
    Path(marker).write_text("imported")
class NoteType(ArtifactTypeDef):
    key = "bootstrap_note"
    table_path = "artifacts/bootstrap_notes"
    model = Note
    required_references = None
"""


def test_resolved_application_selection_needs_no_controller_source_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "artisan.execution.application_bootstrap.observed_editable_sources",
        lambda *_args: pytest.fail("Application selection reopened controller sources"),
    )
    modules = ("selected_application",)
    assert validate_worker_applications(modules, qualified_modules=modules) == modules
    with pytest.raises(ValueError, match="qualified software selection"):
        validate_worker_applications(modules, qualified_modules=())
    with (
        worker_application_scope(modules),
        pytest.raises(ValueError, match="live call selection"),
    ):
        validate_worker_applications((), qualified_modules=())


@pytest.fixture
def site(tmp_path: Path) -> Path:
    directory = tmp_path / "site"
    directory.mkdir()
    for name, source in (
        ("bootstrap_models", MODEL_SOURCE),
        ("bootstrap_application", APPLICATION_SOURCE),
    ):
        package = directory / name
        package.mkdir()
        (package / "__init__.py").write_text(dedent(source))
        metadata = directory / f"{name}-1.0.dist-info"
        metadata.mkdir()
        (metadata / "METADATA").write_text(f"Name: {name}\nVersion: 1.0\n")
        (metadata / "RECORD").write_text(f"{name}/__init__.py,,\n")
    return directory


def _run(
    source: str, site: Path, root: Path, *args: str
) -> subprocess.CompletedProcess[str]:
    tests = Path(__file__).resolve().parents[2]
    setup = "import sys; sys.path[:0] = sys.argv[1:3]\n"
    return subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            setup + dedent(source),
            str(site),
            str(tests),
            str(root),
            *args,
        ],
        check=False,
        env={
            **os.environ,
            "PYTHONDONTWRITEBYTECODE": "1",
            "POLARS_MAX_THREADS": "2",
            "RAYON_NUM_THREADS": "2",
            "ARTISAN_LOAD_MODULES": "ambient_untrusted_application",
        },
        text=True,
        capture_output=True,
        timeout=90,
    )


PREPARE = """
import json
from pathlib import Path
from artisan.registry import bootstrap_applications
from artisan.composites.base.compiler import compile_definition
from artisan.execution.admitted_worker import execute_admitted_assignment
from artisan.execution.assignment_transport import AdmittedAssignment, AssignmentDispatch
from artisan.execution.curator_worker import AdmittedCurator, execute_admitted_curator
from artisan.execution.environments.observed_sources import observed_editable_sources
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.execution.runtime_compatibility import capture_runtime_compatibility
from artisan.execution.worker_bootstrap import serialize_worker_call
from artisan.execution.worker_requirements import collect_worker_packages
from artisan.schemas.execution.assignment import AssignmentAttempt, AssignmentIdentity
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.storage.io.admission import publish_admission
from artisan.storage.core.retained_reader import RetainedReader
from artisan.operations.curator import Merge
from fixtures.retained_acceptance import open_committer
from fixtures.retained_curators import admit_curator
from fsspec.implementations.local import LocalFileSystem
bootstrap_applications(("bootstrap_application",))
from bootstrap_models import CopyNote, Note, Software
root = Path(sys.argv[3])
kind = sys.argv[4]
plan = compile_definition(CopyNote)
sources = observed_editable_sources(("dexterity-artisan",))
names = collect_worker_packages(plan, (Software,), editable_sources=sources, application_modules=("bootstrap_application",))
assert "bootstrap-application" in names
compatibility = capture_runtime_compatibility(names)
software = Software(compatibility, ("bootstrap_application",))
committer = open_committer(str(root / "store"), LocalFileSystem(), StorageConfig(), initialize=True)
graph = OccurrenceGraph(boundary_invocation=AssignmentIdentity(step_run_id="a" * 32, unit_index=0).assignment_id)
graph.add_input("notes", Note.draft({"value": 1}, "original").finalize())
boundary = graph.project({}, {})
attempt = AssignmentAttempt(assignment=AssignmentIdentity(step_run_id="a" * 32, unit_index=0), attempt=0, generation=0, recipe_digest=plan.recipe.digest, software_digest=software.software_digest, software_cacheable=False, input_context_digest=boundary.input_context_digest)
reference = publish_admission(attempt, plan.recipe, boundary, {address: graph.nodes[address].artifact for address in boundary.boundary_addresses()}, step_number=0, delta_root=committer.delta_root, files_root=committer.files_root, fs=committer.fs)
committer.journal.admit(attempt, admission_digest=reference.record_digest)
dispatch = AssignmentDispatch(committer.delta_root, committer.files_root, str(root / "staging"), StorageConfig(), root / "worker", application_modules=("bootstrap_application",))
admitted = AdmittedAssignment(plan, reference)
function = execute_admitted_assignment
if kind == "curator":
    receipt = execute_admitted_assignment(admitted, software, dispatch, working_root=dispatch.working_root)
    committer.accept(receipt)
    result = RetainedReader(committer).read(receipt)
    coordinator, admission, reference = admit_curator(committer, Merge(), {"notes": result.output("notes")}, step_run_id="b" * 32, software_digest=software.software_digest, software_cacheable=False)
    from dataclasses import replace
    dispatch = replace(dispatch, staging_root=coordinator.staging.staging_dir)
    admitted = AdmittedCurator(Merge(), reference)
    function = execute_admitted_curator
call = serialize_worker_call(function, compatibility=compatibility, args=(admitted, software, dispatch), kwargs={"working_root": dispatch.working_root}, application_modules=dispatch.application_modules)
(root / "header.json").write_text(call.header.model_dump_json())
(root / "payload.bin").write_bytes(call.payload)
"""

EXECUTE = """
import os
from pathlib import Path
from artisan.execution.worker_bootstrap import PreparedProcessCall, execute_worker_call
from artisan.execution.environments.observed_sources import observed_editable_sources
from artisan.schemas.execution.worker_transport import WorkerTransportHeader
from artisan.schemas.artifact.registry import ArtifactTypeDef
root = Path(sys.argv[3])
assert "bootstrap_application" not in sys.modules
assert "bootstrap_note" not in ArtifactTypeDef.get_all()
os.environ["APPLICATION_IMPORT_MARKER"] = str(root / "imported")
os.environ["OPERATION_ENTRY_MARKER"] = str(root / "science")
header = WorkerTransportHeader.model_validate_json((root / "header.json").read_text())
call = PreparedProcessCall(header, (root / "payload.bin").read_bytes())
result = execute_worker_call(call)
assert ArtifactTypeDef.get("bootstrap_note").key == "bootstrap_note"
assert "ambient_untrusted_application" not in sys.modules
assert (root / "imported").read_text() == "imported"
if hasattr(result, "success"):
    assert result.success, result.error
else:
    assert (root / "science").read_text() == "entered"
print("qualified bootstrap and custom row succeeded")
"""


@pytest.mark.parametrize("kind", ["creator", "curator"])
def test_fresh_worker_registers_separate_initializer_before_custom_rows(
    tmp_path: Path, site: Path, kind: str
) -> None:
    prepared = _run(PREPARE, site, tmp_path, kind)
    assert prepared.returncode == 0, prepared.stdout + prepared.stderr
    completed = _run(EXECUTE, site, tmp_path)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "custom row succeeded" in completed.stdout


@pytest.mark.parametrize("top_level_metadata", [False, True])
def test_fresh_worker_bootstraps_literal_editables_with_normalized_owners(
    tmp_path: Path, site: Path, top_level_metadata: bool
) -> None:
    for name in ("bootstrap_models", "bootstrap_application"):
        project = tmp_path / f"{name}_project"
        imports = project / "src"
        imports.mkdir(parents=True)
        (site / name).rename(imports / name)
        metadata = site / f"{name}-1.0.dist-info"
        (metadata / "METADATA").write_text(f"Name: {name.title()}\nVersion: 1.0\n")
        pointer = site / f"_{name}.pth"
        pointer.write_text(str(imports) + "\n")
        (metadata / "RECORD").write_text(f"{pointer.name},,\n")
        (metadata / "direct_url.json").write_text(
            json.dumps({"url": project.as_uri(), "dir_info": {"editable": True}})
        )
        if top_level_metadata:
            (metadata / "top_level.txt").write_text(name + "\n")
    activate = "import site as installed_site; installed_site.addsitedir(sys.argv[1])\n"
    preparation = PREPARE.replace(
        'names = collect_worker_packages(plan, (Software,), editable_sources=sources, application_modules=("bootstrap_application",))',
        "from artisan.execution.worker_requirements import observed_controller_sources\n"
        'names, sources = observed_controller_sources(plan, (Software,), application_modules=("bootstrap_application",))',
    )
    prepared = _run(activate + preparation, site, tmp_path, "creator")
    assert prepared.returncode == 0, prepared.stdout + prepared.stderr
    completed = _run(activate + EXECUTE, site, tmp_path)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "custom row succeeded" in completed.stdout


def test_changed_initializer_version_rejects_before_unpickle_or_import(
    tmp_path: Path, site: Path
) -> None:
    prepared = _run(PREPARE, site, tmp_path, "creator")
    assert prepared.returncode == 0, prepared.stdout + prepared.stderr
    path = site / "bootstrap_application-1.0.dist-info/METADATA"
    path.write_text("Name: bootstrap_application\nVersion: 2.0\n")
    completed = _run(EXECUTE, site, tmp_path)
    assert completed.returncode != 0
    assert "bootstrap-application' differs from prepared metadata" in completed.stderr
    assert not (tmp_path / "imported").exists()


@pytest.mark.parametrize(
    "change", ["unqualified", "selection", "prefix", "shadow", "ambient"]
)
def test_fresh_worker_rejects_unqualified_or_inconsistent_application(
    tmp_path: Path, site: Path, change: str
) -> None:
    preparation = PREPARE
    execution = EXECUTE
    expected = "outside qualified worker code"
    if change == "unqualified":
        pass
    elif change == "selection":
        preparation = preparation.replace(
            'Software(compatibility, ("bootstrap_application",))',
            "Software(compatibility, ())",
        )
        expected = "differs from the qualified software selection"
    elif change == "prefix":
        preparation = preparation.replace(
            "application_modules=dispatch.application_modules", "application_modules=()"
        )
        expected = "differs from the live call selection"
    elif change == "shadow":
        shadow = tmp_path / "shadow" / "bootstrap_application"
        shadow.mkdir(parents=True)
        (shadow / "__init__.py").write_text(APPLICATION_SOURCE)
        execution = execution.replace(
            "root = Path(sys.argv[3])",
            'root = Path(sys.argv[3]); sys.path.insert(0, str(root / "shadow"))',
        )
    else:
        preparation = preparation.replace(
            'Software(compatibility, ("bootstrap_application",))',
            "Software(compatibility, ())",
        ).replace(
            'root / "worker", application_modules=("bootstrap_application",)',
            'root / "worker", application_modules=()',
        )
        execution = execution.replace(
            'assert "bootstrap_application" not in sys.modules',
            'os.environ["ARTISAN_LOAD_MODULES"] = "bootstrap_application"\nassert "bootstrap_application" not in sys.modules',
        )
        expected = "bootstrap_note"
    prepared = _run(preparation, site, tmp_path, "creator")
    assert prepared.returncode == 0, prepared.stdout + prepared.stderr
    if change == "unqualified":
        path = tmp_path / "header.json"
        header = json.loads(path.read_text())
        header["compatibility"]["packages"] = [
            item
            for item in header["compatibility"]["packages"]
            if item["name"] != "bootstrap-application"
        ]
        path.write_text(json.dumps(header))
    completed = _run(execution, site, tmp_path)
    assert completed.returncode != 0
    assert expected in completed.stderr
    assert (tmp_path / "imported").exists() == (change == "selection")
    assert not (tmp_path / "science").exists()


def test_loaded_parent_cannot_redirect_a_qualified_child_import(
    tmp_path: Path, site: Path
) -> None:
    package = site / "bootstrap_application"
    (package / "child.py").write_text("VALUE = 1\n")
    metadata = site / "bootstrap_application-1.0.dist-info" / "RECORD"
    metadata.write_text(metadata.read_text() + "bootstrap_application/child.py,,\n")
    shadow = tmp_path / "shadow"
    shadow.mkdir()
    (shadow / "child.py").write_text(
        "from pathlib import Path\n"
        f"Path({str(tmp_path / 'redirected')!r}).write_text('imported')\n"
    )
    source = """
        import importlib
        from pathlib import Path
        from artisan.execution.application_bootstrap import bootstrap_worker_applications
        from artisan.execution.environments.observed_sources import observed_editable_sources
        from artisan.execution.runtime_compatibility import capture_runtime_compatibility
        from artisan.schemas.execution.worker_transport import TRANSPORT_PACKAGES
        root = Path(sys.argv[3])
        package = importlib.import_module("bootstrap_application")
        names = (*TRANSPORT_PACKAGES, "bootstrap-application", "bootstrap-models")
        compatibility = capture_runtime_compatibility(names)
        package.__path__ = [str(root / "shadow")]
        bootstrap_worker_applications(("bootstrap_application.child",), qualified_modules=("bootstrap_application.child",), compatibility=compatibility)
    """
    completed = _run(source, site, tmp_path)
    assert completed.returncode != 0
    assert "changed its qualified import path" in completed.stderr
    assert not (tmp_path / "redirected").exists()


def test_custom_role_types_are_registered_before_operation_module_unpickling(
    tmp_path: Path, site: Path
) -> None:
    start = MODEL_SOURCE.index("class CopyNote(")
    end = MODEL_SOURCE.index("@dataclass")
    package = site / "bootstrap_models"
    (package / "__init__.py").write_text(MODEL_SOURCE[:start] + MODEL_SOURCE[end:])
    operation = (
        MODEL_SOURCE[: MODEL_SOURCE.index("class Note(")]
        + "from bootstrap_models import Note\n"
        + MODEL_SOURCE[start:end]
    ).replace('artifact_type="any"', 'artifact_type="bootstrap_note"')
    (package / "operation.py").write_text(operation)
    metadata = site / "bootstrap_models-1.0.dist-info" / "RECORD"
    metadata.write_text(metadata.read_text() + "bootstrap_models/operation.py,,\n")
    preparation = PREPARE.replace(
        "from bootstrap_models import CopyNote, Note, Software",
        "from bootstrap_models import Note, Software\nfrom bootstrap_models.operation import CopyNote",
    )
    prepared = _run(preparation, site, tmp_path, "creator")
    assert prepared.returncode == 0, prepared.stdout + prepared.stderr
    completed = _run(EXECUTE, site, tmp_path)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "custom row succeeded" in completed.stdout

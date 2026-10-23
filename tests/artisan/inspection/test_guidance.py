"""Exact, offline operating-guide retrieval across resource origins."""

from __future__ import annotations

import os
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest
from pydantic import ValidationError

from artisan import __version__
from artisan.errors import ArtisanError, ErrorCode
from artisan.inspection import guidance
from artisan.inspection._evidence import MAX_PAYLOAD_BYTES, encode_json
from artisan.utils.hashing import compute_content_digest

_RESOURCES = {
    "assessment-and-comparison": "references/assessment-and-comparison.md",
    "evidence-exports": "references/evidence-exports.md",
    "workflow": "references/operating-guide.md",
}


def _write_skill(directory: Path) -> None:
    directory.mkdir(parents=True)
    (directory / "references").mkdir()
    (directory / "SKILL.md").write_text("# Portable skill\n", encoding="utf-8")
    for topic, resource in _RESOURCES.items():
        (directory / resource).write_bytes(f"# {topic}\r\nCafé 日本語\r\n".encode())


@pytest.fixture
def packaged(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    package = tmp_path / "package"
    directory = package / "_skills" / "pipeline-operate"
    _write_skill(directory)
    monkeypatch.setattr(guidance, "files", lambda _: package)
    return directory


@pytest.fixture
def checkout(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    root = tmp_path / "checkout"
    module = root / "src" / "artisan" / "inspection" / "guidance.py"
    module.parent.mkdir(parents=True)
    module.write_text("", encoding="utf-8")
    (root / "pyproject.toml").write_text(
        '[project]\nname = "dexterity-artisan"\n', encoding="utf-8"
    )
    directory = root / "skills" / "pipeline-operate"
    _write_skill(directory)
    monkeypatch.setattr(guidance, "__file__", str(module))
    monkeypatch.setattr(guidance, "files", lambda _: root / "empty-package")
    return root


def test_list_operating_guides_exact_sorted_metadata(packaged: Path) -> None:
    catalog = guidance.list_operating_guides()
    assert catalog.guidance_format_version == "1"
    assert catalog.artisan_version == __version__
    assert catalog.guide_id == "pipeline-operate"
    assert catalog.origin == "package"
    assert [entry.topic for entry in catalog.topics] == sorted(_RESOURCES)
    assert len(encode_json(catalog)) <= MAX_PAYLOAD_BYTES
    for entry in catalog.topics:
        raw = (packaged / _RESOURCES[entry.topic]).read_bytes()
        assert entry.resource == _RESOURCES[entry.topic]
        assert entry.content_bytes == len(raw)
        assert entry.content_digest == compute_content_digest(raw)
        assert entry.digest_algorithm == "xxh3_128"
        assert "markdown" not in entry.model_dump()


@pytest.mark.parametrize("topic", _RESOURCES)
def test_read_operating_guide_preserves_complete_bytes(
    packaged: Path, topic: str
) -> None:
    result = guidance.read_operating_guide(topic)
    raw = (packaged / _RESOURCES[topic]).read_bytes()
    assert result.markdown.encode("utf-8") == raw
    assert result.content_bytes == len(raw)
    assert result.content_digest == compute_content_digest(raw)
    assert result.artisan_version == __version__
    assert result.origin == "package"
    assert len(encode_json(result)) <= MAX_PAYLOAD_BYTES
    entry = next(
        item for item in guidance.list_operating_guides().topics if item.topic == topic
    )
    assert (
        result.model_dump(include=set(type(entry).model_fields)) == entry.model_dump()
    )


def test_read_operating_guide_default_and_strict_models(packaged: Path) -> None:
    result = guidance.read_operating_guide()
    assert result.topic == "workflow"
    assert (
        guidance.OperatingGuideTopic.model_validate_json(result.model_dump_json())
        == result
    )
    with pytest.raises(ValidationError):
        guidance.OperatingGuideTopic.model_validate(
            {**result.model_dump(), "extra": True}
        )
    with pytest.raises(ValidationError):
        guidance.OperatingGuideTopic.model_validate(
            {**result.model_dump(), "content_bytes": "12"}
        )
    with pytest.raises(ValidationError):
        result.markdown = "changed"


@pytest.mark.parametrize(
    "topic", ["", "../SKILL.md", "/private/path", "WORKFLOW", None, []]
)
def test_read_operating_guide_unknown_topic_is_safe(topic: object) -> None:
    with pytest.raises(ArtisanError) as caught:
        guidance.read_operating_guide(topic)  # type: ignore[arg-type]
    assert caught.value.code == ErrorCode.UNKNOWN_OPERATING_GUIDE_TOPIC
    assert caught.value.envelope.message == "Unknown operating guide topic"
    assert (
        len(encode_json(caught.value.to_dict(include_cause=False))) < MAX_PAYLOAD_BYTES
    )


@pytest.mark.parametrize("raw", [b"x" * MAX_PAYLOAD_BYTES, "é".encode() * 9000])
def test_read_operating_guide_checks_complete_json_budget(
    packaged: Path, raw: bytes
) -> None:
    (packaged / _RESOURCES["workflow"]).write_bytes(raw)
    with pytest.raises(ArtisanError) as caught:
        guidance.read_operating_guide()
    assert caught.value.code == ErrorCode.EVIDENCE_RESPONSE_TOO_LARGE


def test_list_operating_guides_checks_catalog_budget(
    packaged: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(
        guidance._TOPICS, "workflow", ("x" * MAX_PAYLOAD_BYTES, _RESOURCES["workflow"])
    )
    with pytest.raises(ArtisanError) as caught:
        guidance.list_operating_guides()
    assert caught.value.code == ErrorCode.EVIDENCE_RESPONSE_TOO_LARGE


@pytest.mark.parametrize("resource", ["SKILL.md", *_RESOURCES.values()])
def test_guidance_missing_resource_never_mixes_origins(
    checkout: Path, packaged: Path, resource: str
) -> None:
    (packaged / resource).unlink()
    for retrieve in (guidance.list_operating_guides, guidance.read_operating_guide):
        with pytest.raises(ArtisanError) as caught:
            retrieve()
        assert caught.value.code == ErrorCode.OPERATING_GUIDANCE_UNAVAILABLE


def test_guidance_invalid_utf8_is_safe(packaged: Path) -> None:
    (packaged / _RESOURCES["workflow"]).write_bytes(b"private-secret\xff")
    with pytest.raises(ArtisanError) as caught:
        guidance.read_operating_guide()
    assert caught.value.code == ErrorCode.OPERATING_GUIDANCE_UNAVAILABLE
    assert caught.value.__cause__ is None
    assert "private-secret" not in str(caught.value.to_dict())


def test_guidance_source_refreshes_without_git_or_reinstall(checkout: Path) -> None:
    first = guidance.read_operating_guide()
    assert first.origin == "source_checkout"
    assert not (checkout / ".git").exists()
    changed = b"# Changed after installation\r\n"
    (checkout / "skills/pipeline-operate" / _RESOURCES["workflow"]).write_bytes(changed)
    second = guidance.read_operating_guide()
    assert second.markdown.encode() == changed
    assert second.content_digest != first.content_digest
    assert second.artisan_version == first.artisan_version


@pytest.mark.parametrize(
    "project", ['[project]\nname="unrelated"', "[broken", "project=42"]
)
def test_guidance_source_requires_exact_project(checkout: Path, project: str) -> None:
    (checkout / "pyproject.toml").write_text(project, encoding="utf-8")
    with pytest.raises(ArtisanError) as caught:
        guidance.read_operating_guide()
    assert caught.value.code == ErrorCode.OPERATING_GUIDANCE_UNAVAILABLE


def test_guidance_ignores_working_directory_and_nearby_checkout(
    checkout: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(checkout)
    monkeypatch.setattr(
        guidance,
        "__file__",
        str(checkout / "site-packages/artisan/inspection/guidance.py"),
    )
    with pytest.raises(ArtisanError) as caught:
        guidance.read_operating_guide()
    assert caught.value.code == ErrorCode.OPERATING_GUIDANCE_UNAVAILABLE


def test_guidance_package_never_reads_checkout(checkout: Path, packaged: Path) -> None:
    (checkout / "pyproject.toml").unlink()
    assert guidance.read_operating_guide().origin == "package"


def test_guidance_source_requires_all_resources(checkout: Path) -> None:
    (checkout / "skills/pipeline-operate" / _RESOURCES["evidence-exports"]).unlink()
    with pytest.raises(ArtisanError) as caught:
        guidance.read_operating_guide()
    assert caught.value.code == ErrorCode.OPERATING_GUIDANCE_UNAVAILABLE


def test_guidance_invalid_bundle_entry_does_not_fall_back(
    checkout: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package = checkout / "package"
    (package / "_skills").mkdir(parents=True)
    (package / "_skills/pipeline-operate").write_text("invalid", encoding="utf-8")
    monkeypatch.setattr(guidance, "files", lambda _: package)
    with pytest.raises(ArtisanError) as caught:
        guidance.read_operating_guide()
    assert caught.value.code == ErrorCode.OPERATING_GUIDANCE_UNAVAILABLE


def test_guidance_archive_backed_resources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive = tmp_path / "resources.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        prefix = "artisan/inspection/_skills/pipeline-operate/"
        bundle.writestr(prefix + "SKILL.md", "# Skill\n")
        for resource in _RESOURCES.values():
            bundle.writestr(prefix + resource, "# Archive\r\nΩ\r\n")
    with zipfile.ZipFile(archive) as bundle:
        directory = zipfile.Path(bundle, at="artisan/inspection/")
        monkeypatch.setattr(guidance, "files", lambda _: directory)
        result = guidance.read_operating_guide()
        assert result.origin == "package"
        assert result.markdown == "# Archive\r\nΩ\r\n"


def test_guidance_retrieval_does_not_import_applications_or_stores(
    tmp_path: Path,
) -> None:
    script = """
import sys
from artisan.inspection import list_operating_guides, read_operating_guide
assert list_operating_guides().topics
assert read_operating_guide().markdown
blocked = ('artisan.storage', 'artisan.orchestration', 'artisan.registry',
           'artisan_mcp', 'mcp', 'fastmcp', 'typer', 'pydantic_settings')
assert not any(name == item or name.startswith(item + '.')
               for name in sys.modules for item in blocked)
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env={**os.environ, "ARTISAN_LOAD_MODULES": "untrusted.application"},
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_operating_guide_assessment_and_export_examples(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fixtures.outcome_store import append_metric_attempt

    from artisan.schemas import MetricArtifact

    store = tmp_path / "store"
    metric = MetricArtifact.draft({"score": 0.8}, "case.json", 0).finalize()
    selection = append_metric_attempt(store, [metric])
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "inspect_existing.py",
            str(store),
            selection.pipeline_run_id,
            selection.step_name,
            selection.role,
            "--expected-count",
            "1",
        ],
    )
    namespace = {}
    for topic in ("assessment-and-comparison", "evidence-exports"):
        markdown = guidance.read_operating_guide(topic).markdown
        example = markdown.split("```python\n", 1)[1].split("```", 1)[0]
        exec(compile(example, f"{topic}.md", "exec"), namespace)
    assert namespace["saved"].assessment_status == "pass"
    assert namespace["saved"].observation_count == 1

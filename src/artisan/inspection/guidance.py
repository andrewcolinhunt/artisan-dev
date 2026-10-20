"""Offline operating guidance from one verified package or source checkout."""

from __future__ import annotations

import tomllib
from importlib.resources import files
from importlib.resources.abc import Traversable
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from artisan import __version__
from artisan.errors import ArtisanError, ErrorCode
from artisan.inspection._evidence import ensure_fits
from artisan.utils.hashing import compute_content_digest

_TOPICS = {
    "assessment-and-comparison": (
        "Assessment and Comparison",
        "references/assessment-and-comparison.md",
    ),
    "evidence-exports": ("Evidence Exports", "references/evidence-exports.md"),
    "workflow": ("Operating Guide", "references/operating-guide.md"),
}
type _Origin = Literal["package", "source_checkout"]


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class _GuideMetadata(_StrictModel):
    topic: str
    title: str
    resource: str
    digest_algorithm: Literal["xxh3_128"] = "xxh3_128"
    content_digest: str = Field(pattern=r"^[0-9a-f]{32}$")
    content_bytes: int = Field(ge=0)


class _OperatingGuide(_StrictModel):
    guidance_format_version: Literal["1"] = "1"
    artisan_version: str
    guide_id: Literal["pipeline-operate"] = "pipeline-operate"
    origin: _Origin


class OperatingGuideCatalog(_OperatingGuide):
    """Available topics with exact per-resource identity, without their text.

    Attributes:
        guidance_format_version: Retrieval format, independent of package version.
        artisan_version: Version generated when the executing package was built.
        guide_id: The supported operating skill's fixed identifier.
        origin: Whether every topic came from package resources or source files.
        topics: Topic-name-sorted metadata with canonical relative paths, raw-byte
            digests and byte counts. Digests identify content, not authenticity.
    """

    topics: tuple[_GuideMetadata, ...]


class OperatingGuideTopic(_OperatingGuide, _GuideMetadata):
    """One complete UTF-8 topic and its package/source attribution.

    Attributes:
        guidance_format_version: Retrieval format, independent of package version.
        artisan_version: Build-generated version; editable text can change later.
        guide_id: The supported operating skill's fixed identifier.
        origin: The complete resource directory used for this retrieval.
        topic: Topic identifier from the fixed allow-list.
        title: Human-readable topic title.
        resource: Canonical path relative to the portable skill directory.
        digest_algorithm: Algorithm used for the original Markdown bytes.
        content_digest: Digest of exactly ``markdown.encode("utf-8")``.
        content_bytes: Length of those original bytes, including line endings.
        markdown: Complete decoded text, without a generated header or clipping.
    """

    markdown: str


def _unavailable() -> ArtisanError:
    return ArtisanError(
        ErrorCode.OPERATING_GUIDANCE_UNAVAILABLE,
        "Operating guidance is unavailable or invalid",
        error_type="config",
        field="guidance",
        hint="Check this Artisan installation's bundled operating guidance.",
        recovery_hint="REPORT_TO_USER",
    )


def _source_directory() -> Path:
    module = Path(__file__).resolve()
    if module.parts[-4:] != ("src", "artisan", "inspection", "guidance.py"):
        raise _unavailable()
    root = module.parents[3]
    project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    metadata = project.get("project")
    if not isinstance(metadata, dict) or metadata.get("name") != "dexterity-artisan":
        raise _unavailable()
    directory = root / "skills" / "pipeline-operate"
    if directory.resolve() != directory:
        raise _unavailable()
    return directory


def _resource_directory() -> tuple[Traversable, _Origin]:
    bundled = files("artisan.inspection").joinpath("_skills", "pipeline-operate")
    if bundled.is_dir():
        return bundled, "package"
    if bundled.is_file():
        raise _unavailable()
    return _source_directory(), "source_checkout"


def _load_topics() -> tuple[_Origin, dict[str, tuple[bytes, str]]]:
    try:
        directory, origin = _resource_directory()
        if not directory.joinpath("SKILL.md").is_file():
            raise _unavailable()
        contents = {}
        for topic, (_, resource) in _TOPICS.items():
            raw = directory.joinpath(*resource.split("/")).read_bytes()
            contents[topic] = (raw, raw.decode("utf-8", errors="strict"))
        return origin, contents
    except (OSError, UnicodeError, ValueError, TypeError, RuntimeError):
        raise _unavailable() from None


def _metadata(topic: str, raw: bytes) -> _GuideMetadata:
    title, resource = _TOPICS[topic]
    return _GuideMetadata(
        topic=topic,
        title=title,
        resource=resource,
        content_digest=compute_content_digest(raw),
        content_bytes=len(raw),
    )


def list_operating_guides() -> OperatingGuideCatalog:
    """List the complete installed operating guide's topics without store access.

    Returns:
        Sorted topic metadata with package version, origin and raw-byte identity.

    Raises:
        ArtisanError: Guidance is unavailable/invalid, or the complete catalogue
            exceeds the shared 48 KiB canonical JSON response budget.
    """
    origin, contents = _load_topics()
    catalog = OperatingGuideCatalog(
        artisan_version=__version__,
        origin=origin,
        topics=tuple(_metadata(topic, contents[topic][0]) for topic in sorted(_TOPICS)),
    )
    ensure_fits(catalog.model_dump(mode="python"))
    return catalog


def read_operating_guide(topic: str = "workflow") -> OperatingGuideTopic:
    """Read one whole installed topic without bootstrapping an application.

    Args:
        topic: Fixed topic identifier; defaults to the operating workflow.

    Returns:
        Exact decoded Markdown and its raw-byte identity and package attribution.
        Editable source content is reread on every call.

    Raises:
        ArtisanError: The topic is unknown, resources are unavailable/invalid, or
            the complete response exceeds the shared 48 KiB JSON budget.
    """
    if type(topic) is not str or topic not in _TOPICS:
        raise ArtisanError(
            ErrorCode.UNKNOWN_OPERATING_GUIDE_TOPIC,
            "Unknown operating guide topic",
            error_type="validation",
            field="topic",
            hint="List the available operating guide topics and select one.",
            recovery_hint="CHECK_INPUT",
        )
    origin, contents = _load_topics()
    raw, markdown = contents[topic]
    result = OperatingGuideTopic(
        **_metadata(topic, raw).model_dump(mode="python"),
        artisan_version=__version__,
        origin=origin,
        markdown=markdown,
    )
    ensure_fits(
        result.model_dump(mode="python"),
        hint="The complete guide topic exceeds the budget; split its authored content.",
    )
    return result

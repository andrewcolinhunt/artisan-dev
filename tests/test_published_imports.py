"""Published Python examples import Artisan only through supported facades."""

from __future__ import annotations

import ast
import json
import re
from collections.abc import Iterator
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_ALLOW_INTERNAL = "<!-- artisan-import-policy: allow-internal -->"
_FENCE = re.compile(r"^\s{0,3}(`{3,}|~{3,})(.*)$")
_PYTHON_LANGUAGES = {"py", "python", "python3", "ipython", "ipython3"}
_PUBLIC_MODULES = {
    "artisan",
    "artisan.composites",
    "artisan.operations.base",
    "artisan.operations.curator",
    "artisan.operations.examples",
    "artisan.operations.lineage",
    "artisan.orchestration",
    "artisan.orchestration.runner_api",
    "artisan.provenance",
    "artisan.registry",
    "artisan.schemas",
    "artisan.storage",
    "artisan.utils",
    "artisan.visualization",
    "artisan_mcp",
}


def _is_python_fence(info: str) -> bool:
    words = info.strip().lower().split()
    if not words:
        return False
    language = words[0].strip("{}").removeprefix(".")
    if language in _PYTHON_LANGUAGES:
        return True
    return (
        words[0] in {"{code-cell}", "{code-block}"}
        and len(words) > 1
        and words[1].strip("{}").removeprefix(".") in _PYTHON_LANGUAGES
    )


def _markdown_blocks(path: Path) -> Iterator[tuple[str, int, str]]:
    lines = path.read_text().splitlines()
    index = 0
    while index < len(lines):
        match = _FENCE.match(lines[index])
        if match is None:
            index += 1
            continue
        fence, info = match.groups()
        start = index + 2
        index += 1
        body: list[str] = []
        closing = re.compile(rf"^\s{{0,3}}{re.escape(fence[0])}{{{len(fence)},}}\s*$")
        while index < len(lines) and closing.match(lines[index]) is None:
            body.append(lines[index])
            index += 1
        exempt = start >= 3 and lines[start - 3].strip() == _ALLOW_INTERNAL
        if _is_python_fence(info) and not exempt:
            yield "\n".join(body), start, str(path.relative_to(_ROOT))
        index += 1


def _notebook_blocks(path: Path) -> Iterator[tuple[str, int, str]]:
    notebook = json.loads(path.read_text())
    relative = path.relative_to(_ROOT)
    for cell_number, cell in enumerate(notebook.get("cells", []), start=1):
        if cell.get("cell_type") != "code":
            continue
        source = cell.get("source", "")
        if isinstance(source, list):
            source = "".join(source)
        yield str(source), 1, f"{relative}:cell-{cell_number}"


def _published_blocks() -> Iterator[tuple[str, int, str]]:
    yield from _markdown_blocks(_ROOT / "README.md")
    generated_docs = _ROOT / "docs" / "_build"
    for path in sorted((_ROOT / "docs").rglob("*")):
        if path.is_relative_to(generated_docs):
            continue
        if path.suffix == ".md":
            yield from _markdown_blocks(path)
        elif path.suffix == ".ipynb":
            yield from _notebook_blocks(path)
    for path in sorted((_ROOT / "docker" / "examples").rglob("*.py")):
        yield path.read_text(), 1, str(path.relative_to(_ROOT))
    for path in sorted((_ROOT / "skills").glob("*/SKILL.md")):
        yield from _markdown_blocks(path)


def _artisan_modules(source: str) -> Iterator[tuple[str, int]]:
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if _is_artisan_module(alias.name):
                    yield alias.name, node.lineno
            continue
        if not isinstance(node, ast.ImportFrom):
            continue

        module = node.module or ""
        if _is_artisan_module(module):
            yield f"{'.' * node.level}{module}", node.lineno
            continue
        if node.level:
            for alias in node.names:
                if _is_artisan_module(alias.name):
                    prefix = f"{module}." if module else ""
                    yield f"{'.' * node.level}{prefix}{alias.name}", node.lineno


def _is_artisan_module(module: str) -> bool:
    return module in {"artisan", "artisan_mcp"} or module.startswith(
        ("artisan.", "artisan_mcp.")
    )


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("import artisan.schemas as schemas", [("artisan.schemas", 1)]),
        (
            "if True: import artisan.execution.private as hidden",
            [("artisan.execution.private", 1)],
        ),
        (
            "ready = True; import artisan.schemas.artifact.base",
            [("artisan.schemas.artifact.base", 1)],
        ),
        (
            "from artisan.schemas import (\n    Artifact,\n)",
            [("artisan.schemas", 1)],
        ),
        ("from .artisan import schemas", [(".artisan", 1)]),
        ("from ..artisan.schemas import Artifact", [("..artisan.schemas", 1)]),
        ("from . import artisan", [(".artisan", 1)]),
        ("from .helpers import artisan", [(".helpers.artisan", 1)]),
        ('example = "import artisan.execution.private"', []),
        ("from helpers import artisan", []),
    ],
)
def test_artisan_modules_walks_the_complete_syntax_tree(
    source: str, expected: list[tuple[str, int]]
) -> None:
    assert list(_artisan_modules(source)) == expected


def test_artisan_modules_propagates_syntax_errors() -> None:
    with pytest.raises(SyntaxError):
        list(_artisan_modules("if True import artisan"))


@pytest.mark.parametrize(
    "info",
    [
        "python",
        "Python title=example.py",
        "python {.example}",
        "{python} title=example.py",
        "{.python #example}",
        "{code-cell} python",
        "{code-block} python",
        "{CODE-CELL} IPYTHON3 tags=[example]",
    ],
)
def test_python_fence_languages_and_attributes_are_recognized(info: str) -> None:
    assert _is_python_fence(info)


@pytest.mark.parametrize("info", ["", "text", "{include} page.md", "javascript"])
def test_non_python_fences_are_ignored(info: str) -> None:
    assert not _is_python_fence(info)


def test_allow_marker_exempts_only_the_immediately_following_fence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(_markdown_blocks.__globals__, "_ROOT", tmp_path)
    page = tmp_path / "page.md"
    page.write_text(
        "\n".join(
            [
                _ALLOW_INTERNAL,
                "```Python title=internal.py",
                "import artisan.private",
                "```",
                _ALLOW_INTERNAL,
                "",
                "~~~{code-cell} python",
                "import artisan.schemas",
                "~~~",
            ]
        )
    )

    assert list(_markdown_blocks(page)) == [("import artisan.schemas", 8, "page.md")]


@pytest.mark.parametrize(
    "source", ["import artisan.schemas\n", ["import artisan.schemas\n"]]
)
def test_notebook_code_cell_accepts_string_and_list_sources(
    source: str | list[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(_notebook_blocks.__globals__, "_ROOT", tmp_path)
    notebook = tmp_path / "example.ipynb"
    notebook.write_text(
        json.dumps(
            {
                "cells": [
                    {"cell_type": "markdown", "source": "ignored"},
                    {"cell_type": "code", "source": source},
                ]
            }
        )
    )

    assert list(_notebook_blocks(notebook)) == [
        ("import artisan.schemas\n", 1, "example.ipynb:cell-2")
    ]


def test_only_top_level_generated_docs_are_excluded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(_published_blocks.__globals__, "_ROOT", tmp_path)
    (tmp_path / "README.md").write_text("")
    generated = tmp_path / "docs" / "_build"
    generated.mkdir(parents=True)
    (generated / "generated.md").write_text(
        "```python\nimport artisan.generated.private\n```\n"
    )
    published = tmp_path / "docs" / "chapter" / "_build"
    published.mkdir(parents=True)
    (published / "page.md").write_text("```python\nimport artisan.schemas\n```\n")

    locations = [location for _, _, location in _published_blocks()]

    assert locations == ["docs/chapter/_build/page.md"]


def test_published_python_uses_only_supported_facades() -> None:
    violations: list[str] = []
    syntax_errors: list[str] = []
    for source, start_line, location in _published_blocks():
        try:
            imports = _artisan_modules(source)
            for module, line in imports:
                if module not in _PUBLIC_MODULES:
                    violations.append(f"{location}:{start_line + line - 1}: {module}")
        except SyntaxError as exc:
            syntax_errors.append(
                f"{location}:{start_line + (exc.lineno or 1) - 1}: {exc.msg}"
            )

    assert not syntax_errors, "invalid published Python:\n" + "\n".join(syntax_errors)
    assert not violations, "non-public Artisan imports:\n" + "\n".join(violations)

"""Published Python examples import Artisan only through supported facades."""

from __future__ import annotations

import ast
import json
import re
import textwrap
from collections.abc import Iterator
from pathlib import Path

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
    if words[0] in _PYTHON_LANGUAGES:
        return True
    return (
        words[0] == "{code-cell}"
        and len(words) > 1
        and words[1]
        in {
            "python",
            "python3",
            "ipython",
            "ipython3",
        }
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
    for path in sorted((_ROOT / "docs").rglob("*")):
        if "_build" in path.parts:
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
    lines = source.splitlines()
    index = 0
    while index < len(lines):
        stripped = lines[index].lstrip()
        if not stripped.startswith(("import ", "from ")):
            index += 1
            continue
        start = index + 1
        statement = [lines[index]]
        balance = stripped.count("(") - stripped.count(")")
        continued = stripped.rstrip().endswith("\\")
        index += 1
        while index < len(lines) and (balance > 0 or continued):
            statement.append(lines[index])
            balance += lines[index].count("(") - lines[index].count(")")
            continued = lines[index].rstrip().endswith("\\")
            index += 1

        tree = ast.parse(textwrap.dedent("\n".join(statement)))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules = (alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                modules = (node.module,)
            else:
                continue
            for module in modules:
                if module in {"artisan", "artisan_mcp"} or module.startswith(
                    ("artisan.", "artisan_mcp.")
                ):
                    yield module, start + node.lineno - 1


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

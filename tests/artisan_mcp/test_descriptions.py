"""Tool-description format regression: length bound + verb-first.

Descriptions are hand-written agent-facing copy: 200-1000 chars, opening
with an imperative verb that says what the tool does.
"""

from __future__ import annotations

import asyncio

from fastmcp import Client

# Imperative openers used across the tool surface. A new tool adds its verb
# here deliberately, keeping the copy uniform.
_VERBS = {"List", "Describe", "Report", "Get", "Find", "Tail", "Walk", "Diagnose"}


def _descriptions(app) -> dict[str, str]:
    async def _run() -> dict[str, str]:
        async with Client(app) as client:
            return {t.name: (t.description or "") for t in await client.list_tools()}

    return asyncio.run(_run())


class TestDescriptions:
    def test_length_in_bounds(self, make_app) -> None:
        for name, desc in _descriptions(make_app()).items():
            assert 200 <= len(desc) <= 1000, f"{name}: {len(desc)} chars"

    def test_verb_first(self, make_app) -> None:
        for name, desc in _descriptions(make_app()).items():
            first = desc.strip().split()[0]
            assert first in _VERBS, (
                f"{name} opens with {first!r}, not an imperative verb"
            )

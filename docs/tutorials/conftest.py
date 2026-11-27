"""Pytest hooks for tutorial-notebook collection — auto-mark and skip-list.

Co-located with the notebooks themselves so the marker hook only loads
when pytest collects under ``docs/tutorials/``. nbval discovers .ipynb
files via ``--nbval-lax`` (passed by the ``test`` and ``test-notebook`` tasks).

Three responsibilities:

1. Auto-apply the ``notebook`` marker to every nbval-collected item, so
   ``pytest -m notebook`` selects them and ``pytest -m 'not notebook'``
   excludes them.

2. Auto-apply infrastructure markers (currently ``modal``) to notebooks
   that need external infrastructure. ``test-notebook`` excludes them via
   ``-m 'notebook and not modal'``; the opt-in ``test-notebook-modal``
   task selects them where the infrastructure exists. Listed by path relative
   to this conftest's directory so the mapping is auditable.

3. Keep each notebook's cells together when the combined suite uses xdist's
   ``loadgroup`` scheduler.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_TUTORIALS_DIR = Path(__file__).parent

INFRA_NOTEBOOKS = {
    "07-compute-backends/04-modal-execution.ipynb": "modal",
    "07-compute-backends/05-modal-r2-outputs.ipynb": "modal",
}

SKIP_NOTEBOOKS_BROKEN: set[str] = set()


def _relative_to_tutorials(item: pytest.Item) -> str | None:
    """Return the item's path relative to docs/tutorials/, or None if outside."""
    fspath = Path(str(item.fspath))
    if fspath.suffix != ".ipynb":
        return None
    try:
        return fspath.relative_to(_TUTORIALS_DIR).as_posix()
    except ValueError:
        return None


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    """Mark and group tutorial cells before xdist assigns worker groups."""
    del config
    skip_broken = pytest.mark.skip(
        reason="Notebook has pre-existing runtime bugs; tracked separately"
    )
    notebook_marker = pytest.mark.notebook

    for item in items:
        rel = _relative_to_tutorials(item)
        if rel is None:
            continue
        item.add_marker(notebook_marker)
        item.add_marker(pytest.mark.xdist_group(name=f"notebook:{rel}"))
        if rel in SKIP_NOTEBOOKS_BROKEN:
            item.add_marker(skip_broken)
            continue
        infra = INFRA_NOTEBOOKS.get(rel)
        if infra is not None:
            item.add_marker(getattr(pytest.mark, infra))

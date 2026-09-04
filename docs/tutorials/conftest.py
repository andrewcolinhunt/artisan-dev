"""Pytest hooks for tutorial-notebook collection — auto-mark and skip-list.

Co-located with the notebooks themselves so the marker hook only loads
when pytest collects under ``docs/tutorials/``. nbval discovers .ipynb
files via ``--nbval-lax`` (passed by the ``test-notebook`` pixi task).

Two responsibilities:

1. Auto-apply the ``notebook`` marker to every nbval-collected item, so
   ``pytest -m notebook`` selects them and ``pytest -m 'not notebook'``
   excludes them.

2. Auto-apply infrastructure markers (currently ``modal``) to notebooks
   that need external infrastructure. ``test-notebook`` excludes them via
   ``-m 'notebook and not modal'``; the opt-in ``test-notebook-modal``
   task selects them where the infrastructure exists. Listed by path relative
   to this conftest's directory so the mapping is auditable.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_TUTORIALS_DIR = Path(__file__).parent

# Notebooks that require external infrastructure, mapped to the marker
# naming what they need. They are excluded from the default notebook run
# and selected by the opt-in task on a Modal-credentialed machine. Paths are
# relative to docs/tutorials/.
INFRA_NOTEBOOKS = {
    "07-compute-backends/04-modal-execution.ipynb": "modal",
    # additionally needs R2/S3 credentials + the r2-artisan Modal secret
    # on the wait_tool deployment (see the notebook's setup cell)
    "07-compute-backends/05-modal-r2-outputs.ipynb": "modal",
    "04-batching/02-batch-execute.ipynb": "modal",
}

# Notebooks with pre-existing runtime bugs to fix as separate work.
# Currently empty — entries should land here, not be silently broken.
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


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    """Apply notebook and infrastructure markers and skip broken notebooks."""
    skip_broken = pytest.mark.skip(
        reason="Notebook has pre-existing runtime bugs; tracked separately"
    )
    notebook_marker = pytest.mark.notebook

    for item in items:
        rel = _relative_to_tutorials(item)
        if rel is None:
            continue
        item.add_marker(notebook_marker)
        if rel in SKIP_NOTEBOOKS_BROKEN:
            item.add_marker(skip_broken)
            continue
        infra = INFRA_NOTEBOOKS.get(rel)
        if infra is not None:
            item.add_marker(getattr(pytest.mark, infra))

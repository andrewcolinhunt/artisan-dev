"""Pytest hooks for tutorial-notebook collection — auto-mark and skip-list.

Co-located with the notebooks themselves so the marker hook only loads
when pytest collects under ``docs/tutorials/``. nbval discovers .ipynb
files via ``--nbval-lax`` (passed by the ``test-notebook`` pixi task).

Two responsibilities:

1. Auto-apply the ``notebook`` marker to every nbval-collected item, so
   ``pytest -m notebook`` selects them and ``pytest -m 'not notebook'``
   excludes them.

2. Skip notebooks that require external infrastructure (SLURM, Modal,
   real S3) — they cannot run on the local CI machine. Listed by path
   relative to this conftest's directory so the deny-list is auditable.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_TUTORIALS_DIR = Path(__file__).parent

# Notebooks that require external infrastructure (SLURM/Modal/S3) and
# can't be exercised in the local CI environment. Paths are relative
# to docs/tutorials/.
SKIP_NOTEBOOKS_INFRA = {
    "07-compute-backends/02-slurm-execution.ipynb",
    "07-compute-backends/03-slurm-intra-execution.ipynb",
    "06-storage/02-external-file-storage.ipynb",
    "07-compute-backends/01-compute-routing.ipynb",
    "07-compute-backends/04-modal-execution.ipynb",
    # 04-batching/02-batch-execute genuinely runs against Modal (header
    # says "Modal account required: Yes"). The kwarg syntax in its
    # cells is now correct (compute_provider=, batch_strategy=) so
    # when a CI job gains Modal credentials it can be un-skipped.
    "04-batching/02-batch-execute.ipynb",
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
    config: pytest.Config,  # noqa: ARG001  # pytest hook signature
    items: list[pytest.Item],
) -> None:
    """Apply the ``notebook`` marker; skip the infra/broken deny-lists."""
    skip_infra = pytest.mark.skip(
        reason="Notebook needs external infrastructure (SLURM/Modal/S3)"
    )
    skip_broken = pytest.mark.skip(
        reason="Notebook has pre-existing runtime bugs; tracked separately"
    )
    notebook_marker = pytest.mark.notebook

    for item in items:
        rel = _relative_to_tutorials(item)
        if rel is None:
            continue
        item.add_marker(notebook_marker)
        if rel in SKIP_NOTEBOOKS_INFRA:
            item.add_marker(skip_infra)
        elif rel in SKIP_NOTEBOOKS_BROKEN:
            item.add_marker(skip_broken)

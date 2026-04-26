"""Pytest hooks for notebook collection — auto-mark and skip-list.

Loaded from tests/conftest.py. nbval discovers .ipynb files under the
``-m notebook`` selector via the ``--nbval-lax`` flag passed in the
``test-notebook`` pixi task.

Two responsibilities:

1. Auto-apply the ``notebook`` marker to every nbval-collected item, so
   ``pytest -m notebook`` selects them and ``pytest -m 'not notebook'``
   excludes them.

2. Skip notebooks that require external infrastructure (SLURM, Modal,
   real S3) — they cannot run on the local CI machine. Listed via
   relative path so the deny-list is auditable.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# Notebooks that require external infrastructure (SLURM/Modal/S3) and
# can't be exercised in the local CI environment.
SKIP_NOTEBOOKS_INFRA = {
    "docs/tutorials/execution/07-slurm-execution.ipynb",
    "docs/tutorials/execution/10-slurm-intra-execution.ipynb",
    "docs/tutorials/execution/11-external-file-storage.ipynb",
    "docs/tutorials/execution/13-compute-routing.ipynb",
    "docs/tutorials/execution/14-modal-execution.ipynb",
    # 15-batch-execute genuinely runs against Modal (header says "Modal
    # account required: Yes"). The kwarg syntax in its cells is now
    # correct (compute_provider=, batch_strategy=) so when a CI job
    # gains Modal credentials it can be un-skipped.
    "docs/tutorials/execution/15-batch-execute.ipynb",
}

# Notebooks with pre-existing runtime bugs unrelated to the API-contract
# tests landing here. Currently empty — bugs in 01-writing-an-operation
# and 02-writing-a-composite were fixed alongside this gate landing.
SKIP_NOTEBOOKS_BROKEN: set[str] = set()

SKIP_NOTEBOOKS = SKIP_NOTEBOOKS_INFRA | SKIP_NOTEBOOKS_BROKEN


def is_notebook_item(item: pytest.Item) -> bool:
    """Match nbval-collected items by their path suffix."""
    return str(item.fspath).endswith(".ipynb")


def relative_notebook_path(item: pytest.Item) -> str:
    """Return the repo-relative path of an nbval item (POSIX form)."""
    fspath = Path(str(item.fspath))
    try:
        return str(fspath.relative_to(Path.cwd())).replace("\\", "/")
    except ValueError:
        return fspath.as_posix()


def pytest_collection_modifyitems(
    config: pytest.Config,  # noqa: ARG001  # pytest hook signature
    items: list[pytest.Item],
) -> None:
    """Apply ``notebook`` marker to every .ipynb item; skip the deny-list."""
    skip_infra = pytest.mark.skip(
        reason="Notebook needs external infrastructure (SLURM/Modal/S3)"
    )
    skip_broken = pytest.mark.skip(
        reason="Notebook has pre-existing runtime bugs; tracked separately"
    )
    notebook_marker = pytest.mark.notebook

    for item in items:
        if not is_notebook_item(item):
            continue
        item.add_marker(notebook_marker)
        rel = relative_notebook_path(item)
        if rel in SKIP_NOTEBOOKS_INFRA:
            item.add_marker(skip_infra)
        elif rel in SKIP_NOTEBOOKS_BROKEN:
            item.add_marker(skip_broken)

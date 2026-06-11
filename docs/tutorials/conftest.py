"""Pytest hooks for tutorial-notebook collection — auto-mark and skip-list.

Co-located with the notebooks themselves so the marker hook only loads
when pytest collects under ``docs/tutorials/``. nbval discovers .ipynb
files via ``--nbval-lax`` (passed by the ``test-notebook`` pixi task).

Three responsibilities:

1. Auto-apply the ``notebook`` marker to every nbval-collected item, so
   ``pytest -m notebook`` selects them and ``pytest -m 'not notebook'``
   excludes them.

2. Auto-apply infrastructure markers (``modal``, ``slurm``) to notebooks
   that need external infrastructure. ``test-notebook`` excludes them via
   ``-m 'notebook and not modal and not slurm'``; the opt-in tasks
   ``test-notebook-modal`` / ``test-notebook-slurm`` select them where
   the infrastructure exists. Listed by path relative to this conftest's
   directory so the mapping is auditable.

3. Preflight the self-hosted Prefect server. Every runnable notebook
   calls ``PipelineManager.create()``, which discovers and health-checks
   the server — without one, every cell fails with an opaque per-cell
   error. Probe once up front and skip loudly instead.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_TUTORIALS_DIR = Path(__file__).parent

# Notebooks that require external infrastructure, mapped to the marker
# naming what they need. They are excluded from the default notebook run
# and selected by the opt-in tasks (test-notebook-modal on a Modal-
# credentialed machine, test-notebook-slurm on a cluster). Paths are
# relative to docs/tutorials/.
INFRA_NOTEBOOKS = {
    "07-compute-backends/04-modal-execution.ipynb": "modal",
    "04-batching/02-batch-execute.ipynb": "modal",
    "07-compute-backends/02-slurm-execution.ipynb": "slurm",
    # slurm-intra additionally needs an active salloc/sbatch allocation.
    "07-compute-backends/03-slurm-intra-execution.ipynb": "slurm",
}

# Notebooks with pre-existing runtime bugs to fix as separate work.
# Currently empty — entries should land here, not be silently broken.
SKIP_NOTEBOOKS_BROKEN: set[str] = set()

# Probe result cache: False = not yet probed; None = server reachable;
# str = skip reason. Probed at most once per pytest process (each xdist
# worker collects independently).
_PREFECT_SKIP_REASON: str | None | bool = False


def _prefect_unavailable() -> str | None:
    """Return a skip reason when no Prefect server is reachable, else None.

    Reuses the production discovery (env var → discovery file → profile,
    plus health check) so this preflight agrees exactly with what
    ``PipelineManager.create()`` will do inside the notebook kernels.
    """
    global _PREFECT_SKIP_REASON
    if _PREFECT_SKIP_REASON is not False:
        return _PREFECT_SKIP_REASON

    from artisan.orchestration.prefect_server import (
        PrefectServerNotFound,
        PrefectServerUnreachable,
        PrefectVersionMismatch,
        discover_server,
    )

    try:
        discover_server()
        _PREFECT_SKIP_REASON = None
    except (
        PrefectServerNotFound,
        PrefectServerUnreachable,
        PrefectVersionMismatch,
    ) as exc:
        detail = str(exc).splitlines()[0] if str(exc) else type(exc).__name__
        _PREFECT_SKIP_REASON = (
            "No Prefect server reachable — tutorial notebooks run real "
            "pipelines against the self-hosted server. Start it with "
            f"`pixi run prefect-start`, then re-run. ({type(exc).__name__}: "
            f"{detail})"
        )
    return _PREFECT_SKIP_REASON


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
    """Apply ``notebook`` + infra markers; skip broken; preflight Prefect."""
    skip_broken = pytest.mark.skip(
        reason="Notebook has pre-existing runtime bugs; tracked separately"
    )
    notebook_marker = pytest.mark.notebook

    runnable: list[pytest.Item] = []
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
        # Infra notebooks stay in `runnable`: when their opt-in task
        # selects them they need the Prefect preflight just like the
        # default set (-m deselection happens after this hook).
        runnable.append(item)

    if not runnable:
        return
    prefect_reason = _prefect_unavailable()
    if prefect_reason is not None:
        # Loud: a banner line on the terminal (when one exists — xdist
        # workers have none) plus the reason on every skipped item, which
        # the short test summary prints.
        reporter = config.pluginmanager.get_plugin("terminalreporter")
        if reporter is not None:
            reporter.write_line(prefect_reason, yellow=True, bold=True)
        skip_prefect = pytest.mark.skip(reason=prefect_reason)
        for item in runnable:
            item.add_marker(skip_prefect)

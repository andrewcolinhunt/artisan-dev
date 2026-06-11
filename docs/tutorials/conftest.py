"""Pytest hooks for tutorial-notebook collection — auto-mark and skip-list.

Co-located with the notebooks themselves so the marker hook only loads
when pytest collects under ``docs/tutorials/``. nbval discovers .ipynb
files via ``--nbval-lax`` (passed by the ``test-notebook`` pixi task).

Three responsibilities:

1. Auto-apply the ``notebook`` marker to every nbval-collected item, so
   ``pytest -m notebook`` selects them and ``pytest -m 'not notebook'``
   excludes them.

2. Skip notebooks that require external infrastructure (SLURM, Modal,
   real S3) — they cannot run on the local CI machine. Listed by path
   relative to this conftest's directory so the deny-list is auditable.

3. Preflight the self-hosted Prefect server. Every runnable notebook
   calls ``PipelineManager.create()``, which discovers and health-checks
   the server — without one, every cell fails with an opaque per-cell
   error. Probe once up front and skip loudly instead.
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
    """Apply the ``notebook`` marker; skip deny-lists; preflight Prefect."""
    skip_infra = pytest.mark.skip(
        reason="Notebook needs external infrastructure (SLURM/Modal/S3)"
    )
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
        if rel in SKIP_NOTEBOOKS_INFRA:
            item.add_marker(skip_infra)
        elif rel in SKIP_NOTEBOOKS_BROKEN:
            item.add_marker(skip_broken)
        else:
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

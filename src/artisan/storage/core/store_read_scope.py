"""One fresh retained visibility boundary per ordinary store query."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from typing import Concatenate, Protocol

from fsspec import AbstractFileSystem

from artisan.errors import StoreIntegrityError
from artisan.storage.core.curator_view import CuratorView, capture_curator_view
from artisan.storage.core.query_view import QueryScope
from artisan.storage.core.store_format import assert_retained_store
from artisan.storage.io.retained_commit import RetainedCommitter


class RetainedStoreReadScope:
    """Share one finite proof across nested artifact and provenance queries.

    The scope holds no snapshot between calls. Concurrent callers receive their
    own view; a curator instead supplies its existing fixed view directly.
    """

    def __init__(self, committer: RetainedCommitter) -> None:
        self.committer = committer
        self._current: ContextVar[CuratorView | None] = ContextVar(
            "artisan_store_read", default=None
        )

    @property
    def files_root(self) -> str:
        """Return the managed carrier root bound to the store."""
        return self.committer.files_root

    @property
    def view(self) -> CuratorView | None:
        """Return only the calling query's view, when a query is active."""
        return self._current.get()

    def validate_binding(
        self, root: str, fs: AbstractFileSystem, storage_options: dict[str, str]
    ) -> None:
        """Reject a facade bound to another store or filesystem instance."""
        if (
            root != self.committer.delta_root
            or fs is not self.committer.fs
            or storage_options != self.committer.storage_options
        ):
            msg = "Retained query scope belongs to another store binding"
            raise StoreIntegrityError(msg)
        assert_retained_store(root, fs)

    @contextmanager
    def activate(self) -> Iterator[None]:
        """Keep nested calls on this query's proof, releasing it on every exit."""
        if self.view is not None:
            yield
            return
        with self.committer._read_snapshot() as read:
            view = _RetainedQueryView(
                self.committer,
                capture_curator_view(self.committer, _read=read),
                _read=read,
            )
            token = self._current.set(view)
            try:
                yield
            finally:
                self._current.reset(token)


class _RetainedQueryView(CuratorView):
    """Use the current query's complete lifecycle proof for ordinary names."""

    def step_names(self, pipeline_run_id: str | None) -> dict[int, str]:
        from artisan.orchestration.engine.retained_step_tracker import (
            RetainedStepTracker,
        )

        assert self._read is not None
        tracker = RetainedStepTracker(self.committer, "")
        _, attempts = tracker._read_lifecycle(self._read)
        return {
            state.step_number: state.step_name
            for state in tracker._select_current_states(attempts, pipeline_run_id)
        }


class _ScopedStore(Protocol):
    _read_scope: QueryScope | None


def with_read_scope[T: _ScopedStore, **P, R](
    method: Callable[Concatenate[T, P], R],
) -> Callable[Concatenate[T, P], R]:
    """Bind ordinary retained queries while preserving fixed curator reads."""

    @wraps(method)
    def read(self: T, /, *args: P.args, **kwargs: P.kwargs) -> R:
        if self._read_scope is None:
            return method(self, *args, **kwargs)
        with self._read_scope.activate():
            return method(self, *args, **kwargs)

    return read

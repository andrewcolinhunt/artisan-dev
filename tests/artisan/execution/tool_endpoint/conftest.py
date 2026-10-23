"""Share endpoint workspace and worker-style storage setup."""

from __future__ import annotations

import tempfile
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from artisan.schemas.execution.storage_config import StorageConfig


@pytest.fixture
def capture_tempdirs(monkeypatch: pytest.MonkeyPatch) -> Callable[[], list[str]]:
    """Install a temporary-directory spy at the test's chosen call boundary."""

    def capture() -> list[str]:
        created: list[str] = []
        real = tempfile.mkdtemp

        def spy(*args: Any, **kwargs: Any) -> str:
            path = real(*args, **kwargs)
            created.append(path)
            return path

        monkeypatch.setattr(tempfile, "mkdtemp", spy)
        return created

    return capture


@pytest.fixture
def configure_ambient_s3(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Callable[[StorageConfig], None]]:
    """Configure storage credentials as the worker receives them via secrets."""
    import s3fs

    def configure(storage: StorageConfig) -> None:
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", storage.options["key"])
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", storage.options["secret"])
        monkeypatch.setenv(
            "AWS_ENDPOINT_URL", storage.options["client_kwargs"]["endpoint_url"]
        )
        # Discard filesystems created before these environment values changed.
        s3fs.S3FileSystem.clear_instance_cache()

    yield configure
    s3fs.S3FileSystem.clear_instance_cache()

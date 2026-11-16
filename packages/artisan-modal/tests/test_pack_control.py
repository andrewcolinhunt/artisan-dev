"""Fixed transport records cannot swap units, live payloads or worker entries."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from artisan_modal.pack_control import (
    ModalPack,
    PackChannel,
    PackEntry,
    PackResult,
    UnitProgress,
)
from fsspec import AbstractFileSystem
from pydantic import ValidationError

from artisan.orchestration.runner_api import (
    InertCallResult,
    OwnershipState,
)


def progress(request: ModalPack, entry: PackEntry, index: int = 0) -> UnitProgress:
    return UnitProgress(
        **entry.model_dump(),
        index=index,
        payload_sha256=request.calls[index].header.payload_sha256,
        outcome=InertCallResult(
            entry_id=uuid4(),
            ownership=OwnershipState.CLOSED,
            reply=b"{}",
            retained_working=False,
        ),
    )


def test_channel_conditional_claim_and_progress_bind_exact_membership(
    pack_request: ModalPack, control_fs: AbstractFileSystem
) -> None:
    channel = PackChannel(pack_request)
    entry = channel.claim(uuid4())
    with pytest.raises(FileExistsError):
        channel.claim(uuid4())
    value = progress(pack_request, entry)
    channel.publish(value)
    assert channel.progress(entry, 0) == value
    with pytest.raises(FileExistsError):
        channel.publish(value)
    serialized = control_fs._path(channel.root + "/entry.json")
    assert "selected-secret" not in Path(serialized).read_text()
    channel.cancel(1, 3)
    assert channel.cancellation(1).cleanup_seconds == 3
    channel.cancel(1, 5)
    assert channel.cancellation(1).cleanup_seconds == 3


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("pack_id", uuid4()),
        ("request_sha256", "0" * 64),
        ("entry_id", uuid4()),
        ("index", 1),
        ("payload_sha256", "1" * 64),
    ],
)
def test_progress_swap_rejects(
    pack_request: ModalPack, control_fs: AbstractFileSystem, field: str, value: object
) -> None:
    channel = PackChannel(pack_request)
    entry = channel.claim(uuid4())
    changed = progress(pack_request, entry).model_copy(update={field: value})
    control_fs.pipe_file(
        channel.root + "/unit-0.json", changed.model_dump_json().encode()
    )
    with pytest.raises(ValueError, match=r"differs|index"):
        channel.progress(entry, 0)


def test_pack_rejects_duplicate_identity_and_mixed_software_before_entry(
    pack_request: ModalPack,
) -> None:
    changed = pack_request.model_copy(
        update={"calls": (pack_request.calls[0], pack_request.calls[0])}
    )
    with pytest.raises(ValidationError, match="duplicate"):
        ModalPack.model_validate_json(changed.model_dump_json())
    call = pack_request.calls[1]
    call = call.model_copy(
        update={"function": call.function.model_copy(update={"image_id": "im-other"})}
    )
    changed = pack_request.model_copy(update={"calls": (pack_request.calls[0], call)})
    with pytest.raises(ValidationError, match="identical"):
        ModalPack.model_validate_json(changed.model_dump_json())


@pytest.mark.usefixtures("control_fs")
def test_closed_pack_requires_all_results_and_distinct_actual_entries(
    pack_request: ModalPack,
) -> None:
    entry = PackChannel(pack_request).claim(uuid4())
    first = progress(pack_request, entry)
    incomplete = PackResult(
        **entry.model_dump(), ownership=OwnershipState.CLOSED, results=(first,)
    )
    with pytest.raises(ValueError, match="every unit"):
        incomplete.verify(pack_request)
    second = progress(pack_request, entry, 1).model_copy(
        update={"outcome": first.outcome}
    )
    with pytest.raises(ValueError, match="reused"):
        incomplete.model_copy(update={"results": (first, second)}).verify(pack_request)


def test_cancellation_rejects_wrong_request_and_invalid_duration(
    pack_request: ModalPack, control_fs: AbstractFileSystem
) -> None:
    channel = PackChannel(pack_request)
    channel.cancel(0, 1)
    path = channel.root + "/cancel-0.json"
    with control_fs.open(path) as stream:
        data = stream.read().replace(pack_request.digest.encode(), b"0" * 64)
    control_fs.pipe_file(path, data)
    with pytest.raises(ValueError, match="exact pack"):
        channel.cancellation(0)


@pytest.mark.parametrize("condition", [None, "If-None-Match"])
def test_cancel_is_idempotent_for_real_s3fs_precondition_mapping(
    pack_request: ModalPack,
    control_fs: AbstractFileSystem,
    monkeypatch: pytest.MonkeyPatch,
    condition: str | None,
) -> None:
    from botocore.exceptions import ClientError
    from s3fs.errors import translate_boto_error

    original = control_fs.pipe_file

    def write(path: str, value: bytes, *, mode: str) -> None:
        try:
            original(path, value, mode=mode)
        except FileExistsError:
            detail = {"Code": "PreconditionFailed", "Message": "existing record"}
            if condition is not None:
                detail["Condition"] = condition
            source = ClientError(
                {"Error": detail, "ResponseMetadata": {"HTTPStatusCode": 412}},
                "PutObject",
            )
            raise translate_boto_error(source) from source

    monkeypatch.setattr(control_fs, "pipe_file", write)
    channel = PackChannel(pack_request)
    channel.cancel(1, 2)
    channel.cancel(1, 5)
    assert channel.cancellation(1).cleanup_seconds == 2
    assert set(Path(control_fs._path(channel.root)).iterdir()) == {
        Path(control_fs._path(channel.root + "/cancel-1.json"))
    }


@pytest.mark.parametrize("code", ["PreconditionFailed", "412"])
def test_write_once_maps_only_confirmed_put_collision(code: str) -> None:
    from unittest.mock import Mock

    from artisan_modal.pack_control import write_once
    from botocore.exceptions import ClientError
    from s3fs.errors import translate_boto_error

    source = ClientError(
        {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": 412}},
        "PutObject",
    )
    error = translate_boto_error(source)
    fs = Mock(spec=AbstractFileSystem)
    fs.pipe_file.side_effect = error
    with pytest.raises(FileExistsError) as caught:
        write_once(fs, "s3://example/control", b"fixed")
    assert caught.value.__cause__ is error
    fs.pipe_file.assert_called_once_with(
        "s3://example/control", b"fixed", mode="create"
    )


@pytest.mark.parametrize(
    ("code", "status", "operation", "condition"),
    [
        ("AccessDenied", 403, "PutObject", None),
        ("InvalidArgument", 400, "PutObject", None),
        ("PreconditionFailed", 400, "PutObject", None),
        ("PreconditionFailed", None, "PutObject", None),
        ("PreconditionFailed", 412, "GetObject", None),
        ("PreconditionFailed", 412, "PutObject", "If-Match"),
    ],
)
def test_write_once_preserves_unrelated_s3_failures(
    code: str, status: int | None, operation: str, condition: str | None
) -> None:
    from unittest.mock import Mock

    from artisan_modal.pack_control import write_once
    from botocore.exceptions import ClientError
    from s3fs.errors import translate_boto_error

    detail = {"Code": code, "Message": "synthetic preserved error"}
    if condition is not None:
        detail["Condition"] = condition
    source = ClientError(
        {"Error": detail, "ResponseMetadata": {"HTTPStatusCode": status}}, operation
    )
    error = translate_boto_error(source)
    fs = Mock(spec=AbstractFileSystem)
    fs.pipe_file.side_effect = error
    with pytest.raises(OSError, match="synthetic preserved error") as caught:
        write_once(fs, "s3://example/control", b"fixed")
    assert caught.value is error


@pytest.mark.parametrize("with_cause", [False, True])
def test_write_once_does_not_infer_collision_from_errno_or_message(
    with_cause: bool,
) -> None:
    import errno
    from unittest.mock import Mock

    from artisan_modal.pack_control import write_once

    error = OSError(errno.EINVAL, "PreconditionFailed")
    if with_cause:
        error.__cause__ = ValueError("untrusted cause")
    fs = Mock(spec=AbstractFileSystem)
    fs.pipe_file.side_effect = error
    with pytest.raises(OSError, match="PreconditionFailed") as caught:
        write_once(fs, "s3://example/control", b"fixed")
    assert caught.value is error

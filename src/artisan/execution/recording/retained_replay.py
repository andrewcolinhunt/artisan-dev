"""Optional closed replay configuration, separate from required result evidence."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from contextlib import suppress
from typing import TYPE_CHECKING

from fsspec import AbstractFileSystem

from artisan.errors import StoreIntegrityError
from artisan.execution.compute.prepared import freeze_runtime_injections
from artisan.schemas.execution.admission import AdmissionReference
from artisan.schemas.execution.assignment import DeliveryIdentity, SealedDelivery
from artisan.schemas.execution.retained_replay import (
    RetainedReplayConfiguration,
    RetainedReplayDiagnostic,
    RetainedReplayDiagnosticRecord,
)
from artisan.storage.core.store_format import assert_retained_store
from artisan.storage.io.publication import publish_immutable_bytes
from artisan.utils.hashing import canonical_json_bytes
from artisan.utils.path import uri_join

if TYPE_CHECKING:
    from artisan.storage.io.retained_commit import RetainedCommitter


_LOGGER = logging.getLogger(__name__)


def record_replay_configuration(
    source: SealedDelivery | DeliveryIdentity,
    runtime_env: Mapping[str, str],
    *,
    admission: AdmissionReference,
    delta_root: str,
    fs: AbstractFileSystem,
) -> None:
    """Capture optional replay keys without making result acceptance depend on them."""
    try:
        publish_replay_configuration(
            capture_replay_configuration(source, runtime_env, admission=admission),
            delta_root=delta_root,
            fs=fs,
        )
    except Exception as error:
        with suppress(Exception):
            _LOGGER.warning(
                "Replay configuration unavailable for delivery %s (%s)",
                _source_delivery(source).delivery_id,
                type(error).__name__,
            )


def capture_replay_configuration(
    source: SealedDelivery | DeliveryIdentity,
    runtime_env: Mapping[str, str],
    *,
    admission: AdmissionReference,
) -> RetainedReplayConfiguration:
    """Capture exact required keys; injected values never enter durable diagnostics."""
    delivery = _source_delivery(source)
    if delivery.attempt != admission.attempt:
        msg = "Replay configuration requires the exact admitted delivery"
        raise ValueError(msg)
    return RetainedReplayConfiguration(
        delivery=delivery,
        admission_digest=admission.record_digest,
        seal_digest=source.seal_digest if isinstance(source, SealedDelivery) else None,
        status="available",
        required_runtime_keys=tuple(sorted(freeze_runtime_injections(runtime_env))),
    )


def publish_replay_configuration(
    configuration: RetainedReplayConfiguration,
    *,
    delta_root: str,
    fs: AbstractFileSystem,
) -> None:
    """Publish optional diagnostics without accepting any result or creating code."""
    assert_retained_store(delta_root, fs)
    configuration = RetainedReplayConfiguration.model_validate(configuration)
    publish_immutable_bytes(
        fs,
        _path(delta_root, configuration.delivery.delivery_id),
        canonical_json_bytes(configuration.model_dump(mode="json")),
    )


def read_replay_configuration(
    source: SealedDelivery | DeliveryIdentity,
    *,
    admission: AdmissionReference,
    delta_root: str,
    fs: AbstractFileSystem,
) -> RetainedReplayConfiguration:
    """Read exact source ownership, distinguishing missing capture from empty keys."""
    assert_retained_store(delta_root, fs)
    expected = capture_replay_configuration(source, {}, admission=admission)
    path = _path(delta_root, expected.delivery.delivery_id)
    if not fs.exists(path):
        return expected.model_copy(
            update={
                "status": "unavailable",
                "unavailable_reason": "No runtime replay configuration was captured",
            }
        )
    with fs.open(path, "rb", cache_type="none") as stream:
        encoded = stream.read()
    configuration = RetainedReplayConfiguration.model_validate_json(encoded)
    if (
        configuration.delivery != expected.delivery
        or configuration.admission_digest != expected.admission_digest
        or configuration.seal_digest != expected.seal_digest
        or canonical_json_bytes(configuration.model_dump(mode="json")) != encoded
    ):
        msg = "Replay configuration changed its source owner or canonical encoding"
        raise StoreIntegrityError(msg)
    return configuration


def _source_delivery(source: SealedDelivery | DeliveryIdentity) -> DeliveryIdentity:
    return source.delivery if isinstance(source, SealedDelivery) else source


def _path(delta_root: str, delivery_id: str) -> str:
    return uri_join(delta_root, "_artisan", "replay", delivery_id, "configuration.json")


def record_replay_diagnostic(
    receipt: SealedDelivery | DeliveryIdentity,
    diagnostic: RetainedReplayDiagnostic,
    *,
    delta_root: str,
    fs: AbstractFileSystem,
) -> None:
    """Record optional replay selection without changing accepted result authority."""
    try:
        publish_replay_diagnostic(receipt, diagnostic, delta_root=delta_root, fs=fs)
    except Exception as error:
        with suppress(Exception):
            _LOGGER.warning(
                "Replay diagnostic unavailable for delivery %s (%s)",
                _source_delivery(receipt).delivery_id,
                type(error).__name__,
            )


def record_selected_failed_replay_diagnostic(
    delivery: DeliveryIdentity,
    diagnostic: RetainedReplayDiagnostic,
    *,
    committer: RetainedCommitter,
) -> None:
    """Record failure selection only after its completed terminal proves the source."""
    from artisan.storage.core.execution_source import (
        FailedScientificExecutionSource,
        read_execution_source,
    )

    try:
        source = read_execution_source(committer, delivery.delivery_id)
        if (
            not isinstance(source, FailedScientificExecutionSource)
            or source.unit.failed_delivery != delivery
        ):
            msg = "Replay failure has no exact completed source selection"
            raise ValueError(msg)
        publish_replay_diagnostic(
            delivery, diagnostic, delta_root=committer.delta_root, fs=committer.fs
        )
    except Exception as error:
        with suppress(Exception):
            _LOGGER.warning(
                "Replay diagnostic unavailable for delivery %s (%s)",
                delivery.delivery_id,
                type(error).__name__,
            )


def publish_replay_diagnostic(
    receipt: SealedDelivery | DeliveryIdentity,
    diagnostic: RetainedReplayDiagnostic,
    *,
    delta_root: str,
    fs: AbstractFileSystem,
) -> None:
    """Record both selected identities without changing the diagnostic seal."""
    assert_retained_store(delta_root, fs)
    record = RetainedReplayDiagnosticRecord(
        delivery=_source_delivery(receipt),
        seal_digest=receipt.seal_digest
        if isinstance(receipt, SealedDelivery)
        else None,
        diagnostic=diagnostic,
    )
    path = uri_join(
        delta_root,
        "_artisan",
        "replay",
        _source_delivery(receipt).delivery_id,
        "diagnostic.json",
    )
    publish_immutable_bytes(
        fs, path, canonical_json_bytes(record.model_dump(mode="json"))
    )


def read_replay_diagnostic(
    receipt: SealedDelivery | DeliveryIdentity,
    *,
    delta_root: str,
    fs: AbstractFileSystem,
) -> RetainedReplayDiagnostic | None:
    """Read optional diagnostic evidence, never an ordinary acceptance source."""
    assert_retained_store(delta_root, fs)
    path = uri_join(
        delta_root,
        "_artisan",
        "replay",
        _source_delivery(receipt).delivery_id,
        "diagnostic.json",
    )
    if not fs.exists(path):
        return None
    with fs.open(path, "rb", cache_type="none") as stream:
        encoded = stream.read()
    record = RetainedReplayDiagnosticRecord.model_validate_json(encoded)
    if (
        record.delivery != _source_delivery(receipt)
        or record.seal_digest
        != (receipt.seal_digest if isinstance(receipt, SealedDelivery) else None)
        or canonical_json_bytes(record.model_dump(mode="json")) != encoded
    ):
        msg = "Replay diagnostic changed its result owner or canonical encoding"
        raise StoreIntegrityError(msg)
    return record.diagnostic

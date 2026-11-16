"""Prepare exact admitted calls without giving the SDK scientific authority."""

from __future__ import annotations

from collections.abc import Callable

from artisan.orchestration.runner_api import (
    AdmittedAssignment,
    AdmittedCurator,
    AssignmentDispatch,
    AssignmentReplyContext,
    CuratorReplyContext,
    execute_admitted_assignment,
    execute_admitted_curator,
    serialize_worker_call,
)
from artisan_modal.diagnostics import sensitive_strings
from artisan_modal.software import ModalSoftware
from artisan_modal.transport import ModalCall


def validate_dispatch(dispatch: AssignmentDispatch, software: ModalSoftware) -> None:
    """Require shared S3 authority, never controller-local paths or a mirror."""
    if dispatch.storage.protocol != "s3":
        message = "Modal assignments require explicit shared S3 storage"
        raise ValueError(message)
    if any(
        not path.startswith("s3://") or not path.removeprefix("s3://").partition("/")[0]
        for path in (dispatch.delta_root, dispatch.files_root, dispatch.staging_root)
    ):
        message = "Modal store and staging roots must be explicit S3 locations"
        raise ValueError(message)
    if dispatch.application_modules != software.application_modules:
        message = "Modal dispatch differs from the qualified application initializers"
        raise ValueError(message)


def sensitive_values(dispatch: AssignmentDispatch) -> tuple[str, ...]:
    """Capture explicit injected/storage values before writing any worker reply."""

    return (
        *dispatch.runtime_env.values(),
        *sensitive_strings(dispatch.storage.options),
        *dispatch.storage.delta_options.values(),
    )


def prepare_call(
    admitted: AdmittedAssignment | AdmittedCurator,
    software: ModalSoftware,
    dispatch: AssignmentDispatch,
) -> ModalCall:
    """Serialize within retained controller ownership; every worker entry is fresh."""
    validate_dispatch(dispatch, software)
    function: Callable[..., object]
    if isinstance(admitted, AdmittedAssignment):
        if (
            admitted.attempt.software_digest != software.software_digest
            or admitted.attempt.software_cacheable != software.software_cacheable
        ):
            message = "Modal assignment differs from its admitted software"
            raise ValueError(message)
        function = execute_admitted_assignment
    else:
        function = execute_admitted_curator
    prepared = serialize_worker_call(
        function,
        compatibility=software.compatibility,
        args=(admitted, software, dispatch),
        application_modules=software.application_modules,
    )
    context: AssignmentReplyContext | CuratorReplyContext
    if isinstance(admitted, AdmittedAssignment):
        context = AssignmentReplyContext(
            attempt=admitted.attempt,
            payload_sha256=prepared.header.payload_sha256,
            sensitive_values=sensitive_values(dispatch),
        )
    else:
        context = CuratorReplyContext(
            admission=admitted.admission,
            payload_sha256=prepared.header.payload_sha256,
            sensitive_values=sensitive_values(dispatch),
        )
    return ModalCall(
        function=software.function,
        runtime=software.image.startup.runtime,
        gpu=software.gpu,
        header=prepared.header,
        payload=prepared.payload,
        reply_context=context.model_dump_json().encode(),
        preserve_working=dispatch.preserve_working,
    )

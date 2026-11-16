"""Supported provider entry points retain typed transport and ownership seams."""

from __future__ import annotations

from inspect import Parameter, signature
from typing import get_type_hints

from artisan.orchestration import runner_api
from artisan.orchestration.runner_api import (
    ApptainerRuntime,
    ContainerLauncherIdentity,
    LocalRunner,
    PreparedSIFEnvironment,
    execute_admitted_assignment,
    execute_admitted_curator,
    execute_inert_call,
    launch_inert_calls,
    serialize_worker_call,
    verify_sif_environment,
)


def test_worker_entry_points_keep_provider_signatures() -> None:
    assert list(signature(execute_admitted_assignment).parameters) == [
        "admitted",
        "software",
        "dispatch",
        "working_root",
    ]
    assert list(signature(execute_admitted_curator).parameters) == [
        "curator",
        "software",
        "dispatch",
        "working_root",
    ]
    assert "application_modules" in signature(serialize_worker_call).parameters
    assert "control" in signature(execute_inert_call).parameters
    assert {"runtime_env", "preserve_working"} <= signature(
        launch_inert_calls
    ).parameters.keys()


def test_local_provider_supports_both_scientific_and_curator_placement() -> None:
    for name in ("resolve", "resolve_curator"):
        assert "application_modules" in signature(getattr(LocalRunner, name)).parameters
    for name in ("launch", "launch_curator"):
        parameters = signature(getattr(LocalRunner, name)).parameters
        assert {"software", "dispatch"} <= parameters.keys()


def test_sif_provider_contracts_keep_typed_verification_seam() -> None:
    assert {
        "ApptainerRuntime",
        "ContainerLauncherIdentity",
        "PreparedSIFEnvironment",
        "verify_sif_environment",
    } <= set(runner_api.__all__)
    assert (
        get_type_hints(ApptainerRuntime.identity)["return"] is ContainerLauncherIdentity
    )
    assert get_type_hints(verify_sif_environment) == {
        "target": PreparedSIFEnvironment,
        "apptainer": ApptainerRuntime,
        "return": type(None),
    }
    assert (
        signature(verify_sif_environment).parameters["apptainer"].kind
        is Parameter.KEYWORD_ONLY
    )

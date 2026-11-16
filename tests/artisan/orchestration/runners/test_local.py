"""Local preparation is explicit; dispatch retains exact pack ownership."""

from __future__ import annotations

from functools import partial
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fixtures.composition_operations import Transform

from artisan.composites.base.compiler import compile_definition
from artisan.execution.assignment_transport import (
    AdmittedAssignment,
    AssignmentDispatch,
)
from artisan.execution.environments.requirements import ToolEnvironmentSelection
from artisan.execution.local_packing import LocalPackStartupError
from artisan.orchestration.runners.assignment import AssignmentLaunchError
from artisan.orchestration.runners.local import LocalRunner
from artisan.schemas.execution.admission import AdmissionReference
from artisan.schemas.execution.assignment import AssignmentAttempt, AssignmentIdentity
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.operation_config.environment import Environment
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
)
from artisan.utils.command_supervisor import OwnershipOutcome

MODULE = "artisan.orchestration.runners.local"


def _runner(tmp_path: Path, **kwargs: object) -> LocalRunner:
    return LocalRunner(
        Environment.from_pixi(tmp_path / "pixi.toml", name="worker"),
        tmp_path / "cache",
        **kwargs,
    )


def _resources(**kwargs: object) -> RunnerResources:
    return RunnerResources(cpus=1, memory_gb=4, gpus=0, **kwargs)


def test_constructor_is_declaration_only_and_resource_rejection_precedes_lookup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for function in (
        "prepare_native_environment",
        "lookup_native_environment",
        "prepare_worker_qualification",
    ):
        monkeypatch.setattr(
            f"{MODULE}.{function}",
            lambda *_args, **_kwargs: pytest.fail(
                "Declaration or unsupported request accessed software"
            ),
        )
    runner = _runner(tmp_path)
    assert not runner.cache_root.exists()
    plan = compile_definition(Transform)
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "")
    with pytest.raises(ValueError, match="GPUs are hidden"):
        runner.resolve(plan, RunnerResources(cpus=1, memory_gb=4, gpus=1))
    with pytest.raises(ValueError, match="provider-specific"):
        runner.resolve(plan, _resources(extra={"partition": "gpu"}))


@pytest.mark.parametrize("observed", [False, True])
def test_resolve_binds_requested_gpu_before_worker_or_tool_launch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, observed: bool
) -> None:
    runner = LocalRunner(cache_root=tmp_path) if observed else _runner(tmp_path)
    allocation, worker = object(), object()
    capture, bind = Mock(return_value=allocation), Mock()
    monkeypatch.setattr(f"{MODULE}.capture_native_gpu", capture)
    monkeypatch.setattr(f"{MODULE}.bind_local_software", bind)
    monkeypatch.setattr(f"{MODULE}.lookup_native_environment", lambda *a, **kw: worker)
    monkeypatch.setattr(
        f"{MODULE}.lookup_worker_qualification", lambda *a, **kw: worker
    )
    monkeypatch.setattr(f"{MODULE}.qualify_observed_worker", lambda *a, **kw: worker)
    for name in ("controller_worker_sources", "observed_controller_sources"):
        monkeypatch.setattr(f"{MODULE}.{name}", lambda *a, **kw: ((), {}))
    runner.resolve(compile_definition(Transform), RunnerResources(gpus=2))
    capture.assert_called_once_with(2)
    assert bind.call_args.kwargs["native_gpu"] is allocation


def test_prepare_deduplicates_tool_sources_and_qualifies_worker_explicitly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = _runner(tmp_path)
    tool = Environment.from_pixi(tmp_path / "pixi.toml", name="tool")
    selections = tuple(
        ToolEnvironmentSelection((name,), "command", declaration)
        for name, declaration in [
            ("first", tool),
            ("second", tool),
            ("inherited", None),
            ("worker", runner.environment),
        ]
    )
    prepared = []
    worker = object()
    monkeypatch.setattr(f"{MODULE}.select_tool_environments", lambda _: selections)
    monkeypatch.setattr(
        f"{MODULE}.prepare_native_environment",
        lambda declaration, **kwargs: prepared.append(declaration) or worker,
    )
    monkeypatch.setattr(
        f"{MODULE}.controller_worker_sources", lambda *_, **kw: (("application",), {})
    )
    qualification = Mock(return_value=worker)
    witness = Mock()
    monkeypatch.setattr(f"{MODULE}.prepare_command_witness", witness)
    monkeypatch.setattr(f"{MODULE}.prepare_worker_qualification", qualification)
    resolved = Mock()
    monkeypatch.setattr(LocalRunner, "resolve", resolved)
    runner.prepare(Transform, params={"add": 9})
    assert prepared == [runner.environment, tool]
    qualification.assert_called_once()
    witness.assert_called_once_with(runner.cache_root)
    resolved.assert_called_once()
    assert not runner.cache_root.exists()


def test_container_builder_requirements_reject_before_any_preparation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = _runner(tmp_path, tool_backend="docker")
    monkeypatch.setattr(
        f"{MODULE}.select_tool_environments",
        lambda _: (ToolEnvironmentSelection((), "command", runner.environment),),
    )
    monkeypatch.setattr(
        f"{MODULE}.prepare_native_environment",
        lambda *_args, **_kwargs: pytest.fail(
            "Invalid builder request installed a worker"
        ),
    )
    with pytest.raises(ValueError, match="explicit local Docker"):
        runner.prepare(Transform)


def test_resolve_uses_only_existing_target_and_selected_qualification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = _runner(tmp_path)
    raw, qualified, bound = object(), object(), object()
    for function in (
        "prepare_native_environment",
        "prepare_worker_qualification",
        "prepare_oci_environment",
        "prepare_sif_environment",
    ):
        monkeypatch.setattr(
            f"{MODULE}.{function}",
            lambda *_args, **_kwargs: pytest.fail("Resolve provisioned software"),
        )
    lookup = Mock(return_value=raw)
    qualification = Mock(return_value=qualified)
    binding = Mock(return_value=bound)
    monkeypatch.setattr(f"{MODULE}.lookup_native_environment", lookup)
    monkeypatch.setattr(
        f"{MODULE}.controller_worker_sources", lambda *_, **kw: (("application",), {})
    )
    monkeypatch.setattr(f"{MODULE}.lookup_worker_qualification", qualification)
    monkeypatch.setattr(f"{MODULE}.bind_local_software", binding)
    plan = compile_definition(Transform)
    assert runner.resolve(plan, _resources()) is bound
    lookup.assert_called_once_with(runner.environment, cache_root=runner.cache_root)
    qualification.assert_called_once_with(
        raw, ("application",), cache_root=runner.cache_root
    )
    assert binding.call_args.args == (plan, qualified)


def test_default_constructor_observes_nothing_and_resolve_never_provisions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for function in (
        "prepare_native_environment",
        "lookup_native_environment",
        "prepare_worker_qualification",
        "lookup_worker_qualification",
        "prepare_command_witness",
    ):
        monkeypatch.setattr(
            f"{MODULE}.{function}",
            lambda *args, **kwargs: pytest.fail(
                "Observed worker accessed prepared worker software"
            ),
        )
    sources = Mock(return_value=(("application",), {}))
    worker = object()
    qualify = Mock(return_value=worker)
    bound = object()
    binding = Mock(return_value=bound)
    monkeypatch.setattr(f"{MODULE}.observed_controller_sources", sources)
    monkeypatch.setattr(f"{MODULE}.qualify_observed_worker", qualify)
    monkeypatch.setattr(f"{MODULE}.bind_local_software", binding)
    runner = LocalRunner(cache_root=tmp_path / "cache")
    sources.assert_not_called()
    qualify.assert_not_called()
    assert runner.environment is None
    plan = compile_definition(Transform)
    assert runner.resolve(plan, _resources()) is bound
    qualify.assert_called_once_with(("application",))
    assert binding.call_args.args == (plan, worker)
    assert not runner.cache_root.exists()


def _admissions() -> tuple[AdmittedAssignment, ...]:
    plan = compile_definition(Transform)
    return tuple(
        AdmittedAssignment(
            plan,
            AdmissionReference(
                attempt=AssignmentAttempt(
                    assignment=AssignmentIdentity(
                        step_run_id="a" * 32, unit_index=index
                    ),
                    attempt=0,
                    generation=0,
                    recipe_digest=plan.recipe.digest,
                    software_digest="c" * 64,
                    input_context_digest="b" * 32,
                ),
                record_digest="d" * 32,
            ),
        )
        for index in range(2)
    )


@pytest.mark.parametrize(
    "outcome", [None, "closed", "unknown", "done-error", "cancel-error"]
)
def test_launch_submits_one_exact_pack_and_retains_startup_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, outcome: str | None
) -> None:
    runner = _runner(tmp_path)
    software = SimpleNamespace(
        worker=object(),
        compatibility=SimpleNamespace(packages=(SimpleNamespace(name="application"),)),
        software_digest="c" * 64,
        application_modules=(),
    )
    monkeypatch.setattr(f"{MODULE}.PreparedLocalBindings", SimpleNamespace)
    verified = Mock()
    monkeypatch.setattr(f"{MODULE}.verify_assignment", verified)
    monkeypatch.setattr(
        f"{MODULE}.controller_worker_sources", lambda *_, **kw: (("application",), {})
    )
    monkeypatch.setattr(
        f"{MODULE}.verify_runtime_compatibility", lambda *_args, **_kwargs: None
    )
    handle = Mock()
    handle.done.return_value = True
    handle.cancel.return_value = CancellationAcknowledgement(
        CancellationStatus.CONFIRMED
    )
    launch = Mock(return_value=(handle, handle))
    owner = Mock()
    if outcome is not None:
        if outcome == "done-error":
            handle.done.side_effect = RuntimeError("Owner polling failed")
        elif outcome == "cancel-error":
            handle.cancel.side_effect = RuntimeError("Cancellation reporting failed")
        transport_state = "unknown" if outcome == "unknown" else "closed"
        launch.side_effect = LocalPackStartupError(
            (handle,), (OwnershipOutcome(transport_state),), (owner,)
        )
    monkeypatch.setattr(f"{MODULE}.launch_local_assignments", launch)
    assignments = _admissions()
    dispatch = AssignmentDispatch(
        "store",
        "files",
        "staging",
        StorageConfig(),
        tmp_path / "workers",
        {"SELECTED": "literal value"},
    )
    if outcome is None:
        assert runner.launch(assignments, software=software, dispatch=dispatch) == (
            handle,
            handle,
        )
    else:
        with pytest.raises(AssignmentLaunchError) as raised:
            runner.launch(assignments, software=software, dispatch=dispatch)
        assert raised.value.handles == (handle,)
        assert raised.value.worker_owners == (owner,)
        assert raised.value.cancellation.status == (
            CancellationStatus.CONFIRMED
            if outcome == "closed"
            else CancellationStatus.UNKNOWN
        )
    assert verified.call_count == 2
    launch.assert_called_once()
    assert launch.call_args.kwargs["units_per_worker"] == 2
    requests = launch.call_args.args[0]
    assert tuple(request.assignment for request in requests) == assignments
    assert all(
        request.admission_dispatch is dispatch and request.sealer is None
        for request in requests
    )


@pytest.mark.parametrize("kind", ["creator", "curator"])
@pytest.mark.parametrize("mismatched", [False, True])
def test_launch_uses_resolved_application_selection_without_reopening_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str, mismatched: bool
) -> None:
    from artisan.execution.curator_worker import AdmittedCurator
    from artisan.operations.curator.merge import Merge
    from artisan.schemas.execution.curator_admission import CuratorAdmissionReference

    modules = ("selected_application",)
    software = SimpleNamespace(compatibility=object(), application_modules=modules)
    monkeypatch.setattr(f"{MODULE}.PreparedLocalBindings", SimpleNamespace)
    monkeypatch.setattr(f"{MODULE}.verify_runtime_compatibility", lambda *_: None)
    monkeypatch.setattr(f"{MODULE}.verify_assignment", lambda *_: None)
    monkeypatch.setattr(
        "artisan.execution.application_bootstrap.observed_editable_sources",
        lambda *_args: pytest.fail("Launch reopened controller sources"),
    )
    dispatch = AssignmentDispatch(
        "store",
        "files",
        "staging",
        StorageConfig(),
        tmp_path / "workers",
        application_modules=() if mismatched else modules,
    )
    runner = _runner(tmp_path)
    handle = Mock()
    if kind == "creator":
        launch = Mock(return_value=(handle,))
        monkeypatch.setattr(f"{MODULE}.launch_local_assignments", launch)
        execute = partial(
            runner.launch, _admissions(), software=software, dispatch=dispatch
        )
    else:
        launch = Mock(return_value=handle)
        monkeypatch.setattr(f"{MODULE}.LocalCuratorHandle", launch)
        admitted = AdmittedCurator(
            Merge(),
            CuratorAdmissionReference(step_run_id="a" * 32, admission_digest="b" * 32),
        )
        execute = partial(
            runner.launch_curator, admitted, software=software, dispatch=dispatch
        )
    if mismatched:
        with pytest.raises(ValueError, match="qualified software selection"):
            execute()
        launch.assert_not_called()
    else:
        assert execute() == ((handle,) if kind == "creator" else handle)
        launch.assert_called_once()


@pytest.mark.parametrize("closure", ["closed", "unknown"])
def test_curator_startup_failure_retains_exact_handle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, closure: str
) -> None:
    from artisan.execution.curator_worker import AdmittedCurator, LocalCuratorHandle
    from artisan.execution.local_assignment import PreparedLocalBindings
    from artisan.execution.local_call import LocalCallStartupError
    from artisan.operations.curator.merge import Merge
    from artisan.orchestration.runners.curator import CuratorLaunchError
    from artisan.schemas.execution.curator_admission import CuratorAdmissionReference

    reference = CuratorAdmissionReference(
        step_run_id="a" * 32, admission_digest="b" * 32
    )
    handle = Mock(spec=LocalCuratorHandle)
    handle.admission = reference
    software = Mock(spec=PreparedLocalBindings)
    software.application_modules = ()
    software.compatibility.packages = ()
    monkeypatch.setattr(f"{MODULE}.verify_runtime_compatibility", lambda *a, **kw: None)
    monkeypatch.setattr(
        f"{MODULE}.LocalCuratorHandle",
        Mock(side_effect=LocalCallStartupError(handle, OwnershipOutcome(closure))),
    )
    dispatch = AssignmentDispatch(
        str(tmp_path / "delta"),
        str(tmp_path / "files"),
        str(tmp_path / "staging"),
        StorageConfig(),
        tmp_path / "scratch",
    )
    with pytest.raises(CuratorLaunchError) as caught:
        LocalRunner(cache_root=tmp_path / "cache").launch_curator(
            AdmittedCurator(Merge(), reference), software=software, dispatch=dispatch
        )
    assert caught.value.handles == (handle,)
    assert caught.value.cancellation.status == (
        CancellationStatus.CONFIRMED
        if closure == "closed"
        else CancellationStatus.UNKNOWN
    )


def test_resolver_uses_trusted_provider_roots_for_creator_and_curator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from artisan.operations.curator.merge import Merge

    class SDK:
        pass

    class Provider(LocalRunner):
        def worker_code_roots(self) -> tuple[type, ...]:
            return (SDK,)

    runner = Provider(cache_root=tmp_path)
    worker = object()
    selected = Mock(return_value=((), {}))
    curator = Mock(return_value=worker)
    monkeypatch.setattr(f"{MODULE}.observed_controller_sources", selected)
    monkeypatch.setattr(f"{MODULE}.qualify_observed_worker", lambda *a, **kw: worker)
    monkeypatch.setattr(f"{MODULE}.bind_local_software", lambda *a, **kw: worker)
    monkeypatch.setattr(f"{MODULE}.resolve_curator_software", curator)
    plan = compile_definition(Transform)
    assert runner.resolve(plan, _resources()) is worker
    selected.assert_called_once_with(plan, (Provider, SDK), application_modules=())
    operation = Merge()
    assert runner.resolve_curator(operation, _resources()) is worker
    assert curator.call_args.args == (operation, (Provider, SDK))
    assert LocalRunner(cache_root=tmp_path).worker_code_roots() == (LocalRunner,)


@pytest.mark.parametrize("curator", [False, True])
def test_explicit_preparation_bootstraps_applications_before_definition_inspection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, curator: bool
) -> None:
    import sys

    from artisan.operations.curator import Merge
    from artisan.operations.examples import DataGenerator

    name = "explicit_prepare_application"
    (tmp_path / f"{name}.py").write_text("INITIALIZED = True\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delitem(sys.modules, name, raising=False)
    observed: list[str] = []
    runner = LocalRunner(cache_root=tmp_path / "cache")

    def inspect(*args: object, **kwargs: object) -> object:
        assert sys.modules[name].INITIALIZED
        observed.append("definition")
        return compile_definition(DataGenerator)

    def resolve(*args: object, **kwargs: object) -> None:
        assert kwargs["application_modules"] == (name,)
        assert observed == ["definition"]
        observed.append("resolved")

    monkeypatch.setattr(f"{MODULE}.capture_native_gpu", lambda *_: None)
    monkeypatch.setattr(f"{MODULE}.select_tool_environments", lambda *_: ())
    monkeypatch.setattr(
        f"{MODULE}.capture_curator_definition"
        if curator
        else f"{MODULE}.compile_definition",
        inspect,
    )
    monkeypatch.setattr(
        LocalRunner, "resolve_curator" if curator else "resolve", resolve
    )
    try:
        if curator:
            runner.prepare_curator(Merge(), application_modules=(name,))
        else:
            runner.prepare(DataGenerator, application_modules=(name,))
        assert observed == ["definition", "resolved"]
    finally:
        sys.modules.pop(name, None)

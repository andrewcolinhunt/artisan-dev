"""Exercise installed local workers, selected tools, and retained result staging."""

from __future__ import annotations

import argparse
import json
import os
import time
from concurrent.futures import CancelledError
from dataclasses import dataclass, replace
from pathlib import Path

import fsspec

from artisan.composites.base.compiled import CompiledPlan
from artisan.composites.base.compiler import compile_definition
from artisan.execution.assignment import Assignment
from artisan.execution.composition import PlanExecution
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.execution.local_assignment import (
    PreparedLocalBindings,
    bind_local_software,
    qualify_local_worker,
)
from artisan.execution.local_worker import launch_local_assignment
from artisan.execution.worker_bootstrap import PreparedProcessCall
from artisan.schemas.execution.assignment import (
    AssignmentAttempt,
    AssignmentIdentity,
    SealedDelivery,
    SuccessfulDelivery,
)
from artisan.schemas.execution.prepared_environment import PreparedNativeEnvironment
from artisan.schemas.execution.worker_transport import TRANSPORT_PACKAGES
from artisan.schemas.orchestration.step_lifecycle import CancellationStatus
from artisan.storage.io.retained_seal import (
    capture_retained_delivery,
    stage_retained_result,
)
from prepared_local_application import ApplicationMetric, TwoToolSequence


@dataclass(frozen=True)
class Seal:
    """Use the real private writer while selected worker files still exist."""

    root: str

    def __call__(
        self, completion: SuccessfulDelivery, result: PlanExecution
    ) -> SealedDelivery:
        return stage_retained_result(
            self.root,
            completion,
            result.projection,
            result.retained_artifacts,
            fs=fsspec.filesystem("file"),
        )


def _write_marker(path: str) -> int:
    return Path(path).write_text("deserialized")


@dataclass(frozen=True)
class DeserializeCanary:
    path: Path

    def __reduce__(self) -> tuple[object, tuple[str]]:
        return _write_marker, (str(self.path),)


def bootstrap_faults(
    assignment: Assignment,
    bindings: PreparedLocalBindings,
    sealer: Seal,
    work: Path,
    runtime: dict[str, str],
) -> list[str]:
    """Corrupt inert headers after serialization and require no deserialization."""
    import artisan.execution.local_worker as worker_module

    original = worker_module.serialize_worker_call
    observed = []
    for fault in (
        "python",
        "dexterity-artisan",
        "prepared-local-application",
        "payload",
    ):
        marker = work / f"unpickled-{fault}"
        canary = replace(assignment, inputs={"source": [DeserializeCanary(marker)]})

        def damaged(
            *args: object, selected_fault: str = fault, **kwargs: object
        ) -> PreparedProcessCall:
            call = original(*args, **kwargs)
            compatibility = call.header.compatibility
            if selected_fault == "payload":
                return PreparedProcessCall(call.header, call.payload + b"changed")
            if selected_fault == "python":
                compatibility = compatibility.model_copy(
                    update={"python_version": "0.0.invalid"}
                )
            else:
                packages = []
                for package in compatibility.packages:
                    if package.name == selected_fault:
                        package = package.model_copy(update={"version": "0.0.invalid"})
                    packages.append(package)
                compatibility = compatibility.model_copy(
                    update={"packages": tuple(packages)}
                )
            return PreparedProcessCall(
                call.header.model_copy(update={"compatibility": compatibility}),
                call.payload,
            )

        worker_module.serialize_worker_call = damaged
        try:
            handle = launch_local_assignment(
                canary,
                bindings,
                sealer,
                working_root=work / "workers",
                runtime_env=runtime,
            )
            try:
                handle.result(timeout=120)
            except RuntimeError as error:
                expected = {
                    "python": "prepared transport runtime",
                    "payload": "transport header",
                }.get(fault, "prepared metadata")
                assert expected in str(error)
            else:
                msg = "An incompatible bootstrap returned a successful receipt"
                raise AssertionError(msg)
            assert not marker.exists()
            observed.append(fault)
        finally:
            worker_module.serialize_worker_call = original
    return observed


def admit(plan: CompiledPlan, software_digest: str, unit: int) -> Assignment:
    """Freeze exact inputs and stable consuming slots before each launch."""
    identity = AssignmentIdentity(step_run_id="1" * 32, unit_index=unit)
    artifact = ApplicationMetric.draft({"value": 5}, "input.json").finalize()
    graph = OccurrenceGraph(boundary_invocation=identity.assignment_id)
    graph.add_input("source", artifact)
    snapshot = graph.project({}, {})
    attempt = AssignmentAttempt(
        assignment=identity,
        attempt=0,
        generation=0,
        recipe_digest=plan.recipe.digest,
        software_digest=software_digest,
        input_context_digest=snapshot.input_context_digest,
    )
    return Assignment(
        attempt,
        plan,
        {"source": [artifact]},
        input_attribution=snapshot.input_attribution,
    )


def inspect(
    receipt: SealedDelivery,
    assignment: Assignment,
    expected: int,
    *,
    selected: str = "synthetic-selected",
) -> dict[str, object]:
    """Hydrate sealed selected output after its source worker and scratch exit."""
    boundary = {
        entry.boundary: assignment.inputs[entry.boundary.role][entry.boundary.index]
        for entry in assignment.input_attribution
    }
    with capture_retained_delivery(
        receipt,
        boundary,
        expected_recipe=assignment.plan.recipe,
        fs=fsspec.filesystem("file"),
    ) as captured:
        exports = captured.projection.export_roles()
        metric = captured.artifacts[exports["metrics"][0]]
        values = metric.values
        assert values["value"] == expected
        assert values["native_selected"] == selected
        assert values["native_literal"] == "a literal $value with spaces"
        assert (
            values["preprocess_pid"]
            == values["worker_pid"]
            == values["postprocess_pid"]
        )
        assert [tool["packaging"] for tool in values["tools"]] == ["24.2", "25.0"]
        assert [tool["activation"] for tool in values["tools"]] == ["a", "b"]
        assert len({tool["python"] for tool in values["tools"]}) == 2
        for tool in values["tools"]:
            assert tool["pythonpath"] is None
            assert tool["loader"] is None
            assert tool["literal"] == "a literal $value with spaces"
            assert tool["selected"] == selected
            assert not Path(tool["scratch"]).exists()
        file = captured.artifacts[exports["files"][0]]
        assert json.loads(file.read_content())["value"] == expected
        assert len(captured.projection.associations) == 1
        assert captured.projection.associations[0].source == exports["files"][0]
        assert captured.projection.associations[0].target == exports["metrics"][0]
        return {
            "value": expected,
            "worker_python": values["worker_python"],
            "worker_pid": values["worker_pid"],
            "tools": values["tools"],
            "delivery_id": receipt.delivery.delivery_id,
        }


def assert_dead(pid: int) -> None:
    """Reject a live observed descendant after confirmed owner closure."""
    status = Path(f"/proc/{pid}/status")
    assert not status.exists() or "State:\tZ" in status.read_text()


def run(args: argparse.Namespace) -> dict[str, object]:
    """Run a fresh installed-wheel profile without provisioning during launch."""
    raw_worker = PreparedNativeEnvironment.model_validate_json(args.worker.read_bytes())
    worker = qualify_local_worker(
        raw_worker,
        [*TRANSPORT_PACKAGES, "prepared-local-application"],
        pixi=args.pixi,
        system_env=Path("/usr/bin/env"),
    )
    params = {"manifest": str(args.manifest)}
    plan = compile_definition(TwoToolSequence, params=params)
    bindings = bind_local_software(
        plan,
        worker,
        cache_root=args.cache,
        pixi=args.pixi,
        system_env=Path("/usr/bin/env"),
    )
    args.work.mkdir(parents=True, exist_ok=True)
    sealer = Seal(str(args.work / "sealed"))
    runtime = {
        "PROBE_LITERAL": "a literal $value with spaces",
        "PROBE_CREDENTIAL": "synthetic-selected",
    }
    os.environ["PYTHONPATH"] = "/unqualified/controller-python"
    os.environ["LD_LIBRARY_PATH"] = "/unqualified/controller-loader"
    assignment = admit(plan, bindings.software_digest, 0)
    receipts = [
        launch_local_assignment(
            assignment,
            bindings,
            sealer,
            working_root=args.work / "workers",
            runtime_env=runtime,
        ).result(timeout=120)
        for _ in range(2)
    ]
    evidence = [inspect(receipt, assignment, 16) for receipt in receipts]
    assert receipts[0].delivery.delivery_id != receipts[1].delivery.delivery_id
    changed_plan = compile_definition(
        TwoToolSequence, params={**params, "increment": 2}
    )
    changed = admit(changed_plan, bindings.software_digest, 1)
    changed_receipt = launch_local_assignment(
        changed,
        bindings,
        sealer,
        working_root=args.work / "workers",
        runtime_env=runtime,
    ).result(timeout=120)
    evidence.append(inspect(changed_receipt, changed, 18))
    faults = bootstrap_faults(assignment, bindings, sealer, args.work, runtime)
    marker = args.work / "descendants.json"
    held_plan = compile_definition(
        TwoToolSequence, params={**params, "hold": 60, "marker": str(marker)}
    )
    held = admit(held_plan, bindings.software_digest, 2)
    cancelled = launch_local_assignment(
        held, bindings, sealer, working_root=args.work / "workers", runtime_env=runtime
    )
    sibling_assignment = admit(plan, bindings.software_digest, 3)
    sibling = launch_local_assignment(
        sibling_assignment,
        bindings,
        sealer,
        working_root=args.work / "workers",
        runtime_env=runtime,
    )
    deadline = time.monotonic() + 90
    while not marker.exists() and time.monotonic() < deadline:
        if cancelled.done():
            cancelled.result()
        time.sleep(0.05)
    assert marker.exists()
    descendants = json.loads(marker.read_text())
    assert cancelled.cancel().status == CancellationStatus.REQUESTED
    try:
        cancelled.result(timeout=20)
    except CancelledError:
        pass
    else:
        msg = "Cancelled assignment returned a successful seal"
        raise AssertionError(msg)
    assert cancelled.cancel().status == CancellationStatus.CONFIRMED
    for pid in descendants.values():
        assert_dead(pid)
    sibling_receipt = sibling.result(timeout=120)
    evidence.append(inspect(sibling_receipt, sibling_assignment, 16))
    assert len(list((args.work / "sealed").glob("*/retained-result.json"))) == 4
    assert not list((args.work / "workers").iterdir())
    from prepared_local_application.packing import run_packing

    packing = run_packing(
        bindings, args.manifest, args.cache, args.work / "packing", runtime
    )
    return {
        "outcome": "passed",
        "packing": packing,
        "software_digest": bindings.software_digest,
        "bootstrap_faults": faults,
        "worker_compatibility": worker.compatibility.model_dump(mode="json"),
        "evidence": evidence,
        "cancellation": {"status": "confirmed", "descendants": descendants},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("worker", "manifest", "cache", "pixi", "work", "report"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    args = parser.parse_args()
    report = run(args)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {"outcome": report["outcome"], "software_digest": report["software_digest"]}
        )
    )


if __name__ == "__main__":
    main()

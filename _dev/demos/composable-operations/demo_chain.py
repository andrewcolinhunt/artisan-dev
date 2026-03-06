"""Demo: Composable Operations — pipeline.chain() end-to-end verification.

Runs several scenarios and verifies correctness at each stage:
1. Baseline: separate run() calls (transform → score)
2. Chain: same operations via pipeline.chain()
3. Intermediates mode: persist
4. Fluent builder syntax
5. Validation: curator rejection, type mismatch
6. Chain output wired to downstream step
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path


def main() -> None:
    import polars as pl
    from prefect.testing.utilities import prefect_test_harness

    from artisan.operations.examples import (
        DataGenerator,
        DataTransformer,
        MetricCalculator,
    )
    from artisan.orchestration import PipelineManager
    from artisan.schemas.enums import TablePath
    from artisan.utils.logging import configure_logging

    configure_logging()

    PASS = "\033[92mPASS\033[0m"
    FAIL = "\033[91mFAIL\033[0m"
    checks_passed = 0
    checks_failed = 0

    def check(description: str, condition: bool, detail: str = "") -> None:
        nonlocal checks_passed, checks_failed
        if condition:
            print(f"  {PASS}: {description}")
            checks_passed += 1
        else:
            msg = f"  {FAIL}: {description}"
            if detail:
                msg += f" — {detail}"
            print(msg)
            checks_failed += 1

    def get_artifact_ids_for_step(
        delta_root: Path, step_number: int, role: str
    ) -> list[str]:
        edges_path = delta_root / TablePath.EXECUTION_EDGES
        if not edges_path.exists():
            return []
        df = pl.read_delta(str(edges_path))
        exec_path = delta_root / TablePath.EXECUTIONS
        if not exec_path.exists():
            return []
        executions = pl.read_delta(str(exec_path))
        run_ids = (
            executions.filter(pl.col("origin_step_number") == step_number)
            .filter(pl.col("success") == True)  # noqa: E712
            ["execution_run_id"]
            .to_list()
        )
        result = (
            df.filter(pl.col("execution_run_id").is_in(run_ids))
            .filter(pl.col("direction") == "output")
            .filter(pl.col("role") == role)["artifact_id"]
            .to_list()
        )
        return sorted(set(result))

    with prefect_test_harness(server_startup_timeout=60):
        from prefect.settings import PREFECT_API_URL

        os.environ["PREFECT_API_URL"] = PREFECT_API_URL.value()

        # =============================================================
        # Scenario 1: Baseline — separate run() calls
        # =============================================================
        print("\n" + "=" * 70)
        print("SCENARIO 1: Baseline — separate run() calls")
        print("=" * 70)

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            pipeline = PipelineManager.create(
                name="baseline",
                delta_root=base / "delta",
                staging_root=base / "staging",
                working_root=base / "working",
            )
            output = pipeline.output

            step0 = pipeline.run(
                DataGenerator, name="generate", params={"count": 3, "seed": 42}
            )
            check("Step 0 (generate) succeeded", step0.success)

            step1 = pipeline.run(
                DataTransformer,
                name="transform",
                inputs={"dataset": output("generate", "datasets")},
                params={"scale_factor": 2.0, "variants": 1, "seed": 100},
            )
            check("Step 1 (transform) succeeded", step1.success)
            check(
                "Step 1 produced 3 datasets",
                step1.succeeded_count == 3,
                f"got {step1.succeeded_count}",
            )

            step2 = pipeline.run(
                MetricCalculator,
                name="score",
                inputs={"dataset": output("transform", "dataset")},
            )
            check("Step 2 (score) succeeded", step2.success)

            pipeline.finalize()
            check("Pipeline has 3 steps", len(pipeline) == 3)

            baseline_metric_ids = get_artifact_ids_for_step(
                base / "delta", 2, "metrics"
            )
            check(
                "Baseline produced 3 metric artifacts",
                len(baseline_metric_ids) == 3,
                f"got {len(baseline_metric_ids)}",
            )

        # =============================================================
        # Scenario 2: Chain — transform + score in one step
        # =============================================================
        print("\n" + "=" * 70)
        print("SCENARIO 2: Chain — transform + score in one step")
        print("=" * 70)

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            pipeline = PipelineManager.create(
                name="chain_test",
                delta_root=base / "delta",
                staging_root=base / "staging",
                working_root=base / "working",
            )
            output = pipeline.output

            step0 = pipeline.run(
                DataGenerator, name="generate", params={"count": 3, "seed": 42}
            )
            check("Step 0 (generate) succeeded", step0.success)

            chain = pipeline.chain(
                inputs={"dataset": output("generate", "datasets")},
                name="transform_and_score",
            )
            chain.add(
                DataTransformer,
                params={"scale_factor": 2.0, "variants": 1, "seed": 100},
            )
            chain.add(MetricCalculator)
            chain_result = chain.run()

            check(
                "Chain step succeeded",
                chain_result.success,
                f"success={chain_result.success}",
            )
            check(
                "Chain step number is 1",
                chain_result.step_number == 1,
                f"got step_number={chain_result.step_number}",
            )
            check(
                "Chain step name is custom",
                chain_result.step_name == "transform_and_score",
                f"got step_name={chain_result.step_name!r}",
            )
            check(
                "Chain output roles include 'metrics'",
                "metrics" in chain_result.output_roles,
                f"got output_roles={chain_result.output_roles}",
            )

            pipeline.finalize()
            check("Pipeline has 2 steps (not 3)", len(pipeline) == 2)

            chain_delta = base / "delta"
            chain_metric_ids = get_artifact_ids_for_step(chain_delta, 1, "metrics")
            check(
                "Chain produced 3 metric artifacts",
                len(chain_metric_ids) == 3,
                f"got {len(chain_metric_ids)}",
            )

            # Verify provenance edges
            art_edges_path = chain_delta / "provenance/artifact_edges"
            if art_edges_path.exists():
                art_edges = pl.read_delta(str(art_edges_path))
                boundary = art_edges.filter(
                    pl.col("step_boundary") == True  # noqa: E712
                )
                internal = art_edges.filter(
                    pl.col("step_boundary") == False  # noqa: E712
                )
                check(
                    "DISCARD mode: has step_boundary edges",
                    len(boundary) > 0,
                    f"got {len(boundary)}",
                )
                check(
                    "DISCARD mode: no internal edges",
                    len(internal) == 0,
                    f"got {len(internal)}",
                )
            else:
                print("  (artifact_edges table not found)")

        # =============================================================
        # Scenario 3: Chain with intermediates="persist"
        # =============================================================
        print("\n" + "=" * 70)
        print("SCENARIO 3: Chain with intermediates='persist'")
        print("=" * 70)

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            pipeline = PipelineManager.create(
                name="chain_persist",
                delta_root=base / "delta",
                staging_root=base / "staging",
                working_root=base / "working",
            )
            output = pipeline.output

            step0 = pipeline.run(
                DataGenerator, name="generate", params={"count": 2, "seed": 42}
            )
            check("Step 0 succeeded", step0.success)

            chain = pipeline.chain(
                inputs={"dataset": output("generate", "datasets")},
                intermediates="persist",
                name="persist_chain",
            )
            chain.add(
                DataTransformer,
                params={"scale_factor": 2.0, "variants": 1, "seed": 100},
            )
            chain.add(MetricCalculator)
            chain_result = chain.run()

            check(
                "Persist chain succeeded",
                chain_result.success,
                f"success={chain_result.success}",
            )

            persist_delta = base / "delta"
            edges_path = persist_delta / "provenance/artifact_edges"
            if edges_path.exists():
                art_edges = pl.read_delta(str(edges_path))
                internal = art_edges.filter(
                    pl.col("step_boundary") == False  # noqa: E712
                )
                boundary = art_edges.filter(
                    pl.col("step_boundary") == True  # noqa: E712
                )
                check(
                    "PERSIST has internal edges",
                    len(internal) > 0,
                    f"got {len(internal)}",
                )
                check(
                    "PERSIST has step_boundary edges",
                    len(boundary) > 0,
                    f"got {len(boundary)}",
                )
                print(
                    f"\n  Persist: {len(internal)} internal"
                    f" + {len(boundary)} boundary edges"
                )
            else:
                check("Artifact edges table exists for persist", False)

            pipeline.finalize()

        # =============================================================
        # Scenario 4: Fluent builder syntax
        # =============================================================
        print("\n" + "=" * 70)
        print("SCENARIO 4: Fluent builder syntax")
        print("=" * 70)

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            pipeline = PipelineManager.create(
                name="fluent",
                delta_root=base / "delta",
                staging_root=base / "staging",
                working_root=base / "working",
            )
            output = pipeline.output

            pipeline.run(
                DataGenerator, name="generate", params={"count": 2, "seed": 42}
            )

            result = (
                pipeline.chain(
                    inputs={"dataset": output("generate", "datasets")}
                )
                .add(
                    DataTransformer,
                    params={"scale_factor": 0.5, "variants": 1, "seed": 50},
                )
                .add(MetricCalculator)
                .run()
            )

            check("Fluent chain succeeded", result.success)
            check(
                "Fluent chain default name",
                result.step_name == "data_transformer_chain_metric_calculator",
                f"got {result.step_name!r}",
            )
            pipeline.finalize()

        # =============================================================
        # Scenario 5: Validation — curator rejection
        # =============================================================
        print("\n" + "=" * 70)
        print("SCENARIO 5: Validation")
        print("=" * 70)

        from artisan.operations.curator.filter import Filter
        from artisan.orchestration.chain_builder import ChainBuilder

        builder = ChainBuilder(pipeline=None)
        try:
            builder.add(Filter)
            check("Curator rejected", False, "should have raised TypeError")
        except TypeError as e:
            check(
                "Curator rejected with TypeError",
                "Curator operations" in str(e),
            )

        # =============================================================
        # Scenario 6: Chain output wired to downstream step
        # =============================================================
        print("\n" + "=" * 70)
        print("SCENARIO 6: Chain output wired to downstream step")
        print("=" * 70)

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            pipeline = PipelineManager.create(
                name="chain_downstream",
                delta_root=base / "delta",
                staging_root=base / "staging",
                working_root=base / "working",
            )
            output = pipeline.output

            pipeline.run(
                DataGenerator, name="generate", params={"count": 2, "seed": 42}
            )

            chain = pipeline.chain(
                inputs={"dataset": output("generate", "datasets")},
                name="double_transform",
            )
            chain.add(
                DataTransformer,
                params={"scale_factor": 2.0, "variants": 1, "seed": 10},
            )
            chain.add(
                DataTransformer,
                params={"scale_factor": 0.5, "variants": 1, "seed": 20},
            )
            chain_result = chain.run()
            check("Double transform chain succeeded", chain_result.success)

            step2 = pipeline.run(
                MetricCalculator,
                name="score",
                inputs={"dataset": output("double_transform", "dataset")},
            )
            check(
                "Downstream score succeeded",
                step2.success,
                f"success={step2.success}",
            )
            check(
                "Downstream score produced 2 metrics",
                step2.succeeded_count == 2,
                f"got {step2.succeeded_count}",
            )

            pipeline.finalize()
            check("Pipeline has 3 steps", len(pipeline) == 3)

        # =============================================================
        # Summary
        # =============================================================
        print("\n" + "=" * 70)
        total = checks_passed + checks_failed
        print(
            f"RESULTS: {checks_passed}/{total} checks passed, "
            f"{checks_failed} failed"
        )
        print("=" * 70)

        os.environ.pop("PREFECT_API_URL", None)

    if checks_failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

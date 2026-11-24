"""Validate standalone native execution inside a Docker container."""

from __future__ import annotations

from artisan.execution import Executor
from artisan.operations.examples import DataGenerator
from artisan.schemas import CSVArtifact


def main() -> None:
    """Generate deterministic CSV artifacts without creating a results store."""
    with Executor() as executor:
        result = executor.run(
            DataGenerator, params={"count": 3, "rows_per_file": 5, "seed": 42}
        )
        assert result.success, result.error
        datasets = result.artifacts["datasets"]
        assert len(datasets) == 3
        for dataset in datasets:
            assert isinstance(dataset, CSVArtifact)
            assert dataset.is_finalized
            assert dataset.row_count == 5
            assert dataset.content is not None
            print(f"{dataset.original_name}: {dataset.row_count} rows")  # noqa: T201
    assert executor.cleanup_complete
    print("SUCCESS")  # noqa: T201


if __name__ == "__main__":
    main()

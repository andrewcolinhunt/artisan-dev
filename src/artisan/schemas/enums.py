"""Framework-wide enumeration types.

Defines enums for cache validation, failure handling, grouping
strategies, and Delta Lake table paths.
"""

from __future__ import annotations

from enum import Enum


class CacheValidationReason(Enum):
    """Reason a cache lookup produced a miss.

    Used in ``CacheMiss`` to provide diagnostic detail about why a
    cached result could not be reused.
    """

    # Cache miss reasons
    NO_PREVIOUS_EXECUTION = "no_previous_execution"
    EXECUTION_FAILED = "execution_failed"


class FailurePolicy(Enum):
    """Policy for handling failures during step execution.

    - CONTINUE: Log failures and continue processing remaining items.
      Commit successful items and report failures in StepResult.
    - FAIL_FAST: Stop immediately on first failure.
      Raise exception, no commit performed.
    """

    CONTINUE = "continue"
    FAIL_FAST = "fail_fast"


class CachePolicy(Enum):
    """Policy controlling when a completed step qualifies as a cache hit.

    - ALL_SUCCEEDED: Cache hit only when step had zero execution failures.
      Infrastructure errors (dispatch/commit) always block caching.
    - STEP_COMPLETED: Cache hit for any completed step, regardless of
      execution failure count. Infrastructure errors still block caching.
    """

    ALL_SUCCEEDED = "all_succeeded"
    STEP_COMPLETED = "step_completed"


class GroupByStrategy(Enum):
    """Strategy for pairing multi-input artifact streams.

    Set via ``OperationDefinition.group_by`` to control how artifacts
    from different input roles are matched before dispatch.
    """

    LINEAGE = "lineage"  # Match artifacts sharing a common ancestor
    CROSS_PRODUCT = "cross_product"  # All combinations of inputs
    ZIP = "zip"  # Positional matching (1st with 1st, etc.)


class TablePath(str, Enum):
    """Delta Lake table paths relative to delta_root (framework tables only).

    Artifact content tables (data, metrics, configs, file_refs) are now
    managed by ArtifactTypeDef in the artifact type registry.

    Since TablePath is (str, Enum), members ARE their string value and
    work directly in path construction without .value.
    """

    ARTIFACT_INDEX = "artifacts/index"

    # Provenance tables
    ARTIFACT_EDGES = "provenance/artifact_edges"
    EXECUTION_EDGES = "provenance/execution_edges"

    # Orchestration tables
    EXECUTIONS = "orchestration/executions"
    STEPS = "orchestration/steps"

    @property
    def table_name(self) -> str:
        """Short table name (last path segment), used for staging filenames."""
        return self.value.rsplit("/", 1)[-1]

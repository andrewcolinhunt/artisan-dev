"""Tests for PostStepFuture compound future."""

from __future__ import annotations

from concurrent.futures import Future

import pytest

from artisan.orchestration.post_step_future import PostStepFuture
from artisan.orchestration.step_future import StepFuture
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.orchestration.output_source import OutputSource
from artisan.schemas.orchestration.step_result import StepResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_step_future(
    step_number: int,
    roles: dict[str, str | None],
    cf_future: Future | None = None,
) -> StepFuture:
    """Create a StepFuture backed by a (possibly unresolved) Future."""
    return StepFuture(
        step_number=step_number,
        step_name=f"Step{step_number}",
        output_roles=frozenset(roles),
        output_types=roles,
        future=cf_future or Future(),
    )


def _build_post_step_future(
    main_roles: dict[str, str | None],
    post_roles: dict[str, str | None],
    *,
    main_cf: Future | None = None,
    post_cf: Future | None = None,
) -> PostStepFuture:
    """Build a PostStepFuture with correct output map precedence."""
    main = _make_step_future(0, main_roles, main_cf)
    post = _make_step_future(1, post_roles, post_cf)

    output_map: dict[str, OutputReference] = {}
    output_types: dict[str, str | None] = {}

    # Main-only roles
    for role in main_roles:
        if role not in post_roles:
            output_map[role] = main.output(role)
            output_types[role] = main_roles[role]

    # Post-step roles take precedence
    for role in post_roles:
        output_map[role] = post.output(role)
        output_types[role] = post_roles[role]

    return PostStepFuture(
        main_future=main,
        post_future=post,
        output_map=output_map,
        output_types=output_types,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestOutputRouting:
    """output() routes to the correct source step."""

    def test_output_routes_to_post_step_for_shared_role(self):
        """Shared role resolves to the post-step."""
        psf = _build_post_step_future(
            main_roles={"data": "data", "metrics": "metric"},
            post_roles={"data": "data"},
        )
        ref = psf.output("data")
        assert ref.source_step == 1  # post-step

    def test_output_routes_to_main_for_exclusive_role(self):
        """Main-only role resolves to the main step."""
        psf = _build_post_step_future(
            main_roles={"data": "data", "metrics": "metric"},
            post_roles={"data": "data"},
        )
        ref = psf.output("metrics")
        assert ref.source_step == 0  # main step

    def test_output_unknown_role_raises(self):
        """ValueError for a role neither step declares."""
        psf = _build_post_step_future(
            main_roles={"data": "data"},
            post_roles={"data": "data"},
        )
        with pytest.raises(ValueError, match="not available"):
            psf.output("nonexistent")

    def test_output_error_lists_available(self):
        """Error message includes sorted available roles."""
        psf = _build_post_step_future(
            main_roles={"data": "data", "metrics": "metric"},
            post_roles={"data": "data"},
        )
        with pytest.raises(ValueError, match="data, metrics"):
            psf.output("missing")


class TestOutputRoles:
    """output_roles is the union of both steps."""

    def test_output_roles_is_union(self):
        """Union includes roles from both steps."""
        psf = _build_post_step_future(
            main_roles={"data": "data", "metrics": "metric"},
            post_roles={"data": "data", "summary": "data"},
        )
        assert psf.output_roles == frozenset({"data", "metrics", "summary"})

    def test_output_roles_disjoint(self):
        """Disjoint roles are both included."""
        psf = _build_post_step_future(
            main_roles={"metrics": "metric"},
            post_roles={"data": "data"},
        )
        assert psf.output_roles == frozenset({"data", "metrics"})


class TestDelegation:
    """result(), done, status, step_name delegate to post_future."""

    def test_result_delegates_to_post_future(self):
        """result() returns the post-step's StepResult."""
        post_cf = Future()
        expected = StepResult(
            step_name="Step1",
            step_number=1,
            success=True,
            output_roles=frozenset(["data"]),
            output_types={"data": "data"},
        )
        post_cf.set_result(expected)

        psf = _build_post_step_future(
            main_roles={"data": "data"},
            post_roles={"data": "data"},
            post_cf=post_cf,
        )
        assert psf.result() is expected

    def test_done_delegates_to_post_future(self):
        """done reflects the post-step's completion state."""
        post_cf = Future()
        psf = _build_post_step_future(
            main_roles={"data": "data"},
            post_roles={"data": "data"},
            post_cf=post_cf,
        )
        assert psf.done is False
        post_cf.set_result(
            StepResult(step_name="Step1", step_number=1, success=True)
        )
        assert psf.done is True

    def test_status_delegates_to_post_future(self):
        """status reflects the post-step's status."""
        post_cf = Future()
        psf = _build_post_step_future(
            main_roles={"data": "data"},
            post_roles={"data": "data"},
            post_cf=post_cf,
        )
        assert psf.status == "running"
        post_cf.set_result(
            StepResult(step_name="Step1", step_number=1, success=True)
        )
        assert psf.status == "completed"

    def test_step_name_delegates_to_post_future(self):
        """step_name returns the post-step's name."""
        psf = _build_post_step_future(
            main_roles={"data": "data"},
            post_roles={"data": "data"},
        )
        assert psf.step_name == "Step1"


class TestProtocolCompliance:
    """PostStepFuture satisfies the OutputSource protocol."""

    def test_satisfies_output_source(self):
        """isinstance check against OutputSource succeeds."""
        psf = _build_post_step_future(
            main_roles={"data": "data"},
            post_roles={"data": "data"},
        )
        assert isinstance(psf, OutputSource)

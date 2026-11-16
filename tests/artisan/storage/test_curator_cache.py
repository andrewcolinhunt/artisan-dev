"""Curator reuse requires complete observed reads and exact accepted sources."""

from __future__ import annotations

from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest
from fixtures.curator_cache import STAMPS, DynamicCurator, stage_observed
from fixtures.retained_acceptance import admit_case, open_committer
from fixtures.retained_curators import admit_curator
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.operations.curator.merge import Merge
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.curator_admission import (
    CuratorAdmission,
    CuratorInputMember,
    CuratorInputRole,
)
from artisan.schemas.execution.curator_cache import CuratorViewFingerprint
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.provenance.occurrence import OccurrenceAddress
from artisan.schemas.provenance.projection import (
    AcceptedCuratorOccurrence,
    AcceptedOccurrence,
)
from artisan.storage.cache.curator_dependencies import (
    CuratorReadRecorder,
    curator_views_match,
    fingerprint_curator_view,
    observe_curator_reads,
    validate_curator_cache_proof,
)
from artisan.storage.cache.curator_identity import curator_cache_key
from artisan.storage.cache.curator_lookup import lookup_curator
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.core.curator_view import CuratorView, capture_curator_view
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.retained_commit import RetainedCommitter


@pytest.fixture
def committer(tmp_path: Path) -> RetainedCommitter:
    return open_committer(
        str(tmp_path / "candidate"), LocalFileSystem(), StorageConfig(), initialize=True
    )


def _admission(committer: RetainedCommitter, owner: str = "b") -> CuratorAdmission:
    return admit_curator(committer, Merge(), {}, step_run_id=owner * 32, **STAMPS)[1]


def _store(view: CuratorView) -> ArtifactStore:
    committer = view.committer
    return ArtifactStore(
        committer.delta_root,
        fs=committer.fs,
        storage_options=committer.storage_options,
        files_root=committer.files_root,
        _view=view,
    )


@pytest.mark.parametrize("empty", [False, True])
def test_query_free_dynamic_cache_reuses_exact_source_after_later_steps(
    committer: RetainedCommitter, monkeypatch: pytest.MonkeyPatch, empty: bool
) -> None:
    operation = DynamicCurator()
    operation.params.empty = empty
    coordinator, source, reference = admit_curator(
        committer, operation, {}, step_run_id="a" * 32, **STAMPS
    )
    staged, terminal, recorder = stage_observed(coordinator, source, operation)
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    terminal = terminal.model_copy(
        update={
            "output_roles": frozenset() if empty else frozenset({"found"}),
            "output_types": {} if empty else {"found": "csv"},
        }
    )
    coordinator.finish(reference, staged.execution_run_id, terminal)
    _, current, current_ref = admit_curator(
        committer, operation, {}, step_run_id="b" * 32, step_number=9, **STAMPS
    )
    assert current.view.digest != source.view.digest
    monkeypatch.setattr(
        DynamicCurator,
        "execute_curator",
        lambda *_: pytest.fail("Cache lookup must not invoke authored code"),
    )
    hit = lookup_curator(current_ref, reader=CuratorReader(committer))
    assert hit is not None
    assert hit.source.plan.admission == reference
    assert hit.source.plan.cache_proof == recorder.proof
    if empty:
        assert not hit.source.outputs
    else:
        assert len(hit.source.output("found")) == 1
    source_state = coordinator.tracker.current_state(reference.step_run_id)
    assert source_state.to_step_result().metadata["authored"] == {
        "selected": "original-source"
    }
    _, _, changed_ref = admit_curator(
        committer,
        operation,
        {},
        step_run_id="c" * 32,
        software_digest="f" * 64,
    )
    assert lookup_curator(changed_ref, reader=CuratorReader(committer)) is None


def test_relevant_visible_content_invalidates_but_untouched_tables_do_not(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    admission = _admission(committer)
    view = CuratorView(committer, admission.view)
    with observe_curator_reads(view, admission) as reads:
        assert _store(view).load_metrics_df(["a" * 32]).is_empty()
    proof = reads.proof
    assert [item.table_path for item in proof.views] == [
        ArtifactTypeDef.get_table_path("metric")
    ]
    _, receipt = admit_case(committer, tmp_path, kind="alias")
    committer.accept(receipt)
    current = CuratorView(committer, capture_curator_view(committer))
    assert curator_views_match(proof, current)
    with observe_curator_reads(view, admission) as index_reads:
        assert _store(view).load_type_map(["a" * 32]) == {}
    assert not curator_views_match(index_reads.proof, current)
    validate_curator_cache_proof(index_reads.proof, view, admission=admission)


@pytest.mark.parametrize("query", ["content", "index", "metrics"])
def test_absent_table_reads_are_captured_and_later_appearance_misses(
    committer: RetainedCommitter, query: str
) -> None:
    table = (
        TablePath.ARTIFACT_INDEX.value
        if query == "index"
        else ArtifactTypeDef.get_table_path("metric" if query == "metrics" else "csv")
    )
    path = f"{committer.delta_root}/{table}"
    admission = _admission(committer)
    committer.fs.rm(path, recursive=True)
    # Admission records the deliberate absence; no live table check may skip it.
    admission = admission.model_copy(update={"view": capture_curator_view(committer)})
    view = CuratorView(committer, admission.view)
    store = _store(view)
    with observe_curator_reads(view, admission) as reads:
        if query == "content":
            assert store.get_artifacts_by_type(["a" * 32], "csv") == {}
        elif query == "index":
            assert store.provenance.load_type_map(["a" * 32]) == {}
        else:
            assert store.load_metrics_df(["a" * 32]).is_empty()
    assert len(reads.proof.views) == 1
    assert reads.proof.views[0].present is False
    schema = view.physical(table).collect_schema()
    pl.DataFrame(schema=schema).write_delta(path)
    assert not curator_views_match(
        reads.proof, CuratorView(committer, capture_curator_view(committer))
    )


def test_lazy_stream_reads_and_failed_consumption_cannot_escape_observation(
    committer: RetainedCommitter,
) -> None:
    admission = _admission(committer)
    view = CuratorView(committer, admission.view)

    def stream() -> Iterator[int]:
        assert _store(view).load_metrics_df(["a" * 32]).is_empty()
        yield 1

    with observe_curator_reads(view, admission) as reads:
        items = stream()
        assert not reads.views
        assert list(items) == [1]
    assert reads.proof.views
    with (
        pytest.raises(ValueError, match="stream failed"),
        observe_curator_reads(view, admission) as failed,
    ):
        assert list(stream()) == [1]
        message = "stream failed"
        raise ValueError(message)
    with pytest.raises(RuntimeError, match="not completed"):
        _ = failed.proof
    assert view._read_recorder is None


def test_external_ids_are_eligible_but_filesystem_and_hydration_are_not(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    admission = _admission(committer)
    view = CuratorView(committer, admission.view)
    with observe_curator_reads(view, admission) as reads:
        assert _store(view).filesystem is committer.fs
    assert reads.proof.ineligible_reasons == ("untracked_filesystem_access",)
    _, receipt = admit_case(committer, tmp_path, kind="file")
    committer.accept(receipt)
    supplied = RetainedReader(committer).read(receipt).support
    _, external, _ = admit_curator(
        committer, Merge(), {"files": supplied}, step_run_id="c" * 32, **STAMPS
    )
    external_view = CuratorView(committer, external.view)
    with observe_curator_reads(external_view, external) as declared:
        pass
    assert not declared.proof.ineligible_reasons
    assert not declared.proof.carrier_reads
    validate_curator_cache_proof(declared.proof, external_view, admission=external)
    with observe_curator_reads(external_view, external) as loaded:
        _store(external_view).get_artifacts_by_type(
            [item.artifact.artifact_id for item in supplied], "file_ref"
        )
    assert loaded.proof.ineligible_reasons == ("untracked_external_content",)


def test_direct_and_ancestry_views_have_distinct_proof_identity(
    committer: RetainedCommitter,
) -> None:
    view = CuratorView(committer, capture_curator_view(committer))
    table = TablePath.ARTIFACT_EDGES.value
    assert fingerprint_curator_view(view, table) != fingerprint_curator_view(
        view, table, direct=True
    )


def test_proof_rejects_changed_source_snapshot_or_definition_stamp(
    committer: RetainedCommitter,
) -> None:
    admission = _admission(committer)
    view = CuratorView(committer, admission.view)
    with observe_curator_reads(view, admission) as reads:
        pass
    for change in ({"snapshot_digest": "f" * 32}, {"definition_digest": "f" * 32}):
        with pytest.raises(StoreIntegrityError, match="admitted science or snapshot"):
            validate_curator_cache_proof(
                reads.proof.model_copy(update=change), view, admission=admission
            )
    later = admission.model_copy(
        update={
            "record": admission.record.model_copy(
                update={"step_run_id": "c" * 32, "step_number": 99}
            )
        }
    )
    assert curator_cache_key(admission) == curator_cache_key(later)


def test_unqualified_admission_and_failed_source_never_hit(
    committer: RetainedCommitter,
) -> None:
    operation = DynamicCurator()
    operation.params.fail = True
    coordinator, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="a" * 32, **STAMPS
    )
    staged, terminal, recorder = stage_observed(coordinator, admission, operation)
    assert not staged.success
    with pytest.raises(RuntimeError, match="not completed"):
        _ = recorder.proof
    assert staged.execution_run_id is not None
    coordinator.finish(reference, staged.execution_run_id, terminal)
    _, _, current_ref = admit_curator(
        committer, operation, {}, step_run_id="b" * 32, **STAMPS
    )
    assert lookup_curator(current_ref, reader=CuratorReader(committer)) is None
    _, unqualified, unqualified_ref = admit_curator(
        committer, operation, {}, step_run_id="c" * 32
    )
    assert lookup_curator(unqualified_ref, reader=CuratorReader(committer)) is None
    with (
        pytest.raises(ValueError, match="admitted code and software"),
        observe_curator_reads(CuratorView(committer, unqualified.view), unqualified),
    ):
        pytest.fail("Unqualified observation cannot begin")


def test_typed_fingerprint_preserves_binary_nanoseconds_and_row_multiset(
    committer: RetainedCommitter, monkeypatch: pytest.MonkeyPatch
) -> None:
    view = CuratorView(committer, capture_curator_view(committer))
    frame = pl.DataFrame(
        {"content": [b"\0\xff", b"\1"], "timestamp": [1, 2]},
        schema={"content": pl.Binary, "timestamp": pl.Datetime("ns")},
    )
    table = TablePath.ARTIFACT_INDEX.value
    monkeypatch.setattr(view, "scan", lambda _: frame.lazy())
    first = fingerprint_curator_view(view, table)
    monkeypatch.setattr(view, "scan", lambda _: frame.reverse().lazy())
    assert fingerprint_curator_view(view, table) == first
    changed = frame.with_columns(pl.col("timestamp") + pl.duration(nanoseconds=1))
    monkeypatch.setattr(view, "scan", lambda _: changed.lazy())
    assert fingerprint_curator_view(view, table) != first


def test_fingerprint_ignores_string_backing_buffer_order(
    committer: RetainedCommitter, monkeypatch: pytest.MonkeyPatch
) -> None:
    view = CuratorView(committer, capture_curator_view(committer))
    values = [f"artifact-{index:032d}" for index in range(10)]
    frames = [
        pl.DataFrame({"artifact_id": order, "boundary": [True] * len(order)})
        for order in (values, list(reversed(values)))
    ]
    assert frames[0].sort("artifact_id").equals(frames[1].sort("artifact_id"))
    table = TablePath.ARTIFACT_INDEX.value
    monkeypatch.setattr(view, "scan", lambda _: frames[0].lazy())
    expected = fingerprint_curator_view(view, table)
    monkeypatch.setattr(view, "scan", lambda _: frames[1].lazy())
    assert fingerprint_curator_view(view, table) == expected


@pytest.mark.parametrize(
    ("dtype", "values"),
    [
        (pl.String, [None, "", "null", 'quote"\n', "é"]),
        (pl.Binary, [None, b"", b"\0", b"\xff"]),
        (pl.Boolean, [None, False, True]),
        (pl.Int64, [None, -(2**63), 0, 2**63 - 1]),
        (pl.UInt64, [None, 0, 2**64 - 1]),
        (
            pl.Float64,
            [
                None,
                float("nan"),
                float("inf"),
                -float("inf"),
                -0.0,
                0.0,
                1.0,
                1.0000000000000002,
            ],
        ),
        (
            pl.Decimal(precision=38, scale=18),
            [None, Decimal("1.000000000000000001"), Decimal("1.000000000000000002")],
        ),
        (pl.Datetime("ns"), [None, 1, 2]),
        (pl.Duration("ns"), [None, 1, 2]),
        (pl.Date, [None, 0, 1]),
        (pl.Time, [None, 1, 2]),
    ],
)
def test_fingerprint_preserves_scalar_values_and_multiplicity(
    committer: RetainedCommitter,
    monkeypatch: pytest.MonkeyPatch,
    dtype: pl.DataType,
    values: list[object],
) -> None:
    view = CuratorView(committer, capture_curator_view(committer))
    table = TablePath.ARTIFACT_INDEX.value

    def fingerprint(selected: list[object]) -> CuratorViewFingerprint:
        frame = pl.DataFrame({"value": selected}, schema={"value": dtype})
        monkeypatch.setattr(view, "scan", lambda _: frame.lazy())
        return fingerprint_curator_view(view, table)

    singles = [fingerprint([value]) for value in values]
    assert len({item.content_digest for item in singles}) == len(values)
    assert fingerprint(values) == fingerprint(list(reversed(values)))
    assert fingerprint(values) != fingerprint([*values, values[-1]])


def test_fingerprint_binds_original_schema(
    committer: RetainedCommitter, monkeypatch: pytest.MonkeyPatch
) -> None:
    view = CuratorView(committer, capture_curator_view(committer))
    table = TablePath.ARTIFACT_INDEX.value
    frame = pl.DataFrame({"value": [1]}, schema={"value": pl.Int64})
    monkeypatch.setattr(view, "scan", lambda _: frame.lazy())
    expected = fingerprint_curator_view(view, table)
    frame = frame.cast({"value": pl.String})
    actual = fingerprint_curator_view(view, table)
    assert actual.content_digest == expected.content_digest
    assert actual != expected


def test_unsupported_view_schema_is_explicitly_ineligible(
    committer: RetainedCommitter, monkeypatch: pytest.MonkeyPatch
) -> None:
    admission = _admission(committer)
    view = CuratorView(committer, admission.view)
    table = TablePath.ARTIFACT_INDEX.value
    frame = pl.DataFrame({"nested": [[1, 2]]}).lazy()
    monkeypatch.setattr(view, "_visible", lambda _: frame)
    with observe_curator_reads(view, admission) as reads:
        view.scan(table).collect()
    assert reads.proof.ineligible_reasons == ("unsupported_view_schema",)
    assert reads.proof.views[0].content_digest is None
    assert reads.proof.views[0].table_path == table
    validate_curator_cache_proof(reads.proof, view, admission=admission)


def test_cache_identity_preserves_source_scope_and_duplicate_input_positions(
    committer: RetainedCommitter,
) -> None:
    admission = _admission(committer)
    address = OccurrenceAddress(invocation_id="producer", role="data", index=0)
    retained = AcceptedOccurrence(
        delivery_id="a" * 32, seal_digest="b" * 32, address=address
    )
    curator = AcceptedCuratorOccurrence(
        step_run_id="a" * 32, plan_digest="b" * 32, address=address
    )
    member = CuratorInputMember(
        artifact_id="c" * 32, artifact_type="csv", producers=(retained,)
    )

    def key(members: tuple[CuratorInputMember, ...]) -> str | None:
        return curator_cache_key(
            admission.model_copy(
                update={"inputs": (CuratorInputRole(role="data", members=members),)}
            )
        )

    assert key((member,)) != key((member, member))
    assert key((member,)) != key((member.model_copy(update={"producers": (curator,)}),))
    assert key((member,)) != key((member.model_copy(update={"producers": ()}),))
    assert key((member.model_copy(update={"producers": (retained, curator)}),)) == key(
        (member.model_copy(update={"producers": (curator, retained)}),)
    )
    assert admission.definition is not None
    assert curator_cache_key(admission) != curator_cache_key(
        admission.model_copy(
            update={
                "definition": admission.definition.model_copy(
                    update={"params_digest": "f" * 32}
                )
            }
        )
    )


def test_recorder_scopes_iterator_calls_without_framework_staging_reads(
    committer: RetainedCommitter,
) -> None:
    admission = _admission(committer)
    view = CuratorView(committer, admission.view)
    store = _store(view)
    recorder = CuratorReadRecorder(view, admission)
    with pytest.raises(RuntimeError, match="Unstarted"):
        recorder.finish()

    def stream() -> Iterator[int]:
        assert store.load_metrics_df(["a" * 32]).is_empty()
        yield 1

    with recorder.authored():
        items = stream()
    assert not recorder.views
    with recorder.authored():
        assert next(items) == 1
    # The serializer performs this read between authored iterator calls.
    store.load_type_map(["a" * 32])
    with pytest.raises(StopIteration), recorder.authored():
        next(items)
    with recorder.authored():
        items.close()
    proof = recorder.finish()
    assert [item.table_path for item in proof.views] == [
        ArtifactTypeDef.get_table_path("metric")
    ]
    assert recorder.finish() == proof
    with pytest.raises(RuntimeError, match="unfinished recorder"), recorder.authored():
        pytest.fail("Finished recorder must not reopen")


def test_recorder_captures_authored_close_and_rejects_failed_finish(
    committer: RetainedCommitter,
) -> None:
    admission = _admission(committer)
    view = CuratorView(committer, admission.view)
    recorder = CuratorReadRecorder(view, admission)

    def stream() -> Iterator[int]:
        try:
            yield 1
        finally:
            _store(view).load_metrics_df(["a" * 32])

    with recorder.authored():
        items = stream()
        next(items)
    with recorder.authored():
        items.close()
    assert recorder.views == {(ArtifactTypeDef.get_table_path("metric"), False)}
    with pytest.raises(ValueError, match="authored failure"), recorder.authored():
        message = "authored failure"
        raise ValueError(message)
    with pytest.raises(RuntimeError, match="failed curator observation"):
        recorder.finish()
    assert view._read_recorder is None


@pytest.mark.parametrize("source_cacheable", [False, True])
def test_intrinsic_cacheability_is_bound_for_source_and_current_admission(
    committer: RetainedCommitter,
    monkeypatch: pytest.MonkeyPatch,
    source_cacheable: bool,
) -> None:
    monkeypatch.setattr(DynamicCurator, "cacheable", source_cacheable)
    operation = DynamicCurator()
    coordinator, source, reference = admit_curator(
        committer, operation, {}, step_run_id="a" * 32, **STAMPS
    )
    staged, terminal, _ = stage_observed(coordinator, source, operation)
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    coordinator.finish(reference, staged.execution_run_id, terminal)
    monkeypatch.setattr(DynamicCurator, "cacheable", not source_cacheable)
    _, current, current_ref = admit_curator(
        committer, operation, {}, step_run_id="b" * 32, **STAMPS
    )
    assert source.cacheable is source_cacheable
    assert current.cacheable is not source_cacheable
    assert curator_cache_key(source) != curator_cache_key(current)
    assert lookup_curator(current_ref, reader=CuratorReader(committer)) is None


def test_worker_rejects_changed_intrinsic_cacheability_before_authored_call(
    committer: RetainedCommitter, monkeypatch: pytest.MonkeyPatch
) -> None:
    operation = DynamicCurator()
    coordinator, admission, _ = admit_curator(
        committer, operation, {}, step_run_id="a" * 32, **STAMPS
    )
    monkeypatch.setattr(DynamicCurator, "cacheable", False)
    monkeypatch.setattr(
        DynamicCurator,
        "execute_curator",
        lambda *_: pytest.fail("Changed intrinsic policy must fail before author code"),
    )
    with pytest.raises(ValueError, match="exact admitted configuration"):
        stage_observed(coordinator, admission, operation)


@pytest.mark.parametrize(
    ("source_cacheable", "current_cacheable"),
    [(False, True), (True, False), (False, False)],
)
def test_software_cache_eligibility_does_not_replace_integrity_or_intrinsic_policy(
    committer: RetainedCommitter, source_cacheable: bool, current_cacheable: bool
) -> None:
    operation = DynamicCurator()
    coordinator, source, reference = admit_curator(
        committer,
        operation,
        {},
        step_run_id="a" * 32,
        software_cacheable=source_cacheable,
        **STAMPS,
    )
    staged, terminal, recorder = stage_observed(coordinator, source, operation)
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    coordinator.finish(reference, staged.execution_run_id, terminal)
    _, current, current_ref = admit_curator(
        committer,
        operation,
        {},
        step_run_id="b" * 32,
        software_cacheable=current_cacheable,
        **STAMPS,
    )
    assert source.cacheable
    assert current.cacheable
    assert (
        source.software_digest == current.software_digest == STAMPS["software_digest"]
    )
    assert recorder.proof.software_digest == source.software_digest
    assert (curator_cache_key(source) == curator_cache_key(current)) == (
        source_cacheable == current_cacheable
    )
    assert lookup_curator(current_ref, reader=CuratorReader(committer)) is None

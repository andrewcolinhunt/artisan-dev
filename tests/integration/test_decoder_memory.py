"""Native decoder RSS for legacy pages larger than the returned-batch budget."""

from __future__ import annotations

import hashlib
import json
import os
import random
import resource
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from artisan.storage.io.bounded_io import BATCH_BYTES, iter_parquet_frames

pytestmark = pytest.mark.integration

_MIB = 1024 * 1024
_VALUES = 32
_LARGE_GROUPS = 16


def _peak_bytes():
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(peak if sys.platform == "darwin" else peak * 1024)


def _prepare(root, encoding):
    """Generate legacy geometry outside every measured reader process."""
    root.mkdir(parents=True)
    values = [random.Random(index).randbytes(_MIB) for index in range(_VALUES)]
    if encoding == "dictionary":
        values *= 2
    table = pa.table({"id": range(len(values)), "content": values})
    manifest = {}
    for groups in (1, _LARGE_GROUPS):
        digest = hashlib.sha256()
        path = root / f"{groups}.parquet"
        with pq.ParquetWriter(
            path,
            table.schema,
            compression=None,
            use_dictionary=["content"] if encoding == "dictionary" else False,
            dictionary_pagesize_limit=64 * _MIB,
            data_page_size=128 * _MIB,
            write_batch_size=len(values),
        ) as writer:
            for _ in range(groups):
                writer.write_table(table, row_group_size=len(values))
                for value in values:
                    digest.update(value)
        with pq.ParquetFile(path) as parquet:
            columns = [
                parquet.metadata.row_group(index).column(1) for index in range(groups)
            ]
            # A whole uncompressed column chunk bounds its largest page/dictionary.
            # No page-header parser or assumed equality with the batch budget is needed.
            region = max(column.total_uncompressed_size for column in columns)
            dictionaries = [
                column.data_page_offset - column.dictionary_page_offset
                for column in columns
                if column.has_dictionary_page
            ]
        manifest[str(groups)] = {
            "rows": groups * len(values),
            "payload_bytes": groups * len(values) * _MIB,
            "content_sha256": digest.hexdigest(),
            "id_sum": groups * sum(range(len(values))),
            "decoder_region_upper_bytes": region,
            "dictionary_bytes": max(dictionaries, default=0),
        }
    (root / "fixture.json").write_text(json.dumps(manifest))


def _measure(root, groups, projection):
    baseline = _peak_bytes()
    rows = 0
    id_sum = 0
    digest = hashlib.sha256()
    columns = ["id"] if projection == "identity" else ["id", "content"]
    for frame in iter_parquet_frames(str(root / f"{groups}.parquet"), columns=columns):
        rows += frame.height
        id_sum += frame["id"].sum()
        if projection == "content":
            for value in frame["content"]:
                digest.update(value)
    result = {
        "baseline_bytes": baseline,
        "peak_bytes": _peak_bytes(),
        "rows": rows,
        "id_sum": id_sum,
        "content_sha256": digest.hexdigest() if projection == "content" else None,
    }
    (root / f"{groups}-{projection}.json").write_text(json.dumps(result))


def _run(*arguments):
    completed = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), *map(str, arguments)],
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
        env={**os.environ, "POLARS_MAX_THREADS": "2"},
    )
    assert completed.returncode == 0, (
        completed.stdout[-3000:] + completed.stderr[-3000:]
    )


@pytest.mark.parametrize("encoding", ["dictionary", "plain_page"])
def test_legacy_decoder_rss_scales_with_largest_region_not_total_payload(
    tmp_path, request, encoding
):
    root = tmp_path / encoding
    _run("prepare", root, encoding)
    fixtures = json.loads((root / "fixture.json").read_text())
    small, large = fixtures["1"], fixtures[str(_LARGE_GROUPS)]
    region = large["decoder_region_upper_bytes"]
    assert region == small["decoder_region_upper_bytes"]
    assert region > BATCH_BYTES
    if encoding == "dictionary":
        assert large["dictionary_bytes"] > BATCH_BYTES
    measured = {}
    for projection in ("identity", "content"):
        measured[projection] = []
        for groups in (1, _LARGE_GROUPS):
            _run("measure", root, groups, projection)
            result = json.loads((root / f"{groups}-{projection}.json").read_text())
            expected = fixtures[str(groups)]
            request.node.user_properties.append(
                (
                    f"{encoding}_{groups}_{projection}_json",
                    json.dumps({**expected, **result}, sort_keys=True),
                )
            )
            assert result["rows"] == expected["rows"]
            assert result["id_sum"] == expected["id_sum"]
            if projection == "content":
                assert result["content_sha256"] == expected["content_sha256"]
            measured[projection].append(result)
    # Allow native decode copies/allocator slack proportional to the fixed legacy G.
    # A 16 MiB returned-batch budget cannot constrain a 32 MiB dictionary or page.
    for result in measured["content"]:
        assert result["peak_bytes"] - result["baseline_bytes"] < 6 * region + 32 * _MIB
    for result in measured["identity"]:
        assert result["peak_bytes"] - result["baseline_bytes"] < 32 * _MIB
    for results in measured.values():
        assert results[1]["peak_bytes"] - results[0]["peak_bytes"] < 2 * region
    assert large["payload_bytes"] - small["payload_bytes"] > 10 * region


if __name__ == "__main__":
    mode, directory, *arguments = sys.argv[1:]
    if mode == "prepare":
        _prepare(Path(directory), arguments[0])
    else:
        _measure(Path(directory), int(arguments[0]), arguments[1])

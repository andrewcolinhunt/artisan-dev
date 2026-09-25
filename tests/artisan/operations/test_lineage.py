"""Opt-in filename matching has strict, predictable ambiguity behavior."""

from __future__ import annotations

import pytest

from artisan.operations.lineage import match_outputs_to_inputs_by_stem


@pytest.mark.parametrize(
    ("outputs", "candidates", "expected"),
    [
        (["sample.csv"], [("sample.dat", "a")], ["a"]),
        (
            ["sample_extended_out.csv"],
            [("sample", "a"), ("sample_extended", "b")],
            ["b"],
        ),
        (["/out/sample_results.tar.gz"], [("/inputs/sample.csv.gz", "a")], ["a"]),
        (
            ["sample.csv"],
            [("sample.dat", "a"), ("sample.json", "a"), ("sample.dat", "a")],
            ["a"],
        ),
        (
            ["sample_two.csv", "sample_one.csv", "sample_one.csv"],
            [("sample.csv", "a")],
            ["a", "a", "a"],
        ),
        (
            ["a" * 32 + "_head.csv"],
            [("/input/" + "a" * 32 + ".csv", "a" * 32)],
            ["a" * 32],
        ),
        (["design_10_out.csv"], [("design_1.csv", "a"), ("design_10.csv", "b")], ["b"]),
        ([], [], []),
    ],
)
def test_exact_prefix_compound_extension_and_one_to_many_matching(
    outputs, candidates, expected
) -> None:
    assert match_outputs_to_inputs_by_stem(outputs, candidates) == expected


@pytest.mark.parametrize("output", ["design_10.csv", "design_100_more.csv"])
def test_digit_boundary_does_not_match_a_shorter_number(output: str) -> None:
    with pytest.raises(ValueError, match=output):
        match_outputs_to_inputs_by_stem([output], [("design_1.csv", "a")])


@pytest.mark.parametrize("output", ["sample_extended.csv", "sample_extended_out.csv"])
def test_ambiguous_best_level_never_falls_back(output: str) -> None:
    candidates = [
        ("sample.csv", "short"),
        ("sample_extended.csv", "a"),
        ("sample_extended.json", "b"),
    ]
    with pytest.raises(ValueError, match="Ambiguous.*sample_extended"):
        match_outputs_to_inputs_by_stem([output], candidates)


@pytest.mark.parametrize("candidates", [[], [("different.csv", "a")]])
def test_unmatched_output_reports_name(candidates) -> None:
    with pytest.raises(ValueError, match="No lineage match.*unmatched"):
        match_outputs_to_inputs_by_stem(["unmatched.csv"], candidates)

"""Optional matching helpers called explicitly by operation authors."""

from __future__ import annotations

from collections.abc import Sequence

from artisan.utils.filename import strip_extensions

__all__ = ["match_outputs_to_inputs_by_stem"]


def match_outputs_to_inputs_by_stem(
    output_names: Sequence[str],
    input_names: Sequence[tuple[str, str]],
) -> list[str]:
    """Match output names to explicitly supplied input candidates.

    Try exact stems, then longest prefixes whose next character is not a digit.
    Repeated identical candidates are harmless. An ambiguous matching level
    never falls through to a shorter prefix.

    Args:
        output_names: Output names in the order to match.
        input_names: Candidate ``(name, artifact_id)`` pairs chosen by the author.

    Returns:
        One input artifact ID per output name, in output order.

    Raises:
        ValueError: If an output has no match or its best match is ambiguous.
    """
    stem_index: dict[str, set[str]] = {}
    for name, artifact_id in input_names:
        stem_index.setdefault(strip_extensions(name), set()).add(artifact_id)
    return [_match_name(name, stem_index) for name in output_names]


def _match_name(output_name: str, stem_index: dict[str, set[str]]) -> str:
    """Match the first eligible stem level, preserving strict ambiguity errors."""
    stem = strip_extensions(output_name)
    prefixes = [
        stem,
        *(stem[:i] for i in range(len(stem) - 1, 0, -1) if not stem[i].isdigit()),
    ]
    for prefix in prefixes:
        candidates = stem_index.get(prefix)
        if candidates:
            if len(candidates) != 1:
                msg = f"Ambiguous lineage match for output {output_name!r} at stem {prefix!r}"
                raise ValueError(msg)
            return next(iter(candidates))
    msg = f"No lineage match for output {output_name!r}"
    raise ValueError(msg)

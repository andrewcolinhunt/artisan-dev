"""Tests for ``_extract_arg_descriptions``.

Covers both ``Attributes:`` and ``Args:`` sections, collision precedence,
empty docstrings, and multi-line continuations.
"""

from __future__ import annotations

from pydantic import BaseModel

from artisan.operations.base._param_docs import _extract_arg_descriptions


class TestAttributesSection:
    """``Attributes:`` is the recommended style for Pydantic Params classes."""

    def test_attributes_section_extracted(self) -> None:
        class M(BaseModel):
            """Header.

            Attributes:
                alpha: First field.
                beta: Second field.
            """

            alpha: int = 0
            beta: str = "x"

        assert _extract_arg_descriptions(M) == {
            "alpha": "First field.",
            "beta": "Second field.",
        }

    def test_handles_multiline_descriptions(self) -> None:
        class M(BaseModel):
            """Header.

            Attributes:
                gamma: A field whose description
                    continues onto a second indented line.
            """

            gamma: int = 0

        descriptions = _extract_arg_descriptions(M)
        assert "gamma" in descriptions
        assert "continues onto a second indented line" in descriptions["gamma"]


class TestArgsSection:
    """``Args:`` is accepted for cross-ecosystem compatibility."""

    def test_args_section_extracted(self) -> None:
        class M(BaseModel):
            """Header.

            Args:
                alpha (int): First field.
                beta (str): Second field.
            """

            alpha: int = 0
            beta: str = "x"

        assert _extract_arg_descriptions(M) == {
            "alpha": "First field.",
            "beta": "Second field.",
        }


class TestCollisionPrecedence:
    """``Attributes:`` wins on collision (matches class-field semantics)."""

    def test_both_sections_attributes_wins_collision(self) -> None:
        class M(BaseModel):
            """Header.

            Args:
                alpha (int): From Args section.

            Attributes:
                alpha: From Attributes section.
            """

            alpha: int = 0

        assert _extract_arg_descriptions(M) == {"alpha": "From Attributes section."}

    def test_args_only_field_kept_when_attributes_lacks_it(self) -> None:
        class M(BaseModel):
            """Header.

            Args:
                alpha (int): Only in Args.

            Attributes:
                beta: Only in Attributes.
            """

            alpha: int = 0
            beta: int = 0

        assert _extract_arg_descriptions(M) == {
            "alpha": "Only in Args.",
            "beta": "Only in Attributes.",
        }


class TestEmpty:
    """Graceful behavior when no descriptions are available."""

    def test_empty_docstring_returns_empty_dict(self) -> None:
        class M(BaseModel):
            alpha: int = 0

        assert _extract_arg_descriptions(M) == {}

    def test_summary_only_docstring_returns_empty_dict(self) -> None:
        class M(BaseModel):
            """A one-liner without any sections."""

            alpha: int = 0

        assert _extract_arg_descriptions(M) == {}

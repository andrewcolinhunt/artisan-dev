"""Tests for artisan.utils.errors."""

from __future__ import annotations

from artisan.utils.errors import format_error


class TestFormatError:
    """Tests for format_error()."""

    def test_includes_traceback(self) -> None:
        """Raised exception includes full traceback text."""
        try:
            raise ValueError("test error")
        except ValueError as exc:
            result = format_error(exc)

        assert "Traceback (most recent call last)" in result
        assert "ValueError: test error" in result

    def test_truncates_long_traceback(self) -> None:
        """Exception with >50-line traceback gets truncated with prefix."""
        # Build a chain of distinct frames so Python can't collapse them
        funcs: dict[str, object] = {}
        for i in range(60):
            if i == 0:
                exec(f"def f{i}(): raise RuntimeError('deep')", funcs)
            else:
                exec(f"def f{i}(): f{i - 1}()", funcs)

        try:
            funcs[f"f{59}"]()
        except RuntimeError as exc:
            result = format_error(exc)

        lines = result.splitlines()
        assert lines[0] == "[truncated]"
        assert len(lines) == 51  # [truncated] + 50 lines

    def test_chained_exceptions(self) -> None:
        """Chained exception via 'from' includes cause."""
        try:
            try:
                raise KeyError("original")
            except KeyError as cause:
                raise ValueError("wrapped") from cause
        except ValueError as exc:
            result = format_error(exc)

        assert "KeyError" in result
        assert "ValueError: wrapped" in result

    def test_no_traceback(self) -> None:
        """Exception without __traceback__ returns type+message."""
        exc = ValueError("no traceback")
        result = format_error(exc)
        assert result == "ValueError: no traceback"

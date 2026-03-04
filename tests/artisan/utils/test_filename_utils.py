"""Unit tests for filename module."""

from __future__ import annotations

import pytest

from artisan.utils.filename import strip_extensions


class TestStripExtensionsBasic:
    """Tests for basic extension stripping behavior."""

    def test_should_strip_single_extension(self):
        """Test stripping a single extension."""
        assert strip_extensions("file.txt") == "file"

    def test_should_strip_all_extensions_by_default(self):
        """Test that all extensions are stripped by default."""
        assert strip_extensions("file.tar.gz") == "file"

    def test_should_strip_compound_extensions(self):
        """Test stripping compound extensions like .tar.gz."""
        assert strip_extensions("archive.tar.bz2") == "archive"

    def test_should_handle_no_extension(self):
        """Test filename with no extension."""
        assert strip_extensions("filename") == "filename"

    def test_should_handle_dotfile(self):
        """Test hidden files starting with dot."""
        assert strip_extensions(".gitignore") == ".gitignore"

    def test_should_handle_dotfile_with_extension(self):
        """Test hidden files with extensions."""
        assert strip_extensions(".config.json") == ".config"


class TestStripExtensionsStripAllFalse:
    """Tests for strip_all=False behavior (single extension only)."""

    def test_should_strip_only_final_extension(self):
        """Test that only the final extension is stripped."""
        assert strip_extensions("file.tar.gz", strip_all=False) == "file.tar"

    def test_should_strip_single_extension(self):
        """Test single extension behavior matches strip_all=True."""
        assert strip_extensions("file.txt", strip_all=False) == "file"

    def test_should_handle_triple_extension(self):
        """Test with triple extension."""
        assert strip_extensions("data.csv.gz.bak", strip_all=False) == "data.csv.gz"

    def test_should_handle_no_extension(self):
        """Test filename with no extension."""
        assert strip_extensions("filename", strip_all=False) == "filename"


class TestStripExtensionsWithSuffixes:
    """Tests for suffix stripping behavior."""

    def test_should_strip_single_suffix(self):
        """Test stripping a single suffix."""
        result = strip_extensions("sample_refined.dat", suffixes_to_strip=["_refined"])
        assert result == "sample"

    def test_should_strip_suffix_in_order(self):
        """Test that suffixes are stripped in order."""
        result = strip_extensions(
            "data_processed_cleaned.csv",
            suffixes_to_strip=["_cleaned", "_processed"],
        )
        assert result == "data"

    def test_should_strip_only_matching_suffix(self):
        """Test that non-matching suffixes are not stripped."""
        result = strip_extensions(
            "sample_processed.dat",
            suffixes_to_strip=["_refined", "_normalized"],
        )
        assert result == "sample_processed"

    def test_should_strip_suffix_only_once(self):
        """Test that each suffix is only stripped once."""
        result = strip_extensions(
            "file_refined_refined.dat",
            suffixes_to_strip=["_refined"],
        )
        assert result == "file_refined"

    def test_should_handle_empty_suffix_list(self):
        """Test with empty suffix list."""
        result = strip_extensions("file.txt", suffixes_to_strip=[])
        assert result == "file"

    def test_should_combine_extension_and_suffix_stripping(self):
        """Test combining extension stripping with suffix stripping."""
        # Note: _processed is in the middle, not at the end, so it's not stripped
        result = strip_extensions(
            "ABC1_processed_000.dat.gz",
            suffixes_to_strip=["_000"],
        )
        assert result == "ABC1_processed"


class TestStripExtensionsWithPaths:
    """Tests for handling full paths."""

    def test_should_extract_filename_from_path(self):
        """Test that only filename is processed, not full path."""
        assert strip_extensions("/path/to/file.txt") == "file"

    def test_should_handle_relative_path(self):
        """Test with relative path."""
        assert strip_extensions("./data/file.csv") == "file"

    def test_should_handle_nested_path(self):
        """Test with deeply nested path."""
        assert strip_extensions("/a/b/c/d/file.tar.gz") == "file"

    def test_should_preserve_filename_only(self):
        """Test that path components with dots don't affect result."""
        # The path has dots but they shouldn't affect stripping
        assert strip_extensions("/path.with.dots/file.txt") == "file"


class TestStripExtensionsEdgeCases:
    """Tests for edge cases and unusual inputs."""

    def test_should_handle_multiple_consecutive_dots(self):
        """Test filename with consecutive dots."""
        assert strip_extensions("file..txt") == "file."

    def test_should_handle_trailing_dot(self):
        """Test filename with trailing dot - Path preserves it."""
        # Path("file.").stem == "file." (trailing dot is not an extension)
        assert strip_extensions("file.") == "file."

    def test_should_handle_numeric_filename(self):
        """Test purely numeric filename."""
        assert strip_extensions("12345.dat") == "12345"

    def test_should_handle_underscore_heavy_filename(self):
        """Test filename with many underscores."""
        result = strip_extensions(
            "design_001_chain_A_processed.dat",
            suffixes_to_strip=["_processed"],
        )
        assert result == "design_001_chain_A"

    def test_should_handle_unicode_filename(self):
        """Test filename with unicode characters."""
        assert strip_extensions("résumé.dat") == "résumé"

    def test_should_handle_spaces_in_filename(self):
        """Test filename with spaces."""
        assert strip_extensions("my file.txt") == "my file"

    def test_should_handle_very_long_extension(self):
        """Test with unusually long extension."""
        assert strip_extensions("file.verylongextension") == "file"


class TestStripExtensionsCombined:
    """Tests combining strip_all and suffixes_to_strip parameters."""

    def test_should_strip_final_extension_then_suffix(self):
        """Test strip_all=False with suffix stripping."""
        result = strip_extensions(
            "sample_refined.dat.gz",
            strip_all=False,
            suffixes_to_strip=["_refined"],
        )
        # First strips .gz -> sample_refined.dat
        # Then removes _refined suffix -> sample.dat (but this is the stem)
        # Actually: stem of "sample_refined.dat.gz" with strip_all=False is "sample_refined.dat"
        # Then suffix _refined is checked against "sample_refined.dat" - doesn't end with _refined
        assert result == "sample_refined.dat"

    def test_should_work_with_strip_all_true_and_suffixes(self):
        """Test strip_all=True (default) with suffix stripping."""
        result = strip_extensions(
            "sample_refined.dat.gz",
            strip_all=True,
            suffixes_to_strip=["_refined"],
        )
        # First strips all extensions -> sample_refined
        # Then removes _refined suffix -> sample
        assert result == "sample"


class TestStripExtensionsCommonUseCases:
    """Tests for common real-world usage patterns."""

    def test_dat_processing_workflow(self):
        """Test typical data file processing pattern."""
        # Input: ABC1.dat
        # After processing: ABC1_processed.dat
        result = strip_extensions(
            "ABC1_processed.dat", suffixes_to_strip=["_processed"]
        )
        assert result == "ABC1"

    def test_numbered_output_files(self):
        """Test handling of numbered output files."""
        # Processing pipeline outputs numbered files
        result = strip_extensions(
            "design_001_scored.dat", suffixes_to_strip=["_scored"]
        )
        assert result == "design_001"

    def test_compressed_intermediate_files(self):
        """Test handling of compressed intermediate files."""
        result = strip_extensions("trajectory.xtc.gz")
        assert result == "trajectory"

    def test_chained_processing_suffixes(self):
        """Test files with multiple processing suffixes."""
        result = strip_extensions(
            "sample_cleaned_normalized.dat",
            suffixes_to_strip=["_normalized", "_cleaned"],
        )
        assert result == "sample"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

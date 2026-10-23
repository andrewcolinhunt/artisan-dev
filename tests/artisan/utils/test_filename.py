"""Unit tests for filename module."""

from __future__ import annotations

import pytest

from artisan.utils.filename import strip_extensions


@pytest.mark.parametrize("suffixes", [None, [], ["_unused"]])
@pytest.mark.parametrize("strip_all", [True, False])
@pytest.mark.parametrize(
    ("filename", "all_stem", "final_stem"),
    [
        ("/path.with.dots/file.tar.gz", "file", "file.tar"),
        (".gitignore", ".gitignore", ".gitignore"),
        (".config.json", ".config", ".config"),
        ("file..txt", "file.", "file."),
        ("file.", "file.", "file"),
        ("file...txt", "file..", "file.."),
        ("..hidden", ".", "..hidden"),
        ("filename", "filename", "filename"),
        ("", "", ""),
    ],
)
def test_suffix_configuration_does_not_change_extension_stripping(
    filename: str,
    all_stem: str,
    final_stem: str,
    strip_all: bool,
    suffixes: list[str] | None,
) -> None:
    assert strip_extensions(
        filename, strip_all=strip_all, suffixes_to_strip=suffixes
    ) == (all_stem if strip_all else final_stem)


class TestStripExtensionsBasic:
    """Tests for basic extension stripping behavior."""

    def test_should_strip_single_extension(self):
        assert strip_extensions("file.txt") == "file"

    def test_should_strip_all_extensions_by_default(self):
        assert strip_extensions("file.tar.gz") == "file"

    def test_should_strip_compound_extensions(self):
        assert strip_extensions("archive.tar.bz2") == "archive"

    def test_should_handle_no_extension(self):
        assert strip_extensions("filename") == "filename"

    def test_should_handle_dotfile(self):
        assert strip_extensions(".gitignore") == ".gitignore"

    def test_should_handle_dotfile_with_extension(self):
        assert strip_extensions(".config.json") == ".config"


class TestStripExtensionsStripAllFalse:
    """Tests for strip_all=False behavior (single extension only)."""

    def test_should_strip_only_final_extension(self):
        assert strip_extensions("file.tar.gz", strip_all=False) == "file.tar"

    def test_should_strip_single_extension(self):
        assert strip_extensions("file.txt", strip_all=False) == "file"

    def test_should_handle_triple_extension(self):
        assert strip_extensions("data.csv.gz.bak", strip_all=False) == "data.csv.gz"

    def test_should_handle_no_extension(self):
        assert strip_extensions("filename", strip_all=False) == "filename"


class TestStripExtensionsWithSuffixes:
    """Tests for suffix stripping behavior."""

    def test_should_strip_single_suffix(self):
        result = strip_extensions("sample_refined.dat", suffixes_to_strip=["_refined"])
        assert result == "sample"

    def test_should_strip_suffix_in_order(self):
        result = strip_extensions(
            "data_processed_cleaned.csv",
            suffixes_to_strip=["_cleaned", "_processed"],
        )
        assert result == "data"

    def test_should_strip_only_matching_suffix(self):
        result = strip_extensions(
            "sample_processed.dat",
            suffixes_to_strip=["_refined", "_normalized"],
        )
        assert result == "sample_processed"

    def test_should_strip_suffix_only_once(self):
        result = strip_extensions(
            "file_refined_refined.dat",
            suffixes_to_strip=["_refined"],
        )
        assert result == "file_refined"

    def test_should_handle_empty_suffix_list(self):
        result = strip_extensions("file.txt", suffixes_to_strip=[])
        assert result == "file"

    def test_should_combine_extension_and_suffix_stripping(self):
        result = strip_extensions(
            "ABC1_processed_000.dat.gz",
            suffixes_to_strip=["_000"],
        )
        assert result == "ABC1_processed"


class TestStripExtensionsWithPaths:
    """Tests for handling full paths."""

    def test_should_extract_filename_from_path(self):
        assert strip_extensions("/path/to/file.txt") == "file"

    def test_should_handle_relative_path(self):
        assert strip_extensions("./data/file.csv") == "file"

    def test_should_handle_nested_path(self):
        assert strip_extensions("/a/b/c/d/file.tar.gz") == "file"

    def test_should_preserve_filename_only(self):
        assert strip_extensions("/path.with.dots/file.txt") == "file"


class TestStripExtensionsEdgeCases:
    """Tests for edge cases and unusual inputs."""

    def test_should_handle_multiple_consecutive_dots(self):
        assert strip_extensions("file..txt") == "file."

    def test_should_handle_trailing_dot(self):
        # A trailing dot is not an extension when stripping all extensions.
        assert strip_extensions("file.") == "file."

    def test_should_handle_numeric_filename(self):
        assert strip_extensions("12345.dat") == "12345"

    def test_should_handle_underscore_heavy_filename(self):
        result = strip_extensions(
            "design_001_chain_A_processed.dat",
            suffixes_to_strip=["_processed"],
        )
        assert result == "design_001_chain_A"

    def test_should_handle_unicode_filename(self):
        assert strip_extensions("résumé.dat") == "résumé"

    def test_should_handle_spaces_in_filename(self):
        assert strip_extensions("my file.txt") == "my file"

    def test_should_handle_very_long_extension(self):
        assert strip_extensions("file.verylongextension") == "file"


class TestStripExtensionsCombined:
    """Tests combining strip_all and suffixes_to_strip parameters."""

    def test_should_strip_final_extension_then_suffix(self):
        result = strip_extensions(
            "sample_refined.dat.gz",
            strip_all=False,
            suffixes_to_strip=["_refined"],
        )
        # The remaining .dat extension prevents _refined from matching.
        assert result == "sample_refined.dat"

    def test_should_work_with_strip_all_true_and_suffixes(self):
        result = strip_extensions(
            "sample_refined.dat.gz",
            strip_all=True,
            suffixes_to_strip=["_refined"],
        )
        assert result == "sample"


class TestStripExtensionsCommonUseCases:
    """Tests for common real-world usage patterns."""

    def test_dat_processing_workflow(self):
        result = strip_extensions(
            "ABC1_processed.dat", suffixes_to_strip=["_processed"]
        )
        assert result == "ABC1"

    def test_numbered_output_files(self):
        result = strip_extensions(
            "design_001_scored.dat", suffixes_to_strip=["_scored"]
        )
        assert result == "design_001"

    def test_compressed_intermediate_files(self):
        result = strip_extensions("trajectory.xtc.gz")
        assert result == "trajectory"

    def test_chained_processing_suffixes(self):
        result = strip_extensions(
            "sample_cleaned_normalized.dat",
            suffixes_to_strip=["_normalized", "_cleaned"],
        )
        assert result == "sample"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

## Summary

Fix the release workflow so PyPI publishes succeed. The hardcoded `version` in
`pyproject.toml` was ignored by the release workflow, causing version mismatches
and duplicate upload rejections. Replaced with dynamic versioning via `hatch-vcs`.

## Changes

- Replaced static `version` field in `pyproject.toml` with `dynamic = ["version"]`
  backed by `hatch-vcs` (derives version from git tags at build time)
- Added `__version__` runtime export to the `artisan` package
- Added `src/artisan/_version.py` to `.gitignore` (auto-generated at build time)
- Added `0.1.2a3` changelog entry

## Motivation

The `v0.1.2a3` release failed because the git tag pointed to a commit where
`pyproject.toml` still had `version = "0.1.1"`. The build produced `0.1.1`
artifacts, and PyPI rejected the upload as a duplicate. Dynamic versioning
eliminates this class of error entirely — the version always matches the tag.

## Testing

- [x] Existing tests pass (`pixi run -e dev test`)
- [x] `python -m build` produces correctly versioned artifacts
- [x] `import artisan; artisan.__version__` works at runtime
- [x] Linting passes (`pixi run -e dev fmt`)

## Checklist

- [x] I have read the [Contributing Guide](../CONTRIBUTING.md)
- [x] Documentation is updated (CHANGELOG.md)

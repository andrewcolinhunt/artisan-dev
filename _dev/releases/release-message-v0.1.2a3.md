# v0.1.2a3

Fixes the release pipeline so PyPI publishes work correctly going forward.

## Highlights

- **Dynamic versioning** — package version is now derived from the git tag at
  build time via `hatch-vcs`, eliminating version mismatch errors during release
- **Runtime `__version__`** — `artisan.__version__` is now available at runtime

## Full Changelog

See [CHANGELOG.md](CHANGELOG.md) for the complete list of changes.

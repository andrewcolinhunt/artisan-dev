# Design: Rename RecordBundle → Appendable

**Date:** 2026-04-05
**Status:** Draft

---

## Motivation

"RecordBundle" is ambiguous — it sounds like a bundle of records rather than
one record in an appendable file. "Appendable" communicates the file property
(appendable/concatenatable) concisely, and the `Artifact` suffix plus the
`record_id` field convey the per-record granularity.

---

## Naming map

| Current | New |
|---------|-----|
| `RecordBundleArtifact` | `AppendableArtifact` |
| `RecordBundleTypeDef` | `AppendableTypeDef` |
| `RecordBundleGenerator` | `AppendableGenerator` |
| `ConsolidateRecordBundles` | `ConsolidateAppendables` |
| `record_bundle` (artifact type key) | `appendable` |
| `artifacts/record_bundles` (table path) | `artifacts/appendables` |
| `record_bundle_generator` (operation name) | `appendable_generator` |
| `consolidate_record_bundles` (operation name) | `consolidate_appendables` |

---

## File renames

| Current | New |
|---------|-----|
| `src/artisan/schemas/artifact/record_bundle.py` | `…/appendable.py` |
| `src/artisan/operations/examples/record_bundle_generator.py` | `…/appendable_generator.py` |
| `src/artisan/operations/curator/consolidate_record_bundles.py` | `…/consolidate_appendables.py` |
| `tests/artisan/schemas/artifact/test_record_bundle.py` | `…/test_appendable.py` |
| `tests/artisan/operations/examples/test_record_bundle_generator.py` | `…/test_appendable_generator.py` |
| `tests/artisan/operations/curator/test_consolidate_record_bundles.py` | `…/test_consolidate_appendables.py` |

---

## Content updates (by file)

### Source files

- **`src/artisan/schemas/artifact/appendable.py`** — class names, artifact_type default, docstrings, TypeDef key/table_path/model
- **`src/artisan/operations/examples/appendable_generator.py`** — class name, operation name, description, artifact_type strings, imports, error messages, docstrings
- **`src/artisan/operations/curator/consolidate_appendables.py`** — class name, operation name, description, artifact_type strings, imports, error messages, docstrings
- **`src/artisan/visualization/inspect.py`** — `"record_bundle"` → `"appendable"` in `_build_details()`

### `__init__.py` re-exports

- **`src/artisan/schemas/artifact/__init__.py`** — import path + class name + `__all__`
- **`src/artisan/schemas/__init__.py`** — import path + class name + `__all__`
- **`src/artisan/operations/examples/__init__.py`** — import path + class name + `__all__`
- **`src/artisan/operations/curator/__init__.py`** — import path + class name + `__all__`

### Test files

- **`tests/…/test_appendable.py`** — imports, class references, type string assertions
- **`tests/…/test_appendable_generator.py`** — imports, class references, type string assertions
- **`tests/…/test_consolidate_appendables.py`** — imports, class references, operation name assertions

### Documentation

- **`docs/tutorials/execution/11-external-file-storage.ipynb`** — imports, operation calls, `load_artifact_ids_by_type` strings, prose
- **`docs/tutorials/execution/12-post-step-consolidation.ipynb`** — imports, operation calls, `load_artifact_ids_by_type` strings, prose

### Design doc

- **`_dev/design/0_current/external-files/5-example-external-artifact-types.md`** — all code blocks and prose references

---

## Verification

```bash
~/.pixi/bin/pixi run -e dev fmt
~/.pixi/bin/pixi run -e dev test-unit
~/.pixi/bin/pixi run -e docs docs-build
```

# Design: Rename RecordBundle → AppendableFileRecord

**Date:** 2026-04-05
**Status:** Draft

---

## Motivation

"RecordBundle" is ambiguous — it sounds like a bundle of records rather than
one record in an appendable file. "AppendableFileRecord" communicates both
the file property (appendable) and the artifact granularity (one record).

---

## Naming map

| Current | New |
|---------|-----|
| `RecordBundleArtifact` | `AppendableFileRecordArtifact` |
| `RecordBundleTypeDef` | `AppendableFileRecordTypeDef` |
| `RecordBundleGenerator` | `AppendableFileRecordGenerator` |
| `ConsolidateRecordBundles` | `ConsolidateAppendableFileRecords` |
| `record_bundle` (artifact type key) | `appendable_file_record` |
| `artifacts/record_bundles` (table path) | `artifacts/appendable_file_records` |
| `record_bundle_generator` (operation name) | `appendable_file_record_generator` |
| `consolidate_record_bundles` (operation name) | `consolidate_appendable_file_records` |

---

## File renames

| Current | New |
|---------|-----|
| `src/artisan/schemas/artifact/record_bundle.py` | `…/appendable_file_record.py` |
| `src/artisan/operations/examples/record_bundle_generator.py` | `…/appendable_file_record_generator.py` |
| `src/artisan/operations/curator/consolidate_record_bundles.py` | `…/consolidate_appendable_file_records.py` |
| `tests/artisan/schemas/artifact/test_record_bundle.py` | `…/test_appendable_file_record.py` |
| `tests/artisan/operations/examples/test_record_bundle_generator.py` | `…/test_appendable_file_record_generator.py` |
| `tests/artisan/operations/curator/test_consolidate_record_bundles.py` | `…/test_consolidate_appendable_file_records.py` |

---

## Content updates (by file)

### Source files

- **`src/artisan/schemas/artifact/appendable_file_record.py`** — class names, artifact_type default, docstrings, TypeDef key/table_path/model
- **`src/artisan/operations/examples/appendable_file_record_generator.py`** — class name, operation name, description, artifact_type strings, imports, error messages, docstrings
- **`src/artisan/operations/curator/consolidate_appendable_file_records.py`** — class name, operation name, description, artifact_type strings, imports, error messages, docstrings
- **`src/artisan/visualization/inspect.py`** — `"record_bundle"` → `"appendable_file_record"` in `_build_details()`

### `__init__.py` re-exports

- **`src/artisan/schemas/artifact/__init__.py`** — import path + class name + `__all__`
- **`src/artisan/schemas/__init__.py`** — import path + class name + `__all__`
- **`src/artisan/operations/examples/__init__.py`** — import path + class name + `__all__`
- **`src/artisan/operations/curator/__init__.py`** — import path + class name + `__all__`

### Test files

- **`tests/…/test_appendable_file_record.py`** — imports, class references, type string assertions
- **`tests/…/test_appendable_file_record_generator.py`** — imports, class references, type string assertions
- **`tests/…/test_consolidate_appendable_file_records.py`** — imports, class references, operation name assertions

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

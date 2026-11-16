# Retained result storage qualification

This executable prototype tests a closed retained-result format and a serialized
controller decision against real local and MinIO Parquet/Delta storage. It does
not activate a production format or replace the production committer.

Run from the repository environment:

```bash
pixi run --locked -e dev pytest tests/qualification/persistence -m 'not s3'
pixi run --locked -e dev pytest tests/qualification/persistence -m s3 --require-s3
```

The S3 command requires the existing Docker/MinIO fixture or the repository's
configured S3 test endpoint. An unavailable backend is an incomplete check.

## Candidate encoding

One delivery contains six Parquet tables and `seal.json`. The seal binds the exact
bytes, sizes and row counts of every table and external file, together with the
unit, attempt, generation, delivery, recipe, software and successful cleanup
evidence. Closed Pydantic models reject unknown fields and historical domains.

| Table | Owner-local natural key | Meaning |
| --- | --- | --- |
| `artifacts` | artifact ID | One serialized registered artifact per retained content identity |
| `occurrences` | occurrence key | Exact retained/boundary occurrence and content/type identity |
| `roles` | kind, position | Declared ordered input/export roles, including empty roles |
| `memberships` | kind, role, position | Ordered input/export/support membership; duplicates remain |
| `relations` | kind, position | Projected parents, preserved original direct associations, required references |
| `context` | key | Frozen association/configuration captures, including captured emptiness |

Every physical result row additionally carries its complete seal digest as
`logical_commit_id`. Logical plans bind exact natural keys, row counts and
canonical table hashes. Parquet row order is not export order. Explicit aliases
use their original occurrence keys; equal-content operation calls remain distinct.

The controller verifies artifact IDs through registered production models and
their `to_row()` integrity check. FileRef bytes transfer through the production
bounded immutable file publisher before the seal. Their payload uses a sealed
relative filename, so accepted hydration needs no worker scratch. External files
remain part of the same closed inventory.

## Durable decision and recovery

Exactly one controller owns the journal at a time. An atomic Delta journal append
is the winner claim point. The claim binds the active attempt/generation, selected
delivery, frozen input/context identity and complete seal digest. Cancellation
recorded first rejects an unclaimed late delivery. A claim recorded first remains
recoverable after cancellation. Another unit has its own decision.

Acceptance verifies a complete seal before claiming, retains immutable evidence,
publishes an exact plan, applies and verifies each table effect, then records
completion. Ordinary readers and cache lookup require the same complete winner.
Recovery never selects a replacement delivery or combines tables across seals.
Diagnostics require their own explicit recovery and are excluded from ordinary
recovery and cache lookup.

The tests terminate separate controller processes with `os._exit` after claim,
plan publication, every table effect, completion and cleanup. They also terminate
a worker during transfer and delete successful worker scratch before acceptance.
Recovered rows must be exact and unique; partial effects remain invisible.

## Evidence and limits

The scientific fixtures use finalized CSV artifacts as synthetic structures and
ordinary metrics. They cover prediction plus measurement, repeated operations
plus a final reducer, equal-content B/A/B export order, explicit aliases and
recursive required support. No scientific service or accelerator is needed.

The prototype reuses production immutable publication, `DiskTable` canonical
evidence, `DeltaSnapshot` schema/protocol checks and `append_table_files` Delta
transactions. It does not bypass production schema or ownership validation.
The standalone cache method proves the prototype's eligibility rule. These
checks do not qualify production acceptance, readers, or cache integration. See
the production [acceptance](../../artisan/storage/test_retained_commit.py),
[recovery](../../artisan/storage/test_retained_recovery.py), and
[pipeline persistence](../../integration/test_step_persistence.py) checks for
that coverage.

This is a small-data feasibility proof, not a memory/scaling benchmark. External
publication uses the bounded production transfer, while the candidate controller
captures its complete small fixture inventory in memory. Concurrent controllers
are outside the serialized-controller contract. Required-reference persistence
uses built-in configuration markers and explicit no-reference policies for
CSV/metric/FileRef; application annotation-policy and reader parity require their
own integration checks. A skipped outer step is a submission concern: this proof
rejects any seal with no admitted assignment and separately accepts a declared
empty optional result.

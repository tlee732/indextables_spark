# Companion Mode Test Audit — Delta vs Iceberg Gaps

**Date:** 2026-03-26 (updated with new test coverage)
**Scope:** All companion test files across `indextables_spark`, `indextables_spark_edward`, and `tantivy4java`

## Inventory

**57+ total companion test files.** Core sync/integration tests:

| Layer | Delta | Iceberg | Parquet | Format-Agnostic |
|-------|-------|---------|---------|-----------------|
| Sync integration | ~93 tests (3 files) | ~16 + **22 new** (5 files) | ~25 tests (1 file) | ~80+ tests (16 files) |
| Streaming | 2 tests | 2 tests | 0 | 0 |
| Cloud (S3/Azure) | yes | yes | yes | - |
| tantivy4java | - | - | 18 files | - |

**New test files on `companion-test-coverage` branch:**
- `IcebergMutationSyncTest.scala` — 10 tests (delete, overwrite, mixed ops, partitioned)
- `IcebergDistributedSyncTest.scala` — 12 tests (WHERE clause, scoped invalidation, TARGET INPUT SIZE)
- `CompanionMergeSplitsTest.scala` — 9 tests (Delta + Iceberg, metadata preservation, data access)
- `CompanionDropPartitionsTest.scala` — 9 tests (Delta + Iceberg, compound predicates, re-sync)
- `CompanionPurgeTest.scala` — 9 tests (Delta + Iceberg, retention, DRY RUN, data integrity)
- `CompanionTruncateTimeTravelTest.scala` — 9 tests (Delta + Iceberg, checkpoint, metadata preservation)
- `CompanionAggregatePushdownCorrectnessTest.scala` — 11 tests (Delta + Iceberg, COUNT/SUM/AVG/MIN/MAX, GROUP BY)
- `CompanionBucketAggregationTest.scala` — 9 tests (Delta + Iceberg, Histogram/DateHistogram/Range/SUM)

---

## Part 1: Delta vs Iceberg Gaps

### F1 — No Iceberg incremental sync with data mutation tests

**Severity: High** — **FIXED**

`IcebergMutationSyncTest.scala` now covers:
- File deletion (equivalent to Delta DELETE) with exact row count verification
- File overwrite (equivalent to OPTIMIZE/compaction) with invalidation + creation checks
- Mixed append + delete operations
- Large snapshot gaps (10 snapshots between syncs)
- Partitioned table append and delete with partition-specific verification

---

### F2 — No Iceberg distributed sync tests

**Severity: High** — **FIXED**

`IcebergDistributedSyncTest.scala` now covers:
- Basic build and metadata recording
- Incremental sync detecting new snapshots
- Re-sync no-op (`shouldBe "no_action"`)
- WHERE-scoped invalidation (outside and within range)
- INVALIDATE ALL PARTITIONS
- TARGET INPUT SIZE
- Partition column detection

---

### F3 — No Iceberg WHERE clause partition filtering tests

**Severity: High** — **FIXED**

`IcebergDistributedSyncTest.scala` now covers:
- WHERE with equality filter (`region = 'us-east'`)
- WHERE with IN filter (`region IN ('us-east', 'eu-west')`)
- WHERE with != filter (`region != 'us-east'`)

Not yet covered (Delta has these): range, compound AND, OR, BETWEEN, date-format columns.

---

### F4 — No Iceberg schema evolution test

**Severity: Medium** — **Open**

Still no test for Iceberg schema evolution (add/drop/rename/reorder columns).

---

### F5 — No Iceberg data type comprehensive tests

**Severity: Medium** — **Open**

Still no Iceberg equivalent of CompanionSplitTypeComprehensiveTest, CompanionRoundTripTest, or CompanionColumnarReadTest. Iceberg tests now verify exact row counts and partition values but not per-row field-level data fidelity for all types.

---

### F6 — No Iceberg TARGET INPUT SIZE tests

**Severity: Medium** — **FIXED**

`IcebergDistributedSyncTest.scala` includes TARGET INPUT SIZE test.

---

### F7 — No Iceberg batching control tests

**Severity: Medium** — **Open**

Still no test for batchSize/maxConcurrentBatches with Iceberg sources.

---

### F8 — No Iceberg compact string indexing tests

**Severity: Medium** — **Open**

Still Parquet-only in CompanionCompactStringTest.

---

### F9 — No Iceberg MERGE SPLITS correctness test

**Severity: Medium** — **FIXED**

`CompanionMergeSplitsTest.scala` now tests Iceberg merge with:
- Companion field preservation (companionSourceFiles, companionFastFieldMode)
- Data accessibility after merge (exact row count + ID verification)
- Aggregation correctness after merge (exact sum/min/max)
- Filter correctness after merge

The known bug (file:// path resolution) has been fixed — the test now expects success.

---

### F10 — No Iceberg partition evolution tests

**Severity: Medium** — **Open**

Still no test for syncing a table whose partition spec changed between snapshots.

---

### F11 — Iceberg cloud tests have lenient assertions

**Severity: Low** — **Open**

Cloud tests unchanged. New local tests have strong assertions.

---

### F12 — No Iceberg metrics validation

**Severity: Low** — **Open**

---

### F13 — No Iceberg error handling tests

**Severity: Low** — **Open**

---

### F14 — No Iceberg write guard test

**Severity: Low** — **Open**

---

### F15 — No Iceberg fast field mode tests

**Severity: Low** — **Partially fixed**

CompanionMergeSplitsTest verifies `companionFastFieldMode == Some("HYBRID")` for Iceberg. Still no test for DISABLED or PARQUET_ONLY modes with Iceberg.

---

### F16 — Edward branch divergence

**Severity: Low** — **Open**

---

### F17 — No Iceberg FROM SNAPSHOT in local tests

**Severity: Low** — **Open**

---

## Part 2: Features With Zero Tests (All Formats)

### F18 — Streaming error recovery / backoff never tested

**Severity: High** — **Open**

Still no test for exponential backoff, consecutive error limit, or quiet poll interval.

---

### F19 — WRITER HEAP SIZE clause never tested

**Severity: Medium** — **Open**

---

### F20 — Arrow FFI path in distributed companion sync never tested

**Severity: Medium** — **Open**

---

### F21 — Path normalization edge cases never tested

**Severity: Medium** — **Open**

---

### F22 — Iceberg physical-to-logical column name mapping never tested

**Severity: Medium** — **Open**

---

### F23 — Parquet companion has no streaming, MERGE SPLITS, DROP PARTITIONS, PURGE, or TRUNCATE tests

**Severity: Medium** — **Open**

---

### F24 — DESCRIBE/PREWARM/CHECKPOINT/REPAIR on companion tables never tested

**Severity: Low** — **Open**

---

### F25 — Companion sync configuration options never tested

**Severity: Low** — **Open**

---

### F26 — Concurrent companion builds never tested

**Severity: Low** — **Open**

---

### F27 — Companion metadata round-trip never directly tested

**Severity: Low** — **Open**

---

## Summary

| Severity | Total | Fixed | Open |
|----------|-------|-------|------|
| **High** | 4 | 3 (F1, F2, F3) | 1 (F18) |
| **Medium** | 12 | 3 (F6, F9, F15 partial) | 9 (F4, F5, F7, F8, F10, F19-F23) |
| **Low** | 11 | 0 | 11 (F11-F17, F24-F27) |

**7 of 27 findings fixed. 20 remain open (1 High, 9 Medium, 10 Low).**

## Updated Coverage Matrix

| Feature | Delta | Iceberg | Parquet |
|---------|-------|---------|---------|
| Basic build | 14 tests | 10 tests | 25 tests |
| Incremental sync (no-op) | yes | yes | yes |
| Incremental sync (mutations) | yes (OPTIMIZE/DELETE/UPDATE/MERGE) | **yes** (delete/overwrite/mixed) | N/A |
| Distributed sync | 27 tests | **12 tests** | **none** |
| WHERE clause filtering | 9+ tests | **yes** (=, IN, !=) | **none** |
| Streaming | 2 tests | 2 tests | **none** |
| Streaming error recovery | **none** | **none** | **none** |
| Schema evolution | yes | **none** | **none** |
| All data types | yes (comprehensive) | **none** | yes (primitives) |
| Round-trip data fidelity | 13 tests | **none** | **none** |
| Columnar read (Arrow FFI) | 20+ tests | **none** | **none** |
| TARGET INPUT SIZE | 5 tests | **yes** (1 test) | 1 test |
| Batching control | 10 tests | **none** | **none** |
| Fast field modes | yes (3 modes) | **HYBRID only** | yes (1 mode) |
| Compact string modes | **none** | **none** | yes |
| MERGE SPLITS | yes | **yes** (metadata + data + agg + filters) | **none** |
| PURGE | yes | **yes** (orphan delete, retention, DRY RUN, integrity) | **none** |
| DROP PARTITIONS | yes | **yes** (basic, correctness, re-sync, cumulative) | **none** |
| TRUNCATE TIME TRAVEL | yes | **yes** (versions, checkpoint, integrity, metadata, re-sync) | **none** |
| Bucket aggregations | yes | **yes** (Histogram, DateHistogram, Range, SUM) | **none** |
| Aggregate pushdown | yes | **yes** (COUNT, SUM, AVG, MIN, MAX, GROUP BY, filtered) | yes |
| Write guard | yes | **none** | yes |
| Error handling | yes | **none** | yes |
| Metrics | yes | **none** | **none** |
| Cloud (S3/Azure) | yes | yes | yes |
| Catalog integration | N/A | yes (REST/Glue/HMS) | N/A |
| Partition evolution | N/A | **none** | N/A |
| FROM VERSION/SNAPSHOT | yes | cloud only | N/A |
| WRITER HEAP SIZE | **none** | **none** | **none** |
| Arrow FFI distributed sync | **none** | **none** | **none** |
| Path normalization | **none** | **none** | **none** |
| Column name mapping | N/A | **none** | N/A |
| DESCRIBE/PREWARM/CHECKPOINT | **none** | **none** | **none** |
| Concurrent builds | **none** | **none** | **none** |
| Metadata round-trip (full) | **none** | **none** | **none** |

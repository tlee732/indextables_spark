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

Additional WHERE predicates added by `IcebergWhereClauseExtendedTest.scala`: range (>=, >), compound AND, OR, BETWEEN-equivalent.

---

### F4 — No Iceberg schema evolution test

**Severity: Medium** — **FIXED**

`IcebergSchemaEvolutionSyncTest.scala` covers add column, drop column, rename column, and verifying new columns are queryable via filter pushdown.

---

### F5 — No Iceberg data type comprehensive tests

**Severity: Medium** — **FIXED**

`IcebergRoundTripTest.scala` provides per-field per-row round-trip validation for scalar types (Int, Long, Double, String, Boolean), Date, Timestamp, Array, Struct, Map, and partitioned mixed-type tables.

---

### F6 — No Iceberg TARGET INPUT SIZE tests

**Severity: Medium** — **FIXED**

`IcebergDistributedSyncTest.scala` includes TARGET INPUT SIZE test.

---

### F7 — No Iceberg batching control tests

**Severity: Medium** — **FIXED**

`IcebergBatchingSyncTest.scala` covers batchSize=1, maxConcurrentBatches=1, default vs batchSize=1 comparison, and incremental sync with batching.

---

### F8 — No Iceberg compact string indexing tests

**Severity: Medium** — **FIXED**

`IcebergCompactStringTest.scala` covers exact_only, text_uuid_exactonly, text_uuid_strip, and multi-mode with equality filter and UUID presence/absence verification.

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

**Severity: Medium** — **FIXED**

`IcebergPartitionEvolutionTest.scala` covers unpartitioned→partitioned evolution, adding a second partition column, and mixed partition specs in a single sync.

---

### F11 — Iceberg cloud tests have lenient assertions

**Severity: Low** — **Open**

Cloud tests unchanged. New local tests have strong assertions.

---

### F12 — No Iceberg metrics validation

**Severity: Low** — **Open**

---

### F13 — No Iceberg error handling tests

**Severity: Low** — **FIXED**

`IcebergMiscFeaturesTest.scala` tests non-existent table (`shouldBe "error"`) and unreachable catalog (`shouldBe "error"`).

---

### F14 — No Iceberg write guard test

**Severity: Low** — **FIXED**

`IcebergMiscFeaturesTest.scala` verifies write to Iceberg companion table is rejected with appropriate error message.

---

### F15 — No Iceberg fast field mode tests

**Severity: Low** — **FIXED**

`IcebergMiscFeaturesTest.scala` tests all three modes (DISABLED, PARQUET_ONLY, HYBRID) with metadata verification and readability check (`count() shouldBe 5`).

---

### F16 — Edward branch divergence

**Severity: Low** — **Open**

---

### F17 — No Iceberg FROM SNAPSHOT in local tests

**Severity: Low** — **FIXED**

`IcebergMiscFeaturesTest.scala` tests FROM SNAPSHOT with specific snapshot ID (verifies only snapshot 1 data indexed, count=3 not 5) and non-existent snapshot error.

---

## Part 2: Features With Zero Tests (All Formats)

### F18 — Streaming error recovery / backoff never tested

**Severity: High** — **FIXED**

`StreamingCompanionErrorRecoveryTest.scala` covers consecutive error limit (maxConsecutiveErrors=3 and =2), error counter reset on success, exponential backoff timing with 80% lower-bound assertions, interrupt during backoff, and happy-path cycle verification.

---

### F19 — WRITER HEAP SIZE clause never tested

**Severity: Medium** — **FIXED**

`CompanionWriterHeapSizeTest.scala` tests 512M, 2G, and default (no clause) with exact row count verification.

---

### F20 — Arrow FFI path in distributed companion sync never tested

**Severity: Medium** — **FIXED**

`CompanionDistributedArrowFfiTest.scala` tests distributed vs non-distributed Iceberg parity (same parquet_files_indexed), Delta companion FFI enabled vs disabled read parity (count + sum), and Iceberg companion FFI read parity (count + sum).

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
| **High** | 4 | 4 (F1, F2, F3, F18) | 0 |
| **Medium** | 12 | 10 (F4-F10, F19, F20, F15) | 2 (F21, F22, F23) |
| **Low** | 11 | 4 (F13, F14, F15, F17) | 7 (F11, F12, F16, F24-F27) |

**19 of 27 findings fixed. 8 remain open (0 High, 3 Medium, 5 Low).**

## Updated Coverage Matrix

| Feature | Delta | Iceberg | Parquet |
|---------|-------|---------|---------|
| Basic build | 14 tests | 10 tests | 25 tests |
| Incremental sync (no-op) | yes | yes | yes |
| Incremental sync (mutations) | yes (OPTIMIZE/DELETE/UPDATE/MERGE) | **yes** (delete/overwrite/mixed) | N/A |
| Distributed sync | 27 tests | **12 tests** | **none** |
| WHERE clause filtering | 9+ tests | **yes** (=, IN, !=) | **none** |
| Streaming | 2 tests | 2 tests | **none** |
| Streaming error recovery | **yes** (7 tests) | **yes** (7 tests) | **none** |
| Schema evolution | yes | **yes** (add/drop/rename) | **none** |
| All data types | yes (comprehensive) | **yes** (round-trip all types) | yes (primitives) |
| Round-trip data fidelity | 13 tests | **yes** (6 tests) | **none** |
| Columnar read (Arrow FFI) | 20+ tests | **yes** (FFI parity) | **none** |
| TARGET INPUT SIZE | 5 tests | **yes** (1 test) | 1 test |
| Batching control | 10 tests | **yes** (4 tests) | **none** |
| Fast field modes | yes (3 modes) | **yes** (3 modes) | yes (1 mode) |
| Compact string modes | **none** | **yes** (4 tests) | yes |
| MERGE SPLITS | yes | **yes** (metadata + data + agg + filters) | **none** |
| PURGE | yes | **yes** (orphan delete, retention, DRY RUN, integrity) | **none** |
| DROP PARTITIONS | yes | **yes** (basic, correctness, re-sync, cumulative) | **none** |
| TRUNCATE TIME TRAVEL | yes | **yes** (versions, checkpoint, integrity, metadata, re-sync) | **none** |
| Bucket aggregations | yes | **yes** (Histogram, DateHistogram, Range, SUM) | **none** |
| Aggregate pushdown | yes | **yes** (COUNT, SUM, AVG, MIN, MAX, GROUP BY, filtered) | yes |
| Write guard | yes | **yes** | yes |
| Error handling | yes | **yes** (2 tests) | yes |
| Metrics | yes | **none** | **none** |
| Cloud (S3/Azure) | yes | yes | yes |
| Catalog integration | N/A | yes (REST/Glue/HMS) | N/A |
| Partition evolution | N/A | **yes** (3 tests) | N/A |
| FROM VERSION/SNAPSHOT | yes | **yes** (local + cloud) | N/A |
| WRITER HEAP SIZE | **yes** (3 tests) | **none** | **none** |
| Arrow FFI distributed sync | **yes** (parity) | **yes** (parity) | **none** |
| Path normalization | **none** | **none** | **none** |
| Column name mapping | N/A | **none** | N/A |
| DESCRIBE/PREWARM/CHECKPOINT | **none** | **none** | **none** |
| Concurrent builds | **none** | **none** | **none** |
| Metadata round-trip (full) | **none** | **none** | **none** |

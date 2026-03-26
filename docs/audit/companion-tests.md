# Companion Mode Test Audit — Delta vs Iceberg Gaps

**Date:** 2026-03-26
**Scope:** All companion test files across `indextables_spark`, `indextables_spark_edward`, and `tantivy4java`

## Inventory

**57 total companion test files.** Core sync/integration tests:

| Layer | Delta | Iceberg | Parquet | Format-Agnostic |
|-------|-------|---------|---------|-----------------|
| Sync integration | ~93 tests (3 files) | ~16 tests (3 files) | ~25 tests (1 file) | ~80+ tests (16 files) |
| Streaming | 2 tests | 2 tests | 0 | 0 |
| Cloud (S3/Azure) | yes | yes | yes | - |
| tantivy4java | - | - | 18 files | - |

**Delta: ~95 tests** — LocalSyncIntegrationTest (14), DistributedDeltaSyncIntegrationTest (27), MultiTransactionSyncTest (52), StreamingCompanionEndToEndTest (2)

**Iceberg: ~18 tests** — LocalIcebergSyncIntegrationTest (10), StreamingCompanionIcebergEndToEndTest (2), cloud tests (~6)

**Ratio: ~5:1 Delta to Iceberg.**

---

## Part 1: Delta vs Iceberg Gaps

### F1 — No Iceberg incremental sync with data mutation tests

**Severity: High**

Delta has extensive tests for source table mutations:
- `MultiTransactionSyncTest`: OPTIMIZE, DELETE, UPDATE, MERGE INTO
- Each verifies splits are invalidated and re-created correctly

Iceberg has **zero** tests for equivalent operations:
- No snapshot compaction (rewrite data files)
- No row-level delete
- No overwrite
- No expire snapshots

---

### F2 — No Iceberg distributed sync tests

**Severity: High**

Delta has `DistributedDeltaSyncIntegrationTest` (27 tests): distributed log reading, WHERE-scoped invalidation (5 tests), WHERE clause filters (9 tests), Arrow FFI parity, column mapping.

Iceberg has **zero** distributed sync tests. The distributed path is the production path.

---

### F3 — No Iceberg WHERE clause partition filtering tests

**Severity: High**

Delta tests WHERE with: equality, IN, range, compound AND, OR, !=, BETWEEN, date-format columns. Iceberg has **none**. Without this, users could index the entire table when they only want one partition.

---

### F4 — No Iceberg schema evolution test

**Severity: Medium**

Delta tests schema evolution (new column added). Iceberg — which supports richer schema evolution (add/drop/rename/reorder) — has zero tests.

---

### F5 — No Iceberg data type comprehensive tests

**Severity: Medium**

Delta has:
- `CompanionSplitTypeComprehensiveTest` — every Spark type as partition and non-partition
- `CompanionRoundTripTest` — 13 tests for exact row-level data fidelity
- `CompanionColumnarReadTest` — 20+ tests for Arrow FFI deserialization
- `MultiTransactionSyncTest` — struct, array, map columns

Iceberg only validates row counts (`count() == 12L`).

---

### F6 — No Iceberg TARGET INPUT SIZE tests

**Severity: Medium**

Delta has 5 tests. Iceberg has zero.

---

### F7 — No Iceberg batching control tests

**Severity: Medium**

Delta has 10 tests for batchSize and maxConcurrentBatches. Iceberg has zero.

---

### F8 — No Iceberg compact string indexing tests

**Severity: Medium**

`CompanionCompactStringTest` only tests Parquet. No test validates compact string modes with Iceberg sources.

---

### F9 — No Iceberg MERGE SPLITS correctness test

**Severity: Medium**

`CompanionMergeSplitsTest` documents a **known bug**: Iceberg merged splits fail parquet file path resolution (`file://` prefix issue). Should be fixed, not just documented.

---

### F10 — No Iceberg partition evolution tests

**Severity: Medium**

Iceberg uniquely supports partition spec evolution (e.g., daily → hourly). No test covers syncing a table whose partition spec changed between snapshots.

---

### F11 — Iceberg cloud tests have lenient assertions

**Severity: Low**

Cloud tests use `count() > 0L` instead of exact values. LocalIcebergSyncIntegrationTest has exact assertions for some checks but lenient for filters.

---

### F12 — No Iceberg metrics validation

**Severity: Low**

Delta verifies `parquet_bytes_downloaded`, `split_bytes_uploaded`, `duration_ms`. Iceberg has none.

---

### F13 — No Iceberg error handling tests

**Severity: Low**

Delta tests non-existent table. Iceberg has no equivalent for unreachable catalog, non-existent table, inaccessible snapshot, or malformed metadata.

---

### F14 — No Iceberg write guard test

**Severity: Low**

Delta and Parquet both test write guard rejection. Iceberg does not.

---

### F15 — No Iceberg fast field mode tests

**Severity: Low**

Delta tests all three FASTFIELDS MODEs. Iceberg doesn't explicitly test any.

---

### F16 — Edward branch divergence

**Severity: Low**

`indextables_spark_edward` has 17 companion test files vs. 22 in main. Missing: CompanionAggregatePushdownCorrectnessTest, CompanionMergeSplitsTest, CompanionDropPartitionsTest, CompanionTruncateTimeTravelTest, CompanionBucketAggregationTest, CompanionPurgeTest.

---

### F17 — No Iceberg FROM SNAPSHOT in local tests

**Severity: Low**

`FROM SNAPSHOT` tested in cloud tests only. Can't be tested without credentials.

---

## Part 2: Features With Zero Tests (All Formats)

### F18 — Streaming error recovery / backoff never tested

**Severity: High**

`StreamingCompanionManager` error handling is completely untested:
- `errorBackoffMultiplier` (default: 2) — exponential backoff formula
- `maxConsecutiveErrors` (default: 10) — stream termination threshold
- `quietPollLogInterval` (default: 10) — log noise reduction
- All streaming tests only exercise the happy path

---

### F19 — WRITER HEAP SIZE clause never tested

**Severity: Medium**

SQL syntax `WRITER HEAP SIZE '1G'` and config `spark.indextables.companion.writerHeapSize` have zero tests.

---

### F20 — Arrow FFI path in distributed companion sync never tested

**Severity: Medium**

`DistributedSourceScanner` has Arrow FFI code paths (`readDeltaCheckpointPartArrowFfi()`, `readIcebergManifestArrowFfi()`) and config `spark.indextables.companion.sync.arrowFfi.enabled`. No test toggles this. Default is `true`, so bugs affect production silently.

---

### F21 — Path normalization edge cases never tested

**Severity: Medium**

`DistributedAntiJoin.normalizePath()` and `DistributedSourceScanner.normalizeLocalPath()` handle URL decoding, `file:` scheme stripping, leading-slash normalization. Untested edge cases:
- URL-encoded paths (spaces, special chars)
- Mixed `s3://` and `s3a://` schemes
- Local paths with/without `file://` prefix

Anti-join correctness depends on path normalization.

---

### F22 — Iceberg physical-to-logical column name mapping never tested

**Severity: Medium**

`SyncToExternalCommand.columnNameMapping()` builds physical-to-logical maps using Iceberg `fieldIdToName`. Critical for reading Parquet files after column renames. Zero tests.

---

### F23 — Parquet companion has no streaming, MERGE SPLITS, DROP PARTITIONS, PURGE, or TRUNCATE tests

**Severity: Medium**

Parquet format is missing tests for every SQL operation except BUILD.

---

### F24 — DESCRIBE/PREWARM/CHECKPOINT/REPAIR on companion tables never tested

**Severity: Low**

No test validates these SQL commands work on companion-mode tables.

---

### F25 — Companion sync configuration options never tested

**Severity: Low**

Untested configs: `batchSize`, `maxConcurrentBatches`, `maxAutomaticHashedFastfields`, `readerBatchSize`, `schedulerPool`.

---

### F26 — Concurrent companion builds never tested

**Severity: Low**

No test for two BUILD COMPANION commands running simultaneously.

---

### F27 — Companion metadata round-trip never directly tested

**Severity: Low**

`buildCompanionMetadata()` writes 15+ keys. No test validates the complete set.

---

## Summary

| Severity | Count | Findings |
|----------|-------|----------|
| **High** | 4 | F1 (mutation sync), F2 (distributed sync), F3 (WHERE clause), F18 (streaming errors) |
| **Medium** | 12 | F4-F10, F19-F23 |
| **Low** | 11 | F11-F17, F24-F27 |

**Total: 27 findings**

## Coverage Matrix

| Feature | Delta | Iceberg | Parquet |
|---------|-------|---------|---------|
| Basic build | 14 tests | 10 tests | 25 tests |
| Incremental sync (no-op) | yes | yes | yes |
| Incremental sync (mutations) | yes (OPTIMIZE/DELETE/UPDATE/MERGE) | **none** | N/A |
| Distributed sync | 27 tests | **none** | **none** |
| WHERE clause filtering | 9+ tests | **none** | **none** |
| Streaming | 2 tests | 2 tests | **none** |
| Streaming error recovery | **none** | **none** | **none** |
| Schema evolution | yes | **none** | **none** |
| All data types | yes (comprehensive) | **none** | yes (primitives) |
| Round-trip data fidelity | 13 tests | **none** | **none** |
| Columnar read (Arrow FFI) | 20+ tests | **none** | **none** |
| TARGET INPUT SIZE | 5 tests | **none** | 1 test |
| Batching control | 10 tests | **none** | **none** |
| Fast field modes | yes (3 modes) | **none** | yes (1 mode) |
| Compact string modes | **none** | **none** | yes |
| MERGE SPLITS | yes | **known bug** | **none** |
| PURGE | yes (agnostic) | yes (agnostic) | **none** |
| DROP PARTITIONS | yes (agnostic) | yes (agnostic) | **none** |
| TRUNCATE TIME TRAVEL | yes (agnostic) | yes (agnostic) | **none** |
| Bucket aggregations | yes (agnostic) | yes (agnostic) | **none** |
| Aggregate pushdown | yes (agnostic) | yes (agnostic) | yes |
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

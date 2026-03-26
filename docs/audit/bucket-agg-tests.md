# Bucket Aggregation Test Audit (v2)

**Date:** 2026-03-25 (re-audited)
**Scope:** All bucket-agg test files across `indextables_spark`, `indextables_spark_edward`, and `tantivy4java`

## Files Audited

| # | File | Tests | Layer | Status |
|---|------|-------|-------|--------|
| 1 | `indextables_spark/.../BucketAggregationTest.scala` | 4 | Spark SQL | updated |
| 2 | `indextables_spark/.../BucketAggregationMultiKeyTest.scala` | 6 | Spark SQL | updated |
| 3 | `indextables_spark/.../BucketAggregationFfiParityTest.scala` | 7 | Spark SQL | updated |
| 4 | `indextables_spark/.../BucketAggregationEmptySplitTest.scala` | 7 | Spark internal | new in main |
| 5 | `indextables_spark/.../BucketAggregationErrorTest.scala` | 2 | Spark SQL | **new** |
| 6 | `indextables_spark/.../BucketAggregationExtendedTest.scala` | 9 | Spark SQL | **new** |
| 7 | `tantivy4java/.../SplitSearcherBucketAggregationTest.java` | 7 | Tantivy4Java | unchanged |

**Total: 42 tests across 7 files** (up from 31 across 5 files in v1)

---

## Original Findings — Resolution Status

| Finding | Severity | Status | Notes |
|---------|----------|--------|-------|
| F1 — Stale comment | Low | **FIXED** | Comment now matches assertions |
| F2 — DateHistogram no value assertions | High | **FIXED** | Fixed timestamps, exact bucket counts (4 buckets, 2/2/1/1) |
| F3 — Range no value assertions | High | **FIXED** | Asserts exact bucket names and counts (cheap=3, mid=1, expensive=2) |
| F4 — Non-deterministic timestamps | Medium | **FIXED** | Uses `Timestamp.valueOf(...)` throughout |
| F5 — MultiKey lenient assertions | Medium | **FIXED** | All 6 tests now use exact `==` counts |
| F6 — FFI test 7 not a bucket test | Low | **FIXED** | Replaced with Range + hostname multi-key parity test |
| F7 — EmptySplitTest missing from main | Medium | **FIXED** | Now in main branch (7 tests) |
| F8 — Cleanup not in finally | Low | **FIXED** | All tests use `finally` blocks + `FileCleanupHelper` |
| F9 — RangeAggregation contradicts CLAUDE.md | High | **RESOLVED** | CLAUDE.md updated — RangeAggregation is fully implemented |
| F10 — No AVG/MIN/MAX sub-aggs | Medium | **FIXED** | AVG and MIN/MAX tests in ExtendedTest |
| F11 — No optional param tests | Medium | **PARTIALLY FIXED** | offset + min_doc_count tested; hardBounds/extendedBounds not exposed in SQL layer |
| F12 — No WHERE clause tests | Medium | **FIXED** | Histogram + WHERE test in ExtendedTest |
| F13 — No FFI parity for Range + multi-key | Low | **FIXED** | Test 7 in FfiParityTest |
| F14 — No error tests in Spark | Medium | **PARTIALLY FIXED** | 2 tests added (string column, invalid interval) |
| F15 — No cross-layer validation | Low | **FIXED** | Spark SQL vs SplitSearcher parity in ExtendedTest |
| F16 — Narrow interval coverage | Low | **PARTIALLY FIXED** | 1h and 1s added (was 1d/15m/30m; now 5 intervals) |
| F17 — SparkSession per test | Low | **FIXED** | MultiKeyTest, ErrorTest, ExtendedTest all use shared sessions |

**Resolved: 12/17 fully fixed, 3/17 partially fixed, 2/17 resolved (no code change needed)**

---

## New Findings (v2)

### N1 — Histogram+SUM test still uses lenient assertion (BucketAggregationTest.scala:330)

**Severity:** Low

The fourth test ("Histogram aggregation with SUM sub-aggregation") still has `rows.length should be > 0` on line 330. The other three tests in this file were all upgraded to exact assertions, but this one was missed. The specific bucket assertions below it are strong, so the impact is minimal — but it should be `rows.length shouldBe 2` for consistency.

---

### N2 — BucketAggregationTest still creates SparkSession per test

**Severity:** Low (perf)

`BucketAggregationTest.scala` still uses `createTestSession()` per test (4 test methods = 4 SparkSession start/stop cycles). `MultiKeyTest`, `ErrorTest`, and `ExtendedTest` were all upgraded to `BeforeAndAfterAll` with shared sessions, but this file was not.

---

### N3 — No DateHistogram + SUM/AVG/MIN/MAX sub-aggregation tests

**Severity:** Medium

All sub-aggregation tests (COUNT, SUM, AVG, MIN, MAX) use `indextables_histogram`. There is no test combining `indextables_date_histogram` with any sub-aggregation beyond COUNT. If the date histogram sub-aggregation code path diverges from histogram, bugs there would go undetected.

---

### N4 — Error test coverage is thin (BucketAggregationErrorTest.scala)

**Severity:** Low

Only 2 error cases are tested:
- Histogram on string column
- DateHistogram with invalid interval string

Missing error scenarios:
- `indextables_histogram(price, -1.0)` — negative interval
- `indextables_histogram(price, 0.0)` — zero interval
- Bucket agg on a non-fast-field column (should fail or fallback gracefully)
- NULL values in the aggregation column

---

### N5 — Edward branch tests are now diverged from main

**Severity:** Medium

Main branch tests have received significant improvements:
- Shared SparkSession (`BeforeAndAfterAll`)
- Exact assertions (`== N` instead of `>= N`)
- `FileCleanupHelper` trait
- `TestBase.INDEXTABLES_FORMAT` constant
- Unique view names per test

The edward branch copies (`BucketAggregationTest`, `MultiKeyTest`, `FfiParityTest`) still use the old patterns. This will cause merge conflicts and means the edward branch has weaker test coverage for bucket-agg.

---

### N6 — Cross-layer validation test uses fragile internal APIs (BucketAggregationExtendedTest.scala:488-573)

**Severity:** Low

The Spark-vs-SplitSearcher cross-layer test directly imports:
- `TransactionLogFactory`
- `ConfigUtils.createSplitCacheConfig`
- `GlobalSplitCacheManager`
- `SplitMetadataFactory`

These are internal APIs subject to change. The test also assumes `txLog.listFiles().head` returns exactly one split. If internal APIs are refactored, this test breaks even if bucket aggregation itself is correct.

---

### N7 — No FFI parity tests for sub-aggregations (SUM, AVG, MIN, MAX)

**Severity:** Low

FFI parity tests exist for Histogram+SUM (`SUM(price)`) but not for AVG, MIN, or MAX sub-aggregations. The extended tests verify these work in the default path, but there's no parity check that FFI and InternalRow paths produce identical results for these functions.

---

### N8 — DateHistogram interval coverage still has gaps

**Severity:** Low

Current intervals tested: `1s`, `15m`, `30m`, `1h`, `1d` (5 of 14). Still untested:
- Sub-second: `1ms`, `10ms`, `100ms`
- Mid-range: `30s`, `1m`, `5m`
- Long-range: `6h`, `12h`, `7d`

---

## Summary by Severity (v2)

| Severity | Count | Findings |
|----------|-------|----------|
| **Medium** | 2 | N3, N5 |
| **Low** | 6 | N1, N2, N4, N6, N7, N8 |

All 3 original **High** severity findings are resolved.

## Coverage Matrix (v2)

| Feature | Spark Test | FFI Parity | Tantivy4Java |
|---------|-----------|------------|-------------|
| Histogram + COUNT | strong | yes | yes |
| Histogram + SUM | strong | yes | - |
| Histogram + AVG | strong | **none** | - |
| Histogram + MIN/MAX | strong | **none** | - |
| DateHistogram + COUNT | strong | yes | yes |
| DateHistogram + SUM/AVG/MIN/MAX | **none** (N3) | **none** | - |
| Range + COUNT | strong | yes | yes |
| Multi-key (2 cols) | strong | yes | - |
| Multi-key (3 cols) | strong | **none** | - |
| Range + multi-key | **none** | yes | - |
| Empty split | yes (7 tests) | **none** | - |
| With WHERE clause | yes | **none** | - |
| Offset param | yes | **none** | - |
| min_doc_count param | yes | **none** | - |
| hardBounds/extendedBounds | N/A (not exposed) | N/A | - |
| Error/invalid input | 2 tests | **none** | yes |
| Cross-layer parity | yes (1 test) | - | - |
| JSON generation | - | - | yes |
| Terms aggregation | - | - | yes |

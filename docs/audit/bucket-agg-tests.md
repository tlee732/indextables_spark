# Bucket Aggregation Test Audit

**Date:** 2026-03-25
**Scope:** All bucket-agg test files across `indextables_spark`, `indextables_spark_edward`, and `tantivy4java`

## Files Audited

| # | File | Tests | Layer |
|---|------|-------|-------|
| 1 | `indextables_spark/.../BucketAggregationTest.scala` | 4 | Spark SQL |
| 2 | `indextables_spark/.../BucketAggregationMultiKeyTest.scala` | 6 | Spark SQL |
| 3 | `indextables_spark/.../BucketAggregationFfiParityTest.scala` | 7 | Spark SQL |
| 4 | `indextables_spark_edward/.../BucketAggregationEmptySplitTest.scala` | 7 | Spark internal |
| 5 | `tantivy4java/.../SplitSearcherBucketAggregationTest.java` | 7 | Tantivy4Java |

**Total: 31 tests across 5 files (unique)**

Note: `indextables_spark_edward` contains copies of files 1-3 that are identical to `indextables_spark` (modulo minor formatting). `BucketAggregationEmptySplitTest` exists only in the edward branch.

---

## Findings

### F1 — Stale comment contradicts assertion (BucketAggregationTest.scala:81)

**Severity:** Low (cosmetic)

Comment on line 81 says:
```
// Expected buckets: 0-50 (4 items), 50-100 (3 items), 100-150 (0 items), 150-200 (1 item)
```

The assertion on line 104-106 correctly expects `0-50 → 3 items` and `50-100 → 4 items`. The comment has the counts swapped.

---

### F2 — DateHistogram test has no value assertions (BucketAggregationTest.scala:129-194)

**Severity:** High

The DateHistogram COUNT test only asserts `rows.length should be > 0`. It does not verify:
- Correct number of daily buckets (should be 4)
- Correct counts per bucket (day 0 → 2, day 1 → 2, day 2 → 1, day 3 → 1)
- Bucket key values

Compare with the Histogram test in the same file which verifies specific bucket keys and counts. This test could silently pass with completely wrong results.

---

### F3 — Range test has no value assertions (BucketAggregationTest.scala:196-261)

**Severity:** High

The Range aggregation test only asserts `rows.length should be > 0`. Expected results are straightforward:
- `cheap` (< 50): items at 10, 25, 35 → 3
- `mid` (50-100): item at 75 → 1
- `expensive` (>= 100): items at 150, 250 → 2

None of this is verified.

---

### F4 — DateHistogram uses non-deterministic timestamps (BucketAggregationTest.scala:136)

**Severity:** Medium

```scala
val baseTime = System.currentTimeMillis()
```

Bucket boundaries in DateHistogram depend on UTC day boundaries. Using `System.currentTimeMillis()` means that if the test runs near midnight UTC, documents may land in unexpected buckets. All other test files correctly use fixed timestamps (e.g., `Timestamp.valueOf("2024-01-01 10:00:00")`).

---

### F5 — MultiKeyTest assertions are too lenient throughout (BucketAggregationMultiKeyTest.scala)

**Severity:** Medium

All 6 tests use either `result.nonEmpty` or `result.length >= N` with generous lower bounds. No test verifies actual bucket keys, hostname values, or count accuracy. For example:

- **DateHistogram + hostname** (line 146): asserts `>= 4` but the expected distinct combinations are exactly 5.
- **Histogram + hostname** (line 275): asserts `>= 3` but the expected combinations are exactly 4.

These assertions can pass even if buckets are duplicated, miscounted, or incorrectly grouped.

---

### F6 — FFI parity test 7 is not a bucket aggregation test (BucketAggregationFfiParityTest.scala:285-327)

**Severity:** Low (misplacement)

Test 7 ("parity: 3-column GROUP BY with COUNT and SUM") does a plain `GROUP BY region, category, quarter` — no `indextables_histogram`, `indextables_date_histogram`, or `indextables_range` function is used. It tests terms-based GROUP BY parity, not bucket aggregation parity. This test belongs in a general aggregation parity test file, not in `BucketAggregationFfiParityTest`.

---

### F7 — BucketAggregationEmptySplitTest missing from main branch

**Severity:** Medium

`BucketAggregationEmptySplitTest` (7 tests for empty-split edge cases in `assembleBucketBatch`) exists only in `indextables_spark_edward`. This tests an important edge case — an empty split should produce a batch with 0 rows and the correct column count/types. This should be merged into the main branch.

---

### F8 — Temp directory cleanup not in `finally` block (BucketAggregationTest.scala)

**Severity:** Low

`deleteRecursively(tempDir)` is called inside the `try` block (e.g., line 123). If an assertion fails before that line, the temp directory leaks. The cleanup should be in the `finally` block or use a test framework mechanism (e.g., ScalaTest's `withClue` + fixture pattern or JUnit's `@TempDir`).

This pattern is consistent across `BucketAggregationTest`, `BucketAggregationMultiKeyTest`, and `BucketAggregationFfiParityTest`.

---

### F9 — Tantivy4Java RangeAggregation test contradicts CLAUDE.md

**Severity:** High (documentation or implementation mismatch)

`SplitSearcherBucketAggregationTest.java:162-201` tests `RangeAggregation` and asserts specific bucket counts (`budget=3`, `mid=2`, `premium=3`).

However, `tantivy4java/CLAUDE.md` states:
> ❌ RangeAggregation - Not implemented in native layer. Returns empty aggregation map.

One of these is wrong:
- If the test passes, the CLAUDE.md documentation is stale and should be updated.
- If the test is not actually run (e.g., `@Disabled`), it's dead code creating false confidence.

---

### F10 — No tests for AVG/MIN/MAX sub-aggregations

**Severity:** Medium

Only `COUNT(*)` and `SUM()` are tested as sub-aggregations within buckets. The system supports `AVG()`, `MIN()`, and `MAX()` per CLAUDE.md but none have bucket-aggregation test coverage.

---

### F11 — No tests for optional bucket parameters

**Severity:** Medium

The following supported parameters have zero test coverage:
- `offset` (DateHistogram, Histogram)
- `minDocCount` (DateHistogram, Histogram)
- `hardBounds` (DateHistogram, Histogram)
- `extendedBounds` (DateHistogram, Histogram)

---

### F12 — No tests for bucket aggregation with WHERE clause

**Severity:** Medium

Every test aggregates over the full dataset. Real-world usage combines filtering with aggregation:
```sql
SELECT indextables_histogram(price, 50.0) as bucket, COUNT(*)
FROM products
WHERE category = 'electronics'
GROUP BY indextables_histogram(price, 50.0)
```

There is no test verifying that filter pushdown interacts correctly with bucket aggregation pushdown.

---

### F13 — No FFI parity test for Range + multi-key

**Severity:** Low

FFI parity tests cover DateHistogram and Histogram both single-key and multi-key, plus Range single-key. Range + multi-key is missing.

---

### F14 — No negative/error case tests in Spark layer

**Severity:** Medium

No Spark SQL tests for:
- Invalid intervals (e.g., `indextables_histogram(price, -1.0)` or `indextables_date_histogram(ts, 'invalid')`)
- Non-fast-field columns used in bucket aggregation
- NULL values in the aggregation column
- Mismatched types (e.g., `indextables_histogram` on a string column)

The Tantivy4Java layer has `testBucketAggregationErrorHandling()` covering some of these, but the Spark SQL integration layer has none.

---

### F15 — No cross-layer validation test

**Severity:** Low

No test validates that the Spark SQL `indextables_histogram()`/`indextables_date_histogram()`/`indextables_range()` functions produce the same results as calling `SplitSearcher` aggregations directly via tantivy4java for the same data. A parity test between layers would catch serialization or translation bugs.

---

### F16 — DateHistogram interval coverage is narrow

**Severity:** Low

Only 3 intervals are tested: `'1d'`, `'15m'`, `'30m'`. The system supports 14 intervals (`1ms`, `10ms`, `100ms`, `1s`, `30s`, `1m`, `5m`, `15m`, `30m`, `1h`, `6h`, `12h`, `1d`, `7d`). Sub-second and week-scale intervals have no coverage.

---

### F17 — MultiKeyTest creates a new SparkSession per test

**Severity:** Low (perf)

`BucketAggregationMultiKeyTest` creates and stops a full SparkSession in each of its 6 tests. This makes the test suite slow. A shared session with `BeforeAll`/`AfterAll` would be more efficient while still providing test isolation through separate temp directories and view names.

---

## Summary by Severity

| Severity | Count | Findings |
|----------|-------|----------|
| **High** | 3 | F2, F3, F9 |
| **Medium** | 6 | F4, F5, F7, F10, F11, F12, F14 |
| **Low** | 5 | F1, F6, F8, F13, F15, F16, F17 |

## Coverage Matrix

| Feature | Spark Test | FFI Parity | Tantivy4Java |
|---------|-----------|------------|-------------|
| Histogram + COUNT | strong | yes | yes |
| Histogram + SUM | strong | yes | - |
| Histogram + AVG/MIN/MAX | **none** | **none** | - |
| DateHistogram + COUNT | weak (F2) | yes | yes |
| DateHistogram + SUM | **none** | **none** | - |
| Range + COUNT | weak (F3) | yes | yes (F9) |
| Multi-key (2 cols) | weak (F5) | yes | - |
| Multi-key (3 cols) | weak (F5) | **none** | - |
| Empty split | edward only (F7) | **none** | - |
| With WHERE clause | **none** | **none** | - |
| Optional params | **none** | **none** | - |
| Error/invalid input | **none** | **none** | yes |
| JSON generation | - | - | yes |
| Terms aggregation | - | - | yes |

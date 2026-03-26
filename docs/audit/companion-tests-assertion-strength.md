# Companion Test Assertion Audit — "Written to Pass" Analysis

**Date:** 2026-03-26 (v3 — final)
**Scope:** 8 uncommitted companion test files on `companion-test-coverage` branch

## Files Audited

| File | v1 Assessment | v3 Assessment |
|------|---------------|---------------|
| CompanionAggregatePushdownCorrectnessTest.scala | **STRONG** | **STRONG** |
| CompanionDropPartitionsTest.scala | **STRONG** | **STRONG** |
| CompanionPurgeTest.scala | **STRONG** | **STRONG** |
| CompanionTruncateTimeTravelTest.scala | **STRONG** | **STRONG** |
| CompanionBucketAggregationTest.scala | MEDIUM | **STRONG** |
| CompanionMergeSplitsTest.scala | PROBLEMATIC | **STRONG** |
| IcebergMutationSyncTest.scala | WEAK | **STRONG** |
| IcebergDistributedSyncTest.scala | PROBLEMATIC | **STRONG** |

**Result: 8/8 STRONG.** All findings from v1 have been resolved.

---

## Original Findings — Resolution Status

### Critical

| Finding | Status | Resolution |
|---------|--------|------------|
| C1 — IcebergDistributedSyncTest: WHERE-scoped invalidation asserted buggy behavior | **FIXED** | Assertion flipped to `usWestSplits shouldBe empty` — now asserts correct behavior. Test will fail if bug recurs. |
| C2 — CompanionMergeSplitsTest: Iceberg data access test intercepted exception as "correct" | **FIXED** | Replaced `intercept[Exception]` with `df.collect()` + exact row count and ID verification (`ids shouldBe Array(1L,...,8L)`). |

### High

| Finding | Status | Resolution |
|---------|--------|------------|
| H1 — IcebergMutationSyncTest: all tests used `> 0` / `>= 0` | **FIXED** | Every test now verifies data via `readCompanion().count() shouldBe N` with exact values. File counts use exact assertions (`shouldBe 3`, `shouldBe 2`, `shouldBe 11`). Snapshot ID verified against `expectedSnapshotId`. Partition tests verify which regions remain/are removed. |
| H2 — IcebergDistributedSyncTest: re-sync allowed ambiguous outcome | **FIXED** | Changed to `shouldBe "no_action"` — exact assertion. |

### Medium

| Finding | Status | Resolution |
|---------|--------|------------|
| M1 — flushCaches() exception swallowing | **FIXED** | All 8 files now call `flushCaches()` directly without try/catch. Config cleanup in `finally` blocks still uses defensive catch — confirmed acceptable. |
| M2 — CompanionMergeSplitsTest version `>= 1L` | **FIXED** | Changed to `shouldBe 1L`. |
| M3 — CompanionBucketAggregationTest `.find().get` pattern | **FIXED** | Replaced with `bucketMap` pattern — builds a `Map[Double, Long]` from results, then asserts `bucketMap(0.0) shouldBe 3` directly. Cleaner error on missing key. |
| M4 — CompanionBucketAggregationTest `rows.length > 0` | **FIXED** | All 5 instances replaced with bucket map lookups + `bucketMap.values.sum shouldBe 8` total verification. |

### Design Decisions (not bugs)

1. **DateHistogram per-bucket assertions** — Tests verify `counts.sum shouldBe 8L` and `counts.foreach(_ should be > 0L)` rather than asserting exact per-day counts. This is correct because exact bucket-day mapping depends on the JVM's timezone, making per-day assertions flaky in CI. Sum-based verification confirms no data loss.

2. **TARGET INPUT SIZE** (IcebergDistributedSyncTest) — Only asserts `splits_created > 0`. Acknowledged as low-impact and skipped — split sizing depends on file sizes which vary across environments.

3. **Config cleanup exception swallowing** — `spark.conf.unset()` in `finally` blocks uses `catch { case _: Exception => }`. Confirmed acceptable — defensive cleanup of SparkConf that can't affect test correctness.

---

## Summary

All 8 findings from the original audit have been resolved. No "written to pass" patterns remain. No exception swallowing in functional code. No tautological assertions. All tests use exact values where deterministic and sum-based verification where timezone-dependent.

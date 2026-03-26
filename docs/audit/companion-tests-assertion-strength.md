# Companion Test Assertion Audit — "Written to Pass" Analysis

**Date:** 2026-03-26
**Scope:** 8 uncommitted companion test files on `companion-test-coverage` branch

## Files Audited

| File | Assessment |
|------|------------|
| CompanionAggregatePushdownCorrectnessTest.scala | **STRONG** |
| CompanionDropPartitionsTest.scala | **STRONG** |
| CompanionPurgeTest.scala | **STRONG** |
| CompanionTruncateTimeTravelTest.scala | **STRONG** |
| CompanionBucketAggregationTest.scala | MEDIUM |
| CompanionMergeSplitsTest.scala | **PROBLEMATIC** |
| IcebergMutationSyncTest.scala | **WEAK** |
| IcebergDistributedSyncTest.scala | **PROBLEMATIC** |

---

## Critical — Tests That Assert Bugs as "Correct"

### C1 — IcebergDistributedSyncTest: WHERE-scoped invalidation asserts buggy behavior (lines 376-397)

**This is the most clear-cut "written to pass" case.**

Comments (lines 371-375, 388, 393-394) explicitly document a known bug:
```
// BUG: Iceberg scoped invalidation does not detect deleted files.
// Currently us-west splits are NOT invalidated (bug)
```

The assertion verifies the bug exists:
```scala
usWestSplits should not be empty  // WRONG: should be empty if invalidation worked
```

The correct assertion would be `usWestSplits shouldBe empty`. The test passes because the feature is broken. **When the bug is fixed, this test will start failing.**

---

### C2 — CompanionMergeSplitsTest: Iceberg data access test asserts exception as "correct" (lines 435-447)

```scala
test("all data accessible after merge on Iceberg companion") {
  withIcebergMerge { (_, df) =>
    val ex = intercept[Exception] {
      df.collect()
    }
    ex.getMessage should include("Failed to read columnar partition")
  }
}
```

Test name says "all data accessible" but it intercepts an exception and asserts the error message. Comments (lines 430-433) admit this is a known bug (merged splits lose `file://` paths). **When the bug is fixed, this test will start failing.**

---

## High — Pervasively Weak Assertions

### H1 — IcebergMutationSyncTest: All 10 tests use `> 0` or `>= 0` assertions

| Test | Assertion | Problem |
|------|-----------|---------|
| initial sync after multiple snapshots | `splits_created > 0` | Could be 1 or 100 — both pass |
| incremental sync detects new files | `splits_invalidated >= 0` | **Tautological** — can never fail |
| sync after file deletion | `splits_invalidated > 0` | Doesn't verify WHICH splits |
| sync after overwrite | `splits_invalidated > 0, splits_created > 0` | Bug that invalidates all + creates 1 passes |
| sync after mixed operations | `splits_created > 0, splits_invalidated > 0` | Same |
| sync after large snapshot gap | `files_indexed >= 10` | Could miss half the files |
| metadata tracks snapshot ID | `snapshotId != 0L` | Doesn't verify CORRECT ID |
| sync after partitioned append | `splits_created > 0` | No partition boundary check |
| sync after partitioned delete | `splits_invalidated > 0` | No partition-specific check |

**None verify data correctness.** Compare with CompanionAggregatePushdownCorrectnessTest which uses exact values like `shouldBe 1800.0`.

---

### H2 — IcebergDistributedSyncTest: Re-sync allows ambiguous outcome (lines 288-289)

```scala
Seq("success", "no_action") should contain(status)
```

Allows both "success" (with 0 splits) and "no_action". Can't distinguish correct behavior from a bug that doesn't detect changes. Should be `status shouldBe "no_action"`.

---

## Medium

### M1 — Exception swallowing in flushCaches() (all Iceberg test files)

```scala
catch { case _: Exception => }  // silently swallowed
```

If cache flushing fails, tests continue with stale state. Could mask real failures.

### M2 — CompanionMergeSplitsTest: Version assertion too lenient (line 238)

`should be >= 1L` when test data is deterministic. Should be `shouldBe 1L`.

### M3 — CompanionBucketAggregationTest: `.find().get` pattern

Throws `NoSuchElementException` (not an assertion) if bucket missing. The `shouldBe defined` check before `.get` catches this, but error messages are poor.

### M4 — CompanionBucketAggregationTest: `rows.length > 0` checks

Lines 277, 420 use `> 0` when exact count is known and should be checked.

---

## Summary

| Category | Count | Findings |
|----------|-------|----------|
| **Critical** (asserts bugs as correct) | 2 | C1, C2 |
| **High** (pervasively weak) | 2 | H1, H2 |
| **Medium** | 4 | M1-M4 |

**4/8 STRONG:** AggregatePushdownCorrectness, DropPartitions, Purge, TruncateTimeTravel
**2/8 PROBLEMATIC:** IcebergDistributedSync, CompanionMergeSplits — assert known-broken behavior as "correct"
**2/8 WEAK:** IcebergMutationSync, CompanionBucketAggregation — can't catch most regressions

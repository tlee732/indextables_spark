/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.indextables.spark.core

import java.io.File
import java.nio.file.Files
import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Parity tests for bucket aggregations between Arrow FFI (columnar) and InternalRow read paths.
 *
 * Each test runs the same bucket aggregation query twice -- once with FFI enabled and once disabled -- then verifies
 * the results match exactly. This ensures the FFI columnar path produces identical results to the object-based
 * InternalRow fallback path for all bucket aggregation types.
 */
class BucketAggregationFfiParityTest extends AnyFunSuite with Matchers
    with io.indextables.spark.testutils.FileCleanupHelper
    with io.indextables.spark.testutils.FfiParityTestBase {

  /**
   * Write shared test data once for a given test, returning the table path. Uses optimized write to produce a single
   * split for deterministic results.
   */
  private def writeTestData(spark: SparkSession): (File, String) = {
    import spark.implicits._

    val testData = Seq(
      ("alpha", 15.0, Timestamp.valueOf("2024-01-01 08:00:00"), "host1"),
      ("bravo", 25.0, Timestamp.valueOf("2024-01-01 08:05:00"), "host1"),
      ("charlie", 35.0, Timestamp.valueOf("2024-01-01 08:10:00"), "host2"),
      ("delta", 55.0, Timestamp.valueOf("2024-01-01 08:20:00"), "host1"),
      ("echo", 75.0, Timestamp.valueOf("2024-01-01 08:25:00"), "host2"),
      ("foxtrot", 85.0, Timestamp.valueOf("2024-01-01 08:35:00"), "host2"),
      ("golf", 95.0, Timestamp.valueOf("2024-01-01 08:45:00"), "host1"),
      ("hotel", 150.0, Timestamp.valueOf("2024-01-01 09:00:00"), "host2"),
      ("india", 10.0, Timestamp.valueOf("2024-01-01 09:10:00"), "host1"),
      ("juliet", 60.0, Timestamp.valueOf("2024-01-01 09:20:00"), "host2")
    ).toDF("name", "price", "timestamp", "hostname")

    val tempDir   = Files.createTempDirectory("bucket-ffi-parity-test").toFile
    val tablePath = tempDir.getAbsolutePath

    testData.write
      .format(PROVIDER)
      .option("spark.indextables.indexing.fastfields", "price,timestamp,hostname")
      .option("spark.indextables.write.optimizeWrite.enabled", "true")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    (tempDir, tablePath)
  }

  // ===== Test 1: DateHistogram with COUNT =====

  test("parity: DateHistogram bucket aggregation with COUNT") {
    val spark = createSparkSession("bucket-ffi-parity-datehistogram-count")
    try {
      val (tempDir, tablePath) = writeTestData(spark)

      val (disabledRows, enabledRows) = runWithBothPaths(
        spark,
        tablePath,
        viewName => s"""
          SELECT indextables_date_histogram(timestamp, '30m') as time_bucket, COUNT(*) as cnt
          FROM $viewName
          GROUP BY indextables_date_histogram(timestamp, '30m')
          ORDER BY time_bucket
        """
      )

      disabledRows.length should be > 0
      assertResultsMatch(disabledRows, enabledRows, "DateHistogram COUNT")

    } finally {
      deleteRecursively(tempDir)
      spark.stop()
    }
  }

  // ===== Test 2: Histogram with COUNT =====

  test("parity: Histogram bucket aggregation with COUNT") {
    val spark = createSparkSession("bucket-ffi-parity-histogram-count")
    try {
      val (tempDir, tablePath) = writeTestData(spark)

      val (disabledRows, enabledRows) = runWithBothPaths(
        spark,
        tablePath,
        viewName => s"""
          SELECT indextables_histogram(price, 50.0) as price_bucket, COUNT(*) as cnt
          FROM $viewName
          GROUP BY indextables_histogram(price, 50.0)
          ORDER BY price_bucket
        """
      )

      disabledRows.length should be > 0
      assertResultsMatch(disabledRows, enabledRows, "Histogram COUNT")

    } finally {
      deleteRecursively(tempDir)
      spark.stop()
    }
  }

  // ===== Test 3: Range with COUNT =====

  test("parity: Range bucket aggregation with COUNT") {
    val spark = createSparkSession("bucket-ffi-parity-range-count")
    try {
      val (tempDir, tablePath) = writeTestData(spark)

      val (disabledRows, enabledRows) = runWithBothPaths(
        spark,
        tablePath,
        viewName => s"""
          SELECT
            indextables_range(price, 'cheap', NULL, 50.0, 'mid', 50.0, 100.0, 'expensive', 100.0, NULL) as price_tier,
            COUNT(*) as cnt
          FROM $viewName
          GROUP BY indextables_range(price, 'cheap', NULL, 50.0, 'mid', 50.0, 100.0, 'expensive', 100.0, NULL)
          ORDER BY price_tier
        """
      )

      disabledRows.length should be > 0
      assertResultsMatch(disabledRows, enabledRows, "Range COUNT")

    } finally {
      deleteRecursively(tempDir)
      spark.stop()
    }
  }

  // ===== Test 4: Histogram with SUM sub-aggregation =====

  test("parity: Histogram bucket aggregation with SUM sub-aggregation") {
    val spark = createSparkSession("bucket-ffi-parity-histogram-sum")
    try {
      val (tempDir, tablePath) = writeTestData(spark)

      val (disabledRows, enabledRows) = runWithBothPaths(
        spark,
        tablePath,
        viewName => s"""
          SELECT indextables_histogram(price, 50.0) as price_bucket, COUNT(*) as cnt, SUM(price) as total_price
          FROM $viewName
          GROUP BY indextables_histogram(price, 50.0)
          ORDER BY price_bucket
        """
      )

      disabledRows.length should be > 0
      assertResultsMatch(disabledRows, enabledRows, "Histogram SUM")

    } finally {
      deleteRecursively(tempDir)
      spark.stop()
    }
  }

  // ===== Test 5: DateHistogram + hostname multi-key =====

  test("parity: DateHistogram + hostname multi-key bucket aggregation") {
    val spark = createSparkSession("bucket-ffi-parity-datehistogram-multikey")
    try {
      val (tempDir, tablePath) = writeTestData(spark)

      val (disabledRows, enabledRows) = runWithBothPaths(
        spark,
        tablePath,
        viewName => s"""
          SELECT indextables_date_histogram(timestamp, '30m') as time_bucket, hostname, COUNT(*) as cnt
          FROM $viewName
          GROUP BY indextables_date_histogram(timestamp, '30m'), hostname
          ORDER BY time_bucket, hostname
        """
      )

      disabledRows.length should be > 0
      assertResultsMatch(disabledRows, enabledRows, "DateHistogram + hostname multi-key")

    } finally {
      deleteRecursively(tempDir)
      spark.stop()
    }
  }

  // ===== Test 6: Histogram + hostname multi-key =====

  test("parity: Histogram + hostname multi-key bucket aggregation") {
    val spark = createSparkSession("bucket-ffi-parity-histogram-multikey")
    try {
      val (tempDir, tablePath) = writeTestData(spark)

      val (disabledRows, enabledRows) = runWithBothPaths(
        spark,
        tablePath,
        viewName => s"""
          SELECT indextables_histogram(price, 50.0) as price_bucket, hostname, COUNT(*) as cnt
          FROM $viewName
          GROUP BY indextables_histogram(price, 50.0), hostname
          ORDER BY price_bucket, hostname
        """
      )

      disabledRows.length should be > 0
      assertResultsMatch(disabledRows, enabledRows, "Histogram + hostname multi-key")

    } finally {
      deleteRecursively(tempDir)
      spark.stop()
    }
  }

  // ===== Test 7: Range + hostname multi-key =====

  test("parity: Range + hostname multi-key bucket aggregation") {
    val spark = createSparkSession("bucket-ffi-parity-range-multikey")
    try {
      val (tempDir, tablePath) = writeTestData(spark)

      val (disabledRows, enabledRows) = runWithBothPaths(
        spark,
        tablePath,
        viewName => s"""
          SELECT
            indextables_range(price, 'cheap', NULL, 50.0, 'mid', 50.0, 100.0, 'expensive', 100.0, NULL) as price_tier,
            hostname,
            COUNT(*) as cnt
          FROM $viewName
          GROUP BY indextables_range(price, 'cheap', NULL, 50.0, 'mid', 50.0, 100.0, 'expensive', 100.0, NULL), hostname
          ORDER BY price_tier, hostname
        """
      )

      disabledRows.length should be > 0
      assertResultsMatch(disabledRows, enabledRows, "Range + hostname multi-key")

    } finally {
      deleteRecursively(tempDir)
      spark.stop()
    }
  }
}

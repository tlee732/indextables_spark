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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.SaveMode

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Parity tests for terms-based GROUP BY aggregations between Arrow FFI and InternalRow read paths.
 *
 * Separated from BucketAggregationFfiParityTest because these tests use plain GROUP BY
 * (terms aggregation) rather than bucket aggregation functions like indextables_histogram,
 * indextables_date_histogram, or indextables_range.
 */
class AggregationGroupByFfiParityTest extends AnyFunSuite with Matchers with io.indextables.spark.testutils.FileCleanupHelper {

  private val PROVIDER   = io.indextables.spark.TestBase.INDEXTABLES_FORMAT
  private val EXTENSIONS = "io.indextables.spark.extensions.IndexTables4SparkExtensions"

  private def createSparkSession(appName: String): SparkSession =
    SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.sql.extensions", EXTENSIONS)
      .getOrCreate()

  private def runQuery(
    spark: SparkSession,
    tablePath: String,
    ffiEnabled: Boolean,
    viewName: String,
    sql: String
  ): Array[Row] = {
    val df = spark.read
      .format(PROVIDER)
      .option("spark.indextables.read.aggregation.arrowFfi.enabled", ffiEnabled.toString)
      .load(tablePath)

    df.createOrReplaceTempView(viewName)
    spark.sql(sql).collect()
  }

  private def runWithBothPaths(
    spark: SparkSession,
    tablePath: String,
    queryTemplate: String => String
  ): (Array[Row], Array[Row]) = {
    val viewDisabled = "tbl_ffi_off"
    val viewEnabled  = "tbl_ffi_on"

    val disabledRows = runQuery(spark, tablePath, ffiEnabled = false, viewDisabled, queryTemplate(viewDisabled))
    val enabledRows  = runQuery(spark, tablePath, ffiEnabled = true, viewEnabled, queryTemplate(viewEnabled))

    (disabledRows, enabledRows)
  }

  private def assertResultsMatch(
    disabledRows: Array[Row],
    enabledRows: Array[Row],
    testLabel: String
  ): Unit = {
    withClue(s"$testLabel: row count mismatch") {
      disabledRows.length shouldBe enabledRows.length
    }

    val sortedDisabled = disabledRows.sortBy(_.get(0).toString)
    val sortedEnabled  = enabledRows.sortBy(_.get(0).toString)

    sortedDisabled.zip(sortedEnabled).zipWithIndex.foreach {
      case ((rowD, rowE), idx) =>
        withClue(s"$testLabel: column count mismatch at row $idx") {
          rowD.length shouldBe rowE.length
        }
        (0 until rowD.length).foreach { col =>
          withClue(
            s"$testLabel: value mismatch at row $idx, col $col: disabled=${rowD.get(col)}, enabled=${rowE.get(col)}"
          ) {
            rowD.get(col) shouldBe rowE.get(col)
          }
        }
    }
  }

  test("parity: 3-column GROUP BY with COUNT and SUM") {
    val spark = createSparkSession("groupby-ffi-parity-3col")
    try {
      import spark.implicits._

      val testData = Seq(
        ("US", "Electronics", "Q1", 100),
        ("US", "Electronics", "Q2", 200),
        ("US", "Books", "Q1", 50),
        ("UK", "Electronics", "Q1", 80),
        ("UK", "Books", "Q2", 30),
        ("US", "Electronics", "Q1", 150)
      ).toDF("region", "category", "quarter", "sales")

      val tempDir   = Files.createTempDirectory("3col-groupby-parity").toFile
      val tablePath = tempDir.getAbsolutePath

      testData.write
        .format(PROVIDER)
        .option("spark.indextables.indexing.fastfields", "region,category,quarter,sales")
        .option("spark.indextables.write.optimizeWrite.enabled", "true")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      val (disabledRows, enabledRows) = runWithBothPaths(
        spark,
        tablePath,
        viewName => s"""
          SELECT region, category, quarter, COUNT(*) as cnt, SUM(sales) as total_sales
          FROM $viewName
          GROUP BY region, category, quarter
          ORDER BY region, category, quarter
        """
      )

      disabledRows.length should be > 0
      assertResultsMatch(disabledRows, enabledRows, "3-column GROUP BY")

      // Verify expected row count (5 distinct combinations)
      disabledRows.length shouldBe 5

    } finally {
      deleteRecursively(tempDir)
      spark.stop()
    }
  }
}

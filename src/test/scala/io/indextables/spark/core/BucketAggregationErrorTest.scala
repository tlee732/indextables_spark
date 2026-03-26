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

import java.nio.file.Files
import java.sql.Timestamp

import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Negative and error case tests for bucket aggregation functions (F14).
 */
class BucketAggregationErrorTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with io.indextables.spark.testutils.FileCleanupHelper {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .appName("BucketAggregationErrorTest")
      .master("local[*]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    super.afterAll()
  }

  test("Histogram on string column should fail gracefully") {
    import spark.implicits._

    val testData = Seq(("alpha", "foo"), ("bravo", "bar")).toDF("name", "category")

    val tempDir   = Files.createTempDirectory("histogram-string-error").toFile
    val tablePath = tempDir.getAbsolutePath

    try {
      testData.write
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      val df = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .load(tablePath)

      df.createOrReplaceTempView("string_test")

      // Histogram on a string column — should throw AnalysisException or produce an error
      an[Exception] should be thrownBy {
        spark.sql(
          """
            |SELECT indextables_histogram(category, 50.0) as bucket, COUNT(*) as cnt
            |FROM string_test
            |GROUP BY indextables_histogram(category, 50.0)
            |""".stripMargin
        ).collect()
      }

    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("DateHistogram with invalid interval should fail gracefully") {
    import spark.implicits._

    val testData = Seq(
      ("e1", Timestamp.valueOf("2024-01-01 10:00:00"))
    ).toDF("name", "event_time")

    val tempDir   = Files.createTempDirectory("datehist-invalid-interval").toFile
    val tablePath = tempDir.getAbsolutePath

    try {
      testData.write
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.fastfields", "event_time")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      val df = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .load(tablePath)

      df.createOrReplaceTempView("invalid_interval_test")

      an[Exception] should be thrownBy {
        spark.sql(
          """
            |SELECT indextables_date_histogram(event_time, 'invalid') as bucket, COUNT(*) as cnt
            |FROM invalid_interval_test
            |GROUP BY indextables_date_histogram(event_time, 'invalid')
            |""".stripMargin
        ).collect()
      }

    } finally {
      deleteRecursively(tempDir)
    }
  }
}

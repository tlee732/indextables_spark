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

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Extended bucket aggregation tests covering gaps identified in the audit:
 * - F10: AVG/MIN/MAX sub-aggregations
 * - F12: Bucket aggregation with WHERE clause
 * - F16: Additional DateHistogram interval coverage
 */
class BucketAggregationExtendedTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with io.indextables.spark.testutils.FileCleanupHelper {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .appName("BucketAggregationExtendedTest")
      .master("local[*]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    super.afterAll()
  }

  // ----- F10: AVG/MIN/MAX sub-aggregations -----

  test("Histogram with AVG sub-aggregation should return correct averages per bucket") {
    import spark.implicits._

    val testData = Seq(
      ("prod1", 15.0, 10),
      ("prod2", 25.0, 20),
      ("prod3", 35.0, 30),
      ("prod4", 55.0, 40),
      ("prod5", 75.0, 50)
    ).toDF("name", "price", "quantity")

    val tempDir   = Files.createTempDirectory("histogram-avg-test").toFile
    val tablePath = tempDir.getAbsolutePath

    try {
      testData.write
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.fastfields", "price,quantity")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      val df = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .load(tablePath)

      df.createOrReplaceTempView("products_avg")

      val rows = spark.sql(
        """
          |SELECT indextables_histogram(price, 50.0) as bucket, COUNT(*) as cnt, AVG(quantity) as avg_qty
          |FROM products_avg
          |GROUP BY indextables_histogram(price, 50.0)
          |ORDER BY bucket
          |""".stripMargin
      ).collect()

      // Bucket 0: quantities 10, 20, 30 → avg = 20.0
      // Bucket 50: quantities 40, 50 → avg = 45.0
      rows.length shouldBe 2

      val bucket0 = rows.find(_.getAs[Double]("bucket") == 0.0)
      bucket0 shouldBe defined
      bucket0.get.getAs[Double]("avg_qty") shouldBe 20.0 +- 0.01

      val bucket50 = rows.find(_.getAs[Double]("bucket") == 50.0)
      bucket50 shouldBe defined
      bucket50.get.getAs[Double]("avg_qty") shouldBe 45.0 +- 0.01

    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("Histogram with MIN and MAX sub-aggregations should return correct extremes per bucket") {
    import spark.implicits._

    val testData = Seq(
      ("prod1", 15.0, 10),
      ("prod2", 25.0, 20),
      ("prod3", 35.0, 30),
      ("prod4", 55.0, 40),
      ("prod5", 75.0, 50)
    ).toDF("name", "price", "quantity")

    val tempDir   = Files.createTempDirectory("histogram-minmax-test").toFile
    val tablePath = tempDir.getAbsolutePath

    try {
      testData.write
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.fastfields", "price,quantity")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      val df = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .load(tablePath)

      df.createOrReplaceTempView("products_minmax")

      val rows = spark.sql(
        """
          |SELECT indextables_histogram(price, 50.0) as bucket, MIN(quantity) as min_qty, MAX(quantity) as max_qty
          |FROM products_minmax
          |GROUP BY indextables_histogram(price, 50.0)
          |ORDER BY bucket
          |""".stripMargin
      ).collect()

      rows.length shouldBe 2

      // Bucket 0: quantities 10, 20, 30 → min=10, max=30
      val bucket0 = rows.find(_.getAs[Double]("bucket") == 0.0)
      bucket0 shouldBe defined
      bucket0.get.getAs[Long]("min_qty") shouldBe 10
      bucket0.get.getAs[Long]("max_qty") shouldBe 30

      // Bucket 50: quantities 40, 50 → min=40, max=50
      val bucket50 = rows.find(_.getAs[Double]("bucket") == 50.0)
      bucket50 shouldBe defined
      bucket50.get.getAs[Long]("min_qty") shouldBe 40
      bucket50.get.getAs[Long]("max_qty") shouldBe 50

    } finally {
      deleteRecursively(tempDir)
    }
  }

  // ----- F12: Bucket aggregation with WHERE clause -----

  test("Histogram with WHERE clause should only aggregate filtered rows") {
    import spark.implicits._

    val testData = Seq(
      ("electronics", 15.0, 10),
      ("electronics", 55.0, 20),
      ("electronics", 75.0, 30),
      ("books", 25.0, 40),
      ("books", 85.0, 50),
      ("books", 150.0, 60)
    ).toDF("category", "price", "quantity")

    val tempDir   = Files.createTempDirectory("histogram-where-test").toFile
    val tablePath = tempDir.getAbsolutePath

    try {
      testData.write
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.fastfields", "price,quantity")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      val df = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .load(tablePath)

      df.createOrReplaceTempView("products_where")

      // Only electronics: prices 15, 55, 75
      // Bucket 0: 15 → 1 item, Bucket 50: 55, 75 → 2 items
      val rows = spark.sql(
        """
          |SELECT indextables_histogram(price, 50.0) as bucket, COUNT(*) as cnt
          |FROM products_where
          |WHERE category = 'electronics'
          |GROUP BY indextables_histogram(price, 50.0)
          |ORDER BY bucket
          |""".stripMargin
      ).collect()

      rows.length shouldBe 2

      val bucket0 = rows.find(_.getAs[Double]("bucket") == 0.0)
      bucket0 shouldBe defined
      bucket0.get.getAs[Long]("cnt") shouldBe 1

      val bucket50 = rows.find(_.getAs[Double]("bucket") == 50.0)
      bucket50 shouldBe defined
      bucket50.get.getAs[Long]("cnt") shouldBe 2

    } finally {
      deleteRecursively(tempDir)
    }
  }

  // ----- F16: Additional DateHistogram interval coverage -----

  test("DateHistogram with 1h interval should produce correct hourly buckets") {
    import spark.implicits._

    val testData = Seq(
      ("e1", Timestamp.valueOf("2024-01-01 08:00:00")),
      ("e2", Timestamp.valueOf("2024-01-01 08:30:00")),
      ("e3", Timestamp.valueOf("2024-01-01 09:00:00")),
      ("e4", Timestamp.valueOf("2024-01-01 09:45:00")),
      ("e5", Timestamp.valueOf("2024-01-01 10:00:00"))
    ).toDF("name", "event_time")

    val tempDir   = Files.createTempDirectory("datehist-1h-test").toFile
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

      df.createOrReplaceTempView("events_1h")

      val rows = spark.sql(
        """
          |SELECT indextables_date_histogram(event_time, '1h') as hour_bucket, COUNT(*) as cnt
          |FROM events_1h
          |GROUP BY indextables_date_histogram(event_time, '1h')
          |ORDER BY hour_bucket
          |""".stripMargin
      ).collect()

      // 08:00-09:00 (2), 09:00-10:00 (2), 10:00-11:00 (1)
      rows.length shouldBe 3
      rows(0).getAs[Long]("cnt") shouldBe 2
      rows(1).getAs[Long]("cnt") shouldBe 2
      rows(2).getAs[Long]("cnt") shouldBe 1

    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("DateHistogram with 1s interval should produce per-second buckets") {
    import spark.implicits._

    val testData = Seq(
      ("e1", Timestamp.valueOf("2024-01-01 10:00:00")),
      ("e2", Timestamp.valueOf("2024-01-01 10:00:00")),
      ("e3", Timestamp.valueOf("2024-01-01 10:00:01")),
      ("e4", Timestamp.valueOf("2024-01-01 10:00:02"))
    ).toDF("name", "event_time")

    val tempDir   = Files.createTempDirectory("datehist-1s-test").toFile
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

      df.createOrReplaceTempView("events_1s")

      val rows = spark.sql(
        """
          |SELECT indextables_date_histogram(event_time, '1s') as sec_bucket, COUNT(*) as cnt
          |FROM events_1s
          |GROUP BY indextables_date_histogram(event_time, '1s')
          |ORDER BY sec_bucket
          |""".stripMargin
      ).collect()

      // :00 (2), :01 (1), :02 (1)
      rows.length shouldBe 3
      rows(0).getAs[Long]("cnt") shouldBe 2
      rows(1).getAs[Long]("cnt") shouldBe 1
      rows(2).getAs[Long]("cnt") shouldBe 1

    } finally {
      deleteRecursively(tempDir)
    }
  }
}

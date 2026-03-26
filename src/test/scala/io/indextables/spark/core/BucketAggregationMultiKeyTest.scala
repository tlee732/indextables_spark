package io.indextables.spark.core

import java.io.File
import java.sql.Timestamp

import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

/**
 * Test for bucket aggregations with multiple GROUP BY keys.
 *
 * Multi-key bucket aggregations are supported using nested TermsAggregation:
 *   - The bucket aggregation (DateHistogram/Histogram) is the outer aggregation
 *   - Additional GROUP BY columns use nested TermsAggregation as sub-aggregations
 *   - Results are flattened: [bucket_key, term_key, aggregation_values]
 */
class BucketAggregationMultiKeyTest extends AnyFunSuite with BeforeAndAfterAll with io.indextables.spark.testutils.FileCleanupHelper {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .appName("BucketAggregationMultiKeyTest")
      .master("local[*]")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    super.afterAll()
  }

  test("DateHistogram with single GROUP BY key should work") {
    import spark.implicits._

    // Test data with timestamps and hostnames
    val testData = Seq(
      (Timestamp.valueOf("2024-01-01 10:00:00"), "host1", 100),
      (Timestamp.valueOf("2024-01-01 10:05:00"), "host1", 200),
      (Timestamp.valueOf("2024-01-01 10:10:00"), "host2", 300),
      (Timestamp.valueOf("2024-01-01 10:20:00"), "host1", 400),
      (Timestamp.valueOf("2024-01-01 10:25:00"), "host2", 500),
      (Timestamp.valueOf("2024-01-01 10:35:00"), "host2", 600)
    ).toDF("timestamp", "hostname", "value")

    val tempDir   = java.nio.file.Files.createTempDirectory("bucket-multikey-test").toFile
    val tablePath = tempDir.getAbsolutePath

    try {
      // Write data with timestamp and hostname as fast fields
      testData.write
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.fastfields", "timestamp,hostname,value")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .load(tablePath)

      df.createOrReplaceTempView("datehist_single")

      val singleKeyResult = spark.sql("""
        SELECT indextables_date_histogram(timestamp, '15m') as slice, count(*) as cnt
        FROM datehist_single
        GROUP BY indextables_date_histogram(timestamp, '15m')
      """).collect()

      // 15m buckets: 10:00-10:15 (3), 10:15-10:30 (2), 10:30-10:45 (1)
      assert(singleKeyResult.length == 3, s"Expected 3 buckets, got ${singleKeyResult.length}")

    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("DateHistogram with additional GROUP BY column should work with nested TermsAggregation") {
    import spark.implicits._

    val testData = Seq(
      (Timestamp.valueOf("2024-01-01 10:00:00"), "host1", 100),
      (Timestamp.valueOf("2024-01-01 10:05:00"), "host1", 200),
      (Timestamp.valueOf("2024-01-01 10:10:00"), "host2", 300),
      (Timestamp.valueOf("2024-01-01 10:20:00"), "host1", 400),
      (Timestamp.valueOf("2024-01-01 10:25:00"), "host2", 500),
      (Timestamp.valueOf("2024-01-01 10:35:00"), "host2", 600)
    ).toDF("timestamp", "hostname", "value")

    val tempDir   = java.nio.file.Files.createTempDirectory("bucket-multikey-test2").toFile
    val tablePath = tempDir.getAbsolutePath

    try {
      testData.write
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.fastfields", "timestamp,hostname,value")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .load(tablePath)

      df.createOrReplaceTempView("datehist_multi")

      val multiKeyResult = spark.sql("""
        SELECT indextables_date_histogram(timestamp, '15m') as slice, hostname, count(*) as cnt
        FROM datehist_multi
        GROUP BY indextables_date_histogram(timestamp, '15m'), hostname
      """).collect()

      // 10:00-10:15 -> host1: 2, host2: 1
      // 10:15-10:30 -> host1: 1, host2: 1
      // 10:30-10:45 -> host2: 1
      assert(multiKeyResult.length == 5, s"Expected 5 rows, got ${multiKeyResult.length}")

    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("Histogram with single GROUP BY key should work") {
    import spark.implicits._

    val testData = Seq(
      ("category_a", "host1", 10.0),
      ("category_a", "host1", 25.0),
      ("category_a", "host2", 55.0),
      ("category_b", "host1", 75.0),
      ("category_b", "host2", 120.0)
    ).toDF("category", "hostname", "price")

    val tempDir   = java.nio.file.Files.createTempDirectory("histogram-singlekey-test").toFile
    val tablePath = tempDir.getAbsolutePath

    try {
      testData.write
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.fastfields", "category,hostname,price")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .load(tablePath)

      df.createOrReplaceTempView("hist_single")

      val result = spark.sql("""
        SELECT indextables_histogram(price, 50.0) as price_bucket, count(*) as cnt
        FROM hist_single
        GROUP BY indextables_histogram(price, 50.0)
      """).collect()

      // Buckets: 0 (10, 25) → 2, 50 (55, 75) → 2, 100 (120) → 1
      assert(result.length == 3, s"Expected 3 buckets, got ${result.length}")

    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("Histogram with additional GROUP BY column should work with nested TermsAggregation") {
    import spark.implicits._

    val testData = Seq(
      ("category_a", "host1", 10.0),
      ("category_a", "host1", 25.0),
      ("category_a", "host2", 55.0),
      ("category_b", "host1", 75.0),
      ("category_b", "host2", 120.0)
    ).toDF("category", "hostname", "price")

    val tempDir   = java.nio.file.Files.createTempDirectory("histogram-multikey-test").toFile
    val tablePath = tempDir.getAbsolutePath

    try {
      testData.write
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.fastfields", "category,hostname,price")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .load(tablePath)

      df.createOrReplaceTempView("hist_multi")

      val result = spark.sql("""
        SELECT indextables_histogram(price, 50.0) as price_bucket, hostname, count(*) as cnt
        FROM hist_multi
        GROUP BY indextables_histogram(price, 50.0), hostname
      """).collect()

      // 0.0/host1: 2 (10, 25), 50.0/host1: 1 (75), 50.0/host2: 1 (55), 100.0/host2: 1 (120)
      assert(result.length == 4, s"Expected 4 rows, got ${result.length}")

    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("DateHistogram with THREE GROUP BY columns should work with nested TermsAggregation") {
    import spark.implicits._

    val testData = Seq(
      (Timestamp.valueOf("2024-01-01 10:00:00"), "host1", "us-east", 100),
      (Timestamp.valueOf("2024-01-01 10:05:00"), "host1", "us-east", 200),
      (Timestamp.valueOf("2024-01-01 10:10:00"), "host2", "us-west", 300),
      (Timestamp.valueOf("2024-01-01 10:20:00"), "host1", "us-west", 400),
      (Timestamp.valueOf("2024-01-01 10:25:00"), "host2", "us-east", 500),
      (Timestamp.valueOf("2024-01-01 10:35:00"), "host2", "us-west", 600)
    ).toDF("timestamp", "hostname", "region", "value")

    val tempDir   = java.nio.file.Files.createTempDirectory("bucket-threekey-test").toFile
    val tablePath = tempDir.getAbsolutePath

    try {
      testData.write
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.fastfields", "timestamp,hostname,region,value")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .load(tablePath)

      df.createOrReplaceTempView("datehist_three")

      val threeKeyResult = spark.sql("""
        SELECT indextables_date_histogram(timestamp, '15m') as slice, hostname, region, count(*) as cnt
        FROM datehist_three
        GROUP BY indextables_date_histogram(timestamp, '15m'), hostname, region
      """).collect()

      // 10:00/host1/us-east: 2, 10:00/host2/us-west: 1,
      // 10:15/host1/us-west: 1, 10:15/host2/us-east: 1,
      // 10:30/host2/us-west: 1
      assert(threeKeyResult.length == 5, s"Expected 5 rows, got ${threeKeyResult.length}")

    } finally {
      deleteRecursively(tempDir)
    }
  }

  test("Histogram with THREE GROUP BY columns should work with nested TermsAggregation") {
    import spark.implicits._

    val testData = Seq(
      ("category_a", "host1", "us-east", 10.0),
      ("category_a", "host1", "us-west", 25.0),
      ("category_a", "host2", "us-east", 55.0),
      ("category_b", "host1", "us-east", 75.0),
      ("category_b", "host2", "us-west", 120.0)
    ).toDF("category", "hostname", "region", "price")

    val tempDir   = java.nio.file.Files.createTempDirectory("histogram-threekey-test").toFile
    val tablePath = tempDir.getAbsolutePath

    try {
      testData.write
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.fastfields", "category,hostname,region,price")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
        .load(tablePath)

      df.createOrReplaceTempView("hist_three")

      val result = spark.sql("""
        SELECT indextables_histogram(price, 50.0) as price_bucket, hostname, region, count(*) as cnt
        FROM hist_three
        GROUP BY indextables_histogram(price, 50.0), hostname, region
      """).collect()

      // 0/host1/us-east: 1 (10), 0/host1/us-west: 1 (25),
      // 50/host1/us-east: 1 (75), 50/host2/us-east: 1 (55),
      // 100/host2/us-west: 1 (120)
      assert(result.length == 5, s"Expected 5 rows, got ${result.length}")

    } finally {
      deleteRecursively(tempDir)
    }
  }
}

package io.indextables.spark.testutils

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.matchers.should.Matchers

/**
 * Shared helpers for FFI parity tests that compare Arrow FFI (columnar) and InternalRow read paths.
 */
trait FfiParityTestBase extends Matchers {

  protected val PROVIDER: String   = io.indextables.spark.TestBase.INDEXTABLES_FORMAT
  protected val EXTENSIONS: String = "io.indextables.spark.extensions.IndexTables4SparkExtensions"

  protected def createSparkSession(appName: String): SparkSession =
    SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.sql.extensions", EXTENSIONS)
      .getOrCreate()

  protected def runQuery(
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

  protected def runWithBothPaths(
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

  protected def assertResultsMatch(
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
}

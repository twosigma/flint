/*
 *  Copyright 2017 TWO SIGMA OPEN SOURCE, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.twosigma.flint.timeseries

import com.twosigma.flint.timeseries.PartitionStrategy.{ FillWithEmptyPartition, MultiTimestampNormalized, OnePartition, Origin }
import com.twosigma.flint.timeseries.row.Schema
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.file.ArrowFileReader
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.scalacheck.Prop.False
import org.scalatest.prop.PropertyChecks

import scala.collection.JavaConverters._
import java.util.concurrent.TimeUnit

import com.twosigma.flint.FlintConf
import com.twosigma.flint.rdd.function.window.summarizer.WindowBatchSummarizer

class SummarizeWindowsBatchSpec extends MultiPartitionSuite with TimeSeriesTestData with PropertyChecks {

  override val defaultResourceDir: String = "/timeseries/summarizewindows"

  private val v1Schema = Schema("id" -> IntegerType, "v1" -> IntegerType)
  private val v2Schema = Schema("id" -> IntegerType, "v2" -> IntegerType)

  private lazy val v1 = fromCSV("v1.csv", v1Schema)
  private lazy val v2 = fromCSV("v2.csv", v2Schema)

  private def withBatchsize[T](batchSize: Int)(block: => T): T = {
    val oldBatchSize = spark.conf.getOption(FlintConf.WINDOW_BATCH_MAXSIZE_CONF)
    spark.conf.set(FlintConf.WINDOW_BATCH_MAXSIZE_CONF, batchSize.toLong)

    try {
      block
    } finally {
      if (oldBatchSize.isEmpty) {
        spark.conf.unset(FlintConf.WINDOW_BATCH_MAXSIZE_CONF)
      } else {
        spark.conf.set(FlintConf.WINDOW_BATCH_MAXSIZE_CONF, oldBatchSize.get)
      }
    }
  }

  import com.twosigma.flint.timeseries.window.summarizer.ArrowWindowBatchSummarizer._

  def computeExpected(
    left: TimeSeriesRDD,
    right: TimeSeriesRDD,
    windowSize: Int,
    shouldMatchSk: Boolean
  ): Seq[Row] = {

    val schema = StructType(left.schema.fields :+ StructField("sum", IntegerType))

    def inWindowWithSk(leftRow: Row, rightRow: Row): Boolean = {
      val inWindow = if (windowSize < 0) {
        (leftRow.getLong(0) + windowSize) <= rightRow.getLong(0) && rightRow.getLong(0) <= leftRow.getLong(0)
      } else {
        leftRow.getLong(0) <= rightRow.getLong(0) && rightRow.getLong(0) <= (leftRow.getLong(0) + windowSize)
      }

      val skMatched = leftRow.getInt(1) == rightRow.getInt(1)

      inWindow && (!shouldMatchSk || skMatched)
    }

    val leftRows = left.collect().toList
    val rightRows = right.collect().toList

    val expected = leftRows.map {
      case leftRow =>
        val sum = leftRow.getAs[Int]("v1") + rightRows.filter(
          rightRow => inWindowWithSk(leftRow, rightRow)
        ).map(_.getAs[Int]("v2")).sum

        new GenericRowWithSchema((leftRow.toSeq :+ sum).toArray, schema)
    }

    expected
  }

  def fileFormatToRows(bytes: Array[Byte], schema: StructType): Seq[Row] = {
    val allocator = new RootAllocator(Int.MaxValue)
    val channel = new ByteArrayReadableSeekableByteChannel(bytes)
    val reader = new ArrowFileReader(channel, allocator)

    val root = reader.getVectorSchemaRoot
    reader.loadNextBatch()
    val vectors = root.getFieldVectors.asScala

    val rowCount = root.getRowCount
    val columnCount = root.getSchema.getFields.size()

    val values = (0 until rowCount).map { i =>
      (0 until columnCount).map{ j =>
        vectors(j).getAccessor.getObject(i)
      }
    }

    val rows = values.map { value =>
      new GenericRowWithSchema(value.toArray, schema)
    }

    reader.close()
    root.close()
    allocator.close()

    rows
  }

  def computeResult(
    left: TimeSeriesRDD,
    right: TimeSeriesRDD,
    windowSize: Int,
    shouldMatchSk: Boolean,
    batchSize: Int
  ): Seq[Row] = {
    withBatchsize(batchSize) {
      val schema = StructType(left.schema.fields :+ StructField("sum", IntegerType))

      val window = if (windowSize < 0) {
        Windows.pastAbsoluteTime(s"${-windowSize}ns")
      } else {
        Windows.futureAbsoluteTime(s"${windowSize}ns")
      }

      val sk = if (shouldMatchSk) Seq("id") else Seq.empty

      val summarizedTSRdd = left.summarizeWindowsBatch(window, sk)

      val result = summarizedTSRdd.collect().flatMap {
        case row =>
          val originLeftRows = row.getAs[Seq[Row]](baseRowsColumnName)
          val leftRows = fileFormatToRows(row.getAs[Array[Byte]](leftBatchColumnName), left.schema)

          assert(originLeftRows == leftRows)

          val rightRows = fileFormatToRows(row.getAs[Array[Byte]](rightBatchColumnName), right.schema)
          val indexRows = fileFormatToRows(
            row.getAs[Array[Byte]](indicesColumnName),
            StructType(Seq(
              StructField(beginIndexColumnName, IntegerType),
              StructField(endIndexColumnName, IntegerType)
            ))
          )

          val resultRows = (leftRows zip indexRows).map {
            case (leftRow, indexRow) =>
              val sum = leftRow.getAs[Int]("v1") +
                rightRows.slice(indexRow.getInt(0), indexRow.getInt(1)).map(_.getAs[Int]("v2")).sum
              new GenericRowWithSchema((leftRow.toSeq :+ sum).toArray, schema)
          }

          resultRows
      }.toList

      result
    }
  }

  /**
   * Do summarize window batch and compare the result with naive implementation.
   *
   * Takes Seq(windowSize, shouldMatchSk, batchSize) as param
   */
  def test(left: TimeSeriesRDD, right: TimeSeriesRDD, params: Seq[Any]): Unit = {
    val windowSize = params(0).asInstanceOf[Int]
    val shouldMatchSk = params(1).asInstanceOf[Boolean]
    val batchSize = params(2).asInstanceOf[Int]
    val expected = computeExpected(left, right, windowSize, shouldMatchSk)
    val result = computeResult(left, right, windowSize, shouldMatchSk, batchSize)

    assert(expected == result)
  }

  {
    val params = for (
      windowSize <- Seq(
        -5000, -900, -500, 0, 500, 900, 5000
      );
      shouldMatchSk <- Seq(false, true);
      batchSize <- Seq(1, 2, 5, 10)
    ) yield Seq(windowSize, shouldMatchSk, batchSize)

    val strategies = Seq(OnePartition, Origin, MultiTimestampNormalized :: FillWithEmptyPartition)
    // This is slow
    // val strategies = DEFAULT

    withPartitionStrategyAndParams(() => v2.renameColumns("v2" -> "v1"), () => v2)("self join")(
      strategies, strategies
    )(params)(test)

  }
}

/*
 *  Copyright 2017-2018 TWO SIGMA OPEN SOURCE, LLC
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

import java.util.concurrent.TimeUnit

import com.twosigma.flint.timeseries.row.Schema
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

class GroupByIntervalSpec extends MultiPartitionSuite with TimeSeriesTestData {

  override val defaultResourceDir: String = "/timeseries/groupbyinterval"
  private val clockBegin = "1970-01-01 00:00:00.000"
  private val clockEnd = "1970-01-01 00:00:00.001"

  "GroupByInterval" should "group by clock correctly" in {
    val volumeTSRdd = fromCSV("Volume.csv", Schema("id" -> IntegerType, "volume" -> LongType))
    val expectedSchema = Schema("rows" -> ArrayType(Schema("id" -> IntegerType, "volume" -> LongType)))
    val rows = volumeTSRdd.rdd.collect()
    val clockTSRdd = Clocks.uniform(
      sc, frequency = "100ns", beginDateTime = clockBegin, endDateTime = clockEnd
    )
    val expectedResults = TimeSeriesRDD.fromDF(spark.createDataFrame(sc.parallelize(Array[Row](
      new GenericRowWithSchema(Array(1100L, Array(rows(0), rows(1), rows(2), rows(3))), expectedSchema),
      new GenericRowWithSchema(Array(1200L, Array(rows(4), rows(5), rows(6), rows(7))), expectedSchema),
      new GenericRowWithSchema(Array(1300L, Array(rows(8), rows(9), rows(10), rows(11))), expectedSchema)
    )), expectedSchema))(isSorted = true, timeUnit = TimeUnit.NANOSECONDS)

    val clockTSRdd2 = Clocks.uniform(
      sc, frequency = "50ns", beginDateTime = clockBegin, endDateTime = clockEnd
    )

    val expectedResults2 = TimeSeriesRDD.fromDF(spark.createDataFrame(sc.parallelize(Array[Row](
      new GenericRowWithSchema(Array(1050L, Array(rows(0), rows(1))), expectedSchema),
      new GenericRowWithSchema(Array(1100L, Array(rows(2), rows(3))), expectedSchema),
      new GenericRowWithSchema(Array(1150L, Array(rows(4), rows(5))), expectedSchema),
      new GenericRowWithSchema(Array(1200L, Array(rows(6), rows(7))), expectedSchema),
      new GenericRowWithSchema(Array(1250L, Array(rows(8), rows(9))), expectedSchema),
      new GenericRowWithSchema(Array(1300L, Array(rows(10), rows(11))), expectedSchema)
    )), expectedSchema))(isSorted = true, timeUnit = TimeUnit.NANOSECONDS)

    def test(rdd: TimeSeriesRDD): Unit = {
      val results = rdd.groupByInterval(clockTSRdd)
      assertEquals(results, expectedResults)

      val results2 = rdd.groupByInterval(clockTSRdd2)
      assertEquals(results2, expectedResults2)
    }

    withPartitionStrategy(volumeTSRdd)(DEFAULT)(test)
  }

  it should "group by clock correctly with with (inclusion, rounding) = (end, end)" in {
    val volumeTSRdd = fromCSV("Volume.csv", Schema("id" -> IntegerType, "volume" -> LongType))
    val clockTSRdd = Clocks.uniform(
      sc, frequency = "100ns", beginDateTime = clockBegin, endDateTime = clockEnd
    )
    val expectedSchema = Schema("rows" -> ArrayType(Schema("id" -> IntegerType, "volume" -> LongType)))
    val rows = volumeTSRdd.rdd.collect()
    val expectedResults = TimeSeriesRDD.fromDF(spark.createDataFrame(sc.parallelize(Array[Row](
      new GenericRowWithSchema(Array(1000L, Array(rows(0), rows(1))), expectedSchema),
      new GenericRowWithSchema(Array(1100L, Array(rows(2), rows(3), rows(4), rows(5))), expectedSchema),
      new GenericRowWithSchema(Array(1200L, Array(rows(6), rows(7), rows(8), rows(9))), expectedSchema),
      new GenericRowWithSchema(Array(1300L, Array(rows(10), rows(11))), expectedSchema)
    )), expectedSchema))(isSorted = true, timeUnit = TimeUnit.NANOSECONDS)

    val clockTSRdd2 = Clocks.uniform(
      sc, frequency = "50ns", beginDateTime = clockBegin, endDateTime = clockEnd
    )

    val expectedResults2 = TimeSeriesRDD.fromDF(spark.createDataFrame(sc.parallelize(Array[Row](
      new GenericRowWithSchema(Array(1000L, Array(rows(0), rows(1))), expectedSchema),
      new GenericRowWithSchema(Array(1050L, Array(rows(2), rows(3))), expectedSchema),
      new GenericRowWithSchema(Array(1100L, Array(rows(4), rows(5))), expectedSchema),
      new GenericRowWithSchema(Array(1150L, Array(rows(6), rows(7))), expectedSchema),
      new GenericRowWithSchema(Array(1200L, Array(rows(8), rows(9))), expectedSchema),
      new GenericRowWithSchema(Array(1250L, Array(rows(10), rows(11))), expectedSchema)
    )), expectedSchema))(isSorted = true, timeUnit = TimeUnit.NANOSECONDS)

    def test(rdd: TimeSeriesRDD): Unit = {
      val results = rdd.groupByInterval(clockTSRdd, inclusion = "end", rounding = "end")
      assertEquals(results, expectedResults)

      val results2 = rdd.groupByInterval(clockTSRdd2, inclusion = "end", rounding = "end")
      assertEquals(results2, expectedResults2)
    }

    withPartitionStrategy(volumeTSRdd)(DEFAULT)(test)
  }

  it should "`groupByInterval` per key correctly" in {
    val volumeTSRdd = fromCSV("Volume.csv", Schema("id" -> IntegerType, "volume" -> LongType))
    val clockTSRdd = Clocks.uniform(
      sc, frequency = "100ns", beginDateTime = clockBegin, endDateTime = clockEnd
    )
    val expectedSchema =
      Schema("id" -> IntegerType, "rows" -> ArrayType(Schema("id" -> IntegerType, "volume" -> LongType)))
    val rows = volumeTSRdd.rdd.collect()
    val expectedResults = TimeSeriesRDD.fromDF(spark.createDataFrame(sc.parallelize(Array[Row](
      new GenericRowWithSchema(Array(1100L, 7, Array(rows(0), rows(3))), expectedSchema),
      new GenericRowWithSchema(Array(1100L, 3, Array(rows(1), rows(2))), expectedSchema),
      new GenericRowWithSchema(Array(1200L, 3, Array(rows(4), rows(6))), expectedSchema),
      new GenericRowWithSchema(Array(1200L, 7, Array(rows(5), rows(7))), expectedSchema),
      new GenericRowWithSchema(Array(1300L, 3, Array(rows(8), rows(10))), expectedSchema),
      new GenericRowWithSchema(Array(1300L, 7, Array(rows(9), rows(11))), expectedSchema)
    )), expectedSchema))(isSorted = true, timeUnit = TimeUnit.NANOSECONDS)

    def test(rdd: TimeSeriesRDD): Unit = {
      val results = rdd.groupByInterval(clockTSRdd, Seq("id"))
      assertEquals(results, expectedResults)
    }

    withPartitionStrategy(volumeTSRdd)(DEFAULT)(test)
  }

  it should "pass cycle data property test" in {
    val testData = cycleData1
    val cycleWidth = cycleMetaData1.cycleWidth
    val intervalWidth = cycleMetaData1.intervalWidth
    val clockBegin = "1970-01-01 00:00:00"
    val clockEnd = "1970-01-01 00:00:01"

    def sum(intervalNanos: Long, key: Seq[String], inclusion: String, rounding: String)(
      rdd: TimeSeriesRDD
    ): TimeSeriesRDD = {
      val clock = Clocks.uniform(
        sc,
        frequency = s"${intervalNanos}ns",
        beginDateTime = clockBegin,
        endDateTime = clockEnd
      )

      rdd.groupByInterval(
        clock,
        key,
        inclusion,
        rounding
      )
    }

    for (
      width <- Seq(cycleWidth, cycleWidth / 2, cycleWidth * 2, intervalWidth, intervalWidth / 2, intervalWidth * 2);
      key <- Seq(Seq.empty, Seq("id"));
      inclusion <- Seq("begin", "end");
      rounding <- Seq("begin", "end")
    ) {
      withPartitionStrategyCompare(testData)(DEFAULT)(sum(width, key, inclusion, rounding))
    }
  }
}

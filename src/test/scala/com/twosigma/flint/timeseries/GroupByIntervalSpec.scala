/*
 *  Copyright 2015-2017 TWO SIGMA OPEN SOURCE, LLC
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

import com.twosigma.flint.timeseries.row.Schema
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

import scala.collection.mutable

class GroupByIntervalSpec extends MultiPartitionSuite with TimeSeriesTestData {

  override val defaultResourceDir: String = "/timeseries/groupbyinterval"
  private val clockBegin = "1970-01-01 00:00:00.000"
  private val clockEnd = "1970-01-01 00:00:00.001"

  private def assertResultsEqual(result: Array[Row], expected: Array[Row]): Unit = {
    result.indices.foreach {
      index =>
        assert(result(index).getAs[mutable.WrappedArray[Row]]("rows").deep ==
          expected(index).getAs[Array[Row]]("rows").deep)
    }
    assert(result.map(_.schema).deep == expected.map(_.schema).deep)
  }

  "GroupByInterval" should "group by clock correctly with beginInclusive = true" in {
    val volumeTSRdd = fromCSV("Volume.csv", Schema("id" -> IntegerType, "volume" -> LongType))
    val expectedSchema = Schema("rows" -> ArrayType(Schema("id" -> IntegerType, "volume" -> LongType)))
    val rows = volumeTSRdd.rdd.collect()
    val clockTSRdd = Clocks.uniform(
      sc, frequency = "100ns", beginDateTime = clockBegin, endDateTime = clockEnd
    )
    val expectedResults = Array[Row](
      new GenericRowWithSchema(Array(1000L, Array(rows(0), rows(1), rows(2), rows(3))), expectedSchema),
      new GenericRowWithSchema(Array(1100L, Array(rows(4), rows(5), rows(6), rows(7))), expectedSchema),
      new GenericRowWithSchema(Array(1200L, Array(rows(8), rows(9), rows(10), rows(11))), expectedSchema)
    )

    val clockTSRdd2 = Clocks.uniform(
      sc, frequency = "50ns", beginDateTime = clockBegin, endDateTime = clockEnd
    )

    val expectedResults2 = Array[Row](
      new GenericRowWithSchema(Array(1000L, Array(rows(0), rows(1))), expectedSchema),
      new GenericRowWithSchema(Array(1050L, Array(rows(2), rows(3))), expectedSchema),
      new GenericRowWithSchema(Array(1100L, Array(rows(4), rows(5))), expectedSchema),
      new GenericRowWithSchema(Array(1150L, Array(rows(6), rows(7))), expectedSchema),
      new GenericRowWithSchema(Array(1200L, Array(rows(8), rows(9))), expectedSchema),
      new GenericRowWithSchema(Array(1250L, Array(rows(10), rows(11))), expectedSchema)
    )

    def test(rdd: TimeSeriesRDD): Unit = {
      val results = rdd.groupByInterval(clockTSRdd).collect()
      assertResultsEqual(results, expectedResults)

      val results2 = rdd.groupByInterval(clockTSRdd2).collect()
      assertResultsEqual(results2, expectedResults2)
    }

    withPartitionStrategy(volumeTSRdd)(DEFAULT)(test)
  }

  it should "group by clock correctly with beginInclusive = false" in {
    val volumeTSRdd = fromCSV("Volume.csv", Schema("id" -> IntegerType, "volume" -> LongType))
    val clockTSRdd = Clocks.uniform(
      sc, frequency = "100ns", beginDateTime = clockBegin, endDateTime = clockEnd
    )
    val expectedSchema = Schema("rows" -> ArrayType(Schema("id" -> IntegerType, "volume" -> LongType)))
    val rows = volumeTSRdd.rdd.collect()
    val expectedResults = Array[Row](
      new GenericRowWithSchema(Array(1000L, Array(rows(0), rows(1))), expectedSchema),
      new GenericRowWithSchema(Array(1100L, Array(rows(2), rows(3), rows(4), rows(5))), expectedSchema),
      new GenericRowWithSchema(Array(1200L, Array(rows(6), rows(7), rows(8), rows(9))), expectedSchema),
      new GenericRowWithSchema(Array(1300L, Array(rows(10), rows(11))), expectedSchema)
    )

    val clockTSRdd2 = Clocks.uniform(
      sc, frequency = "50ns", beginDateTime = clockBegin, endDateTime = clockEnd
    )

    val expectedResults2 = Array[Row](
      new GenericRowWithSchema(Array(1000L, Array(rows(0), rows(1))), expectedSchema),
      new GenericRowWithSchema(Array(1050L, Array(rows(2), rows(3))), expectedSchema),
      new GenericRowWithSchema(Array(1100L, Array(rows(4), rows(5))), expectedSchema),
      new GenericRowWithSchema(Array(1150L, Array(rows(6), rows(7))), expectedSchema),
      new GenericRowWithSchema(Array(1200L, Array(rows(8), rows(9))), expectedSchema),
      new GenericRowWithSchema(Array(1250L, Array(rows(10), rows(11))), expectedSchema)
    )

    def test(rdd: TimeSeriesRDD): Unit = {
      val results = rdd.groupByInterval(clockTSRdd, beginInclusive = false).collect()
      assertResultsEqual(results, expectedResults)

      val results2 = rdd.groupByInterval(clockTSRdd2, beginInclusive = false).collect()
      assertResultsEqual(results2, expectedResults2)
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
    val expectedResults = Array[Row](
      new GenericRowWithSchema(Array(1000L, 7, Array(rows(0), rows(3))), expectedSchema),
      new GenericRowWithSchema(Array(1000L, 3, Array(rows(1), rows(2))), expectedSchema),
      new GenericRowWithSchema(Array(1100L, 3, Array(rows(4), rows(6))), expectedSchema),
      new GenericRowWithSchema(Array(1100L, 7, Array(rows(5), rows(7))), expectedSchema),
      new GenericRowWithSchema(Array(1100L, 3, Array(rows(8), rows(10))), expectedSchema),
      new GenericRowWithSchema(Array(1200L, 7, Array(rows(9), rows(11))), expectedSchema)
    )

    def test(rdd: TimeSeriesRDD): Unit = {
      val results = rdd.groupByInterval(clockTSRdd, Seq("id")).collect()
      assertResultsEqual(results, expectedResults)
    }

    withPartitionStrategy(volumeTSRdd)(DEFAULT)(test)
  }

  it should "pass cycle data property test" in {
    val testData = cycleData1
    val cycleWidth = cycleMetaData1.cycleWidth
    val intervalWidth = cycleMetaData1.intervalWidth
    val clockBegin = "1970-01-01 00:00:00"
    val clockEnd = "1970-01-01 00:00:01"

    def sum(intervalNanos: Long, key: Seq[String], beginInclusive: Boolean)(
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
        beginInclusive
      )
    }

    for (
      width <- Seq(cycleWidth, cycleWidth / 2, cycleWidth * 2, intervalWidth, intervalWidth / 2, intervalWidth * 2);
      key <- Seq(Seq.empty, Seq("id"));
      beginInclusive <- Seq(true, false)
    ) {
      withPartitionStrategyCompare(testData)(DEFAULT)(sum(width, key, beginInclusive))
    }
  }
}

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

import com.twosigma.flint.timeseries.PartitionStrategy.{ MultiTimestampNormalized, MultiTimestampUnnormailzed }
import com.twosigma.flint.timeseries.row.Schema
import org.apache.spark.sql.types.{ DoubleType, IntegerType, LongType }

class LeftJoinSpec extends MultiPartitionSuite with TimeSeriesTestData {
  override val defaultResourceDir: String = "/timeseries/leftjoin"

  "LeftJoin" should "pass `JoinOnTime` test." in {
    val volumeTSRdd = fromCSV("Volume.csv", Schema("id" -> IntegerType, "volume" -> LongType))
    val resultsTSRdd = fromCSV(
      "JoinOnTime.results", Schema("id" -> IntegerType, "price" -> DoubleType, "volume" -> LongType)
    )

    def test(rdd1: TimeSeriesRDD, rdd2: TimeSeriesRDD): Unit = {
      assertEquals(rdd1.leftJoin(volumeTSRdd, "0ns", Seq("id")), resultsTSRdd)
    }

    val priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
    withPartitionStrategy(priceTSRdd, volumeTSRdd)(DEFAULT)(test)
  }

  it should "pass `JoinOnTime` with tolerance test" in {
    val volumeTSRdd = fromCSV("Volume.csv", Schema("id" -> IntegerType, "volume" -> LongType))
    val resultsTSRdd = fromCSV(
      "JoinOnTimeWithTolerance.results", Schema("id" -> IntegerType, "price" -> DoubleType, "volume" -> LongType)
    )

    def test(rdd1: TimeSeriesRDD, rdd2: TimeSeriesRDD): Unit = {
      assertEquals(
        rdd1.leftJoin(volumeTSRdd.shift(Windows.futureAbsoluteTime("1ns")), "1000ns", Seq("id")),
        resultsTSRdd
      )
    }

    val priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
    withPartitionStrategy(priceTSRdd, volumeTSRdd)(DEFAULT)(test)
  }

  it should "pass `JoinOnTimeWithMissingMatching` test." in {
    val resultsTSRdd = fromCSV(
      "JoinOnTimeWithMissingMatching.results",
      Schema("id" -> IntegerType, "price" -> DoubleType, "volume" -> LongType)
    )

    def test(rdd1: TimeSeriesRDD, rdd2: TimeSeriesRDD): Unit = {
      val joinedTSRdd = rdd1.leftJoin(
        rdd2.deleteRows(row => row.getAs[Long]("time") == 1050L), "0ns", Seq("id")
      )
      assertEquals(joinedTSRdd, resultsTSRdd)
    }

    val priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
    val volumeTSRdd = fromCSV("Volume.csv", Schema("id" -> IntegerType, "volume" -> LongType))
    withPartitionStrategy(priceTSRdd, volumeTSRdd)(DEFAULT)(test)
  }

  it should "pass `JoinOnTimeAndMultipleKeys` test." in {
    val resultsTSRdd = fromCSV(
      "JoinOnTimeAndMultipleKeys.results",
      Schema("id" -> IntegerType, "group" -> IntegerType, "price" -> DoubleType, "volume" -> LongType)
    )

    def test(rdd1: TimeSeriesRDD, rdd2: TimeSeriesRDD): Unit = {
      val joinedTSRdd = rdd1.leftJoin(rdd2, "0ns", Seq("id", "group"))
      assertEquals(joinedTSRdd, resultsTSRdd)
    }

    {
      val priceTSRdd = fromCSV(
        "PriceWithIndustryGroup.csv",
        Schema("id" -> IntegerType, "group" -> IntegerType, "price" -> DoubleType)
      )
      val volumeTSRdd = fromCSV(
        "VolumeWithIndustryGroup.csv",
        Schema("id" -> IntegerType, "group" -> IntegerType, "volume" -> LongType)
      )
      withPartitionStrategy(priceTSRdd, volumeTSRdd)(DEFAULT)(test)
    }
  }

  it should "pass cycle data property test" in {
    val testData1 = cycleData1
    val testData2 = cycleData2
    val cycleWidth = cycleMetaData1.cycleWidth
    val intervalWidth = cycleMetaData1.intervalWidth

    def leftJoin(
      tolerance: Long,
      key: Seq[String],
      rightShift: String
    )(rdd1: TimeSeriesRDD, rdd2: TimeSeriesRDD): TimeSeriesRDD = {
      rdd1.leftJoin(
        rdd2.shift(Windows.futureAbsoluteTime(rightShift)),
        tolerance = s"${tolerance}ns",
        key = key,
        rightAlias = "right"
      )
    }

    for (
      tolerance <- Seq(0, cycleWidth, intervalWidth);
      key <- Seq(Seq.empty, Seq("id"));
      rightShift <- Seq("0ns", "1ns")
    ) {
      // Ideally we want to run DEFAULT partition strategies, but it's too freaking slow!
      withPartitionStrategyCompare(
        testData1, testData2
      )(
        NONE :+ MultiTimestampUnnormailzed
      )(
          leftJoin(tolerance, key, rightShift)
        )
    }
  }
}

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

import com.twosigma.flint.timeseries.PartitionStrategy.MultiTimestampUnnormailzed
import com.twosigma.flint.timeseries.row.Schema
import org.apache.spark.sql.types._
import org.scalatest.tagobjects.Slow

class FutureLeftJoinSpec extends MultiPartitionSuite with TimeSeriesTestData {

  override val defaultResourceDir: String = "/timeseries/futureleftjoin"

  "FutureLeftJoin" should "pass `JoinOnTime` with strictLookahead test." in {
    val resultsTSRdd = fromCSV(
      "JoinOnTime.results",
      Schema("id" -> IntegerType, "price" -> DoubleType, "volume" -> LongType, "time2" -> LongType)
    )

    def test(rdd1: TimeSeriesRDD, rdd2: TimeSeriesRDD): Unit = {
      val joinedTSRdd = rdd1.futureLeftJoin(
        right = rdd2.addColumns(
          "time2" -> LongType -> { _.getAs[Long](TimeSeriesRDD.timeColumnName) }
        ),
        tolerance = "100ns",
        key = Seq("id"),
        strictLookahead = true
      )
      assertEquals(joinedTSRdd, resultsTSRdd)
    }

    {
      val priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
      val volumeTSRdd = fromCSV("Volume.csv", Schema("id" -> IntegerType, "volume" -> LongType))
      withPartitionStrategy(priceTSRdd, volumeTSRdd)(DEFAULT)(test)
    }
  }

  it should "pass `JoinOnTimeAndMultipleKeys` test." in {
    val resultsTSRdd = fromCSV(
      "JoinOnTimeAndMultipleKeys.results",
      Schema("id" -> IntegerType, "group" -> IntegerType, "price" -> DoubleType, "volume" -> LongType)
    )

    def test(priceTSRdd: TimeSeriesRDD, volumeTSRdd: TimeSeriesRDD): Unit = {
      val joinedTSRdd = priceTSRdd.leftJoin(volumeTSRdd, "0ns", Seq("id", "group"))
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

  it should "pass cycle data property test" taggedAs Slow in {
    val testData1 = cycleData1
    val testData2 = cycleData2
    val cycleWidth = cycleMetaData1.cycleWidth
    val intervalWidth = cycleMetaData1.intervalWidth

    def futureLeftJoin(
      tolerance: Long,
      key: Seq[String],
      strictLookahead: Boolean,
      rightShift: String
    )(rdd1: TimeSeriesRDD, rdd2: TimeSeriesRDD): TimeSeriesRDD = {
      rdd1.futureLeftJoin(
        rdd2.shift(Windows.pastAbsoluteTime(rightShift)),
        tolerance = s"${tolerance}ns",
        key = key,
        strictLookahead = strictLookahead, rightAlias = "right"
      )
    }

    for (
      tolerance <- Seq(0, cycleWidth, intervalWidth);
      key <- Seq(Seq.empty, Seq("id"));
      strictLookahead <- Seq(true, false);
      rightShift <- Seq("0ns", "1ns")
    ) {
      // Ideally we want to run DEFAULT partition strategies, but it's too freaking slow!
      withPartitionStrategyCompare(
        testData1, testData2
      )(
        NONE :+ MultiTimestampUnnormailzed
      )(
          futureLeftJoin(tolerance, key, strictLookahead, rightShift)
        )
    }
  }
}

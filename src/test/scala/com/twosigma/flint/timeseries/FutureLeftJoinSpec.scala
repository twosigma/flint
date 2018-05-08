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

import com.twosigma.flint.timeseries.PartitionStrategy.MultiTimestampUnnormailzed
import com.twosigma.flint.timeseries.row.Schema
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.tagobjects.Slow

class FutureLeftJoinSpec extends MultiPartitionSuite with TimeSeriesTestData with TimeTypeSuite {

  override val defaultResourceDir: String = "/timeseries/futureleftjoin"

  private var priceTSRdd: TimeSeriesRDD = _
  private var volumeTSRdd: TimeSeriesRDD = _
  private var volumeTSRddRowFiltered: TimeSeriesRDD = _
  private var priceTSRddWithGroup: TimeSeriesRDD = _
  private var volumeTSRddWithGroup: TimeSeriesRDD = _

  private def init: Unit = {
    priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
    volumeTSRdd = fromCSV("Volume.csv", Schema("id" -> IntegerType, "volume" -> LongType))
    volumeTSRddRowFiltered = fromCSV("VolumeWithRowFiltered.csv", Schema("id" -> IntegerType, "volume" -> LongType))
    priceTSRddWithGroup = fromCSV(
      "PriceWithIndustryGroup.csv",
      Schema("id" -> IntegerType, "group" -> IntegerType, "price" -> DoubleType)
    )
    volumeTSRddWithGroup = fromCSV(
      "VolumeWithIndustryGroup.csv",
      Schema("id" -> IntegerType, "group" -> IntegerType, "volume" -> LongType)
    )
  }

  "FutureLeftJoin" should "join on time" in {
    withAllTimeType {
      init
      val resultsTSRdd = fromCSV(
        "JoinOnTime.results",
        Schema("id" -> IntegerType, "price" -> DoubleType, "volume" -> LongType)
      )

      def test(rdd1: TimeSeriesRDD, rdd2: TimeSeriesRDD): Unit = {
        val joinedTSRdd = rdd1.futureLeftJoin(
          right = rdd2.deleteColumns("id"),
          tolerance = "100s"
        )
        assertEquals(joinedTSRdd, resultsTSRdd)
      }

      withPartitionStrategy(priceTSRdd, volumeTSRdd)(DEFAULT)(test)
    }
  }

  it should "join on time and key" in {
    withAllTimeType {
      init
      val resultsTSRdd = fromCSV(
        "JoinOnTimeAndKey.results",
        Schema("id" -> IntegerType, "price" -> DoubleType, "volume" -> LongType)
      )

      def test(rdd1: TimeSeriesRDD, rdd2: TimeSeriesRDD): Unit = {
        val joinedTSRdd = rdd1.futureLeftJoin(
          right = rdd2,
          tolerance = "100s",
          key = Seq("id")
        )
        assertEquals(joinedTSRdd, resultsTSRdd)
      }

      withPartitionStrategy(priceTSRdd, volumeTSRdd)(DEFAULT)(test)
    }
  }

  it should "join on time and key with strictLookahead" in {
    withAllTimeType {
      init
      val resultsTSRdd = fromCSV(
        "JoinOnTimeStrictLookahead.results",
        Schema("id" -> IntegerType, "price" -> DoubleType, "volume" -> LongType)
      )

      def test(rdd1: TimeSeriesRDD, rdd2: TimeSeriesRDD): Unit = {
        val joinedTSRdd = rdd1.futureLeftJoin(
          right = rdd2,
          tolerance = "100s",
          key = Seq("id"),
          strictLookahead = true
        )
        assertEquals(joinedTSRdd, resultsTSRdd)
      }

      withPartitionStrategy(priceTSRdd, volumeTSRdd)(DEFAULT)(test)
    }
  }

  it should "join on time and key with right table shifted" in {
    withAllTimeType {
      init
      val resultsTSRdd = fromCSV(
        "JoinOnTimeRightShifted.results",
        Schema("id" -> IntegerType, "price" -> DoubleType, "volume" -> LongType)
      )

      def test(rdd1: TimeSeriesRDD, rdd2: TimeSeriesRDD): Unit = {
        val joinedTSRdd = rdd1.futureLeftJoin(
          right = rdd2.shift(Windows.futureAbsoluteTime("200s")),
          tolerance = "100s",
          key = Seq("id")
        )
        assertEquals(joinedTSRdd, resultsTSRdd)
      }

      withPartitionStrategy(priceTSRdd, volumeTSRdd)(DEFAULT)(test)
    }
  }

  it should "join on time and key with column filtered" in {
    withAllTimeType {
      init
      val resultsTSRdd = fromCSV(
        "JoinOnTimeAndKeyColumnFiltered.results",
        Schema("id" -> IntegerType, "price" -> DoubleType, "volume" -> LongType)
      )

      def test(rdd1: TimeSeriesRDD, rdd2: TimeSeriesRDD): Unit = {
        val joinedTSRdd = rdd1.futureLeftJoin(
          right = rdd2.keepRows { r: Row => r.getAs[Int]("id") == 3 },
          tolerance = "50s",
          key = Seq("id")
        )
        assertEquals(joinedTSRdd, resultsTSRdd)
      }

      withPartitionStrategy(priceTSRdd, volumeTSRdd)(DEFAULT)(test)
    }
  }

  it should "join on time and key with row filtered" in {
    withAllTimeType {
      init
      val resultsTSRdd = fromCSV(
        "JoinOnTimeAndKeyRowFiltered.results",
        Schema("id" -> IntegerType, "price" -> DoubleType, "volume" -> LongType)
      )

      def test(rdd1: TimeSeriesRDD, rdd2: TimeSeriesRDD): Unit = {
        val joinedTSRdd = rdd1.futureLeftJoin(
          right = rdd2,
          tolerance = "50s",
          key = Seq("id")
        )

        assertEquals(joinedTSRdd, resultsTSRdd)
      }

      withPartitionStrategy(priceTSRdd, volumeTSRddRowFiltered)(DEFAULT)(test)
    }
  }

  it should "pass `JoinOnTimeAndMultipleKeys` test." in {
    withAllTimeType {
      init
      val resultsTSRdd = fromCSV(
        "JoinOnTimeAndMultipleKeys.results",
        Schema("id" -> IntegerType, "group" -> IntegerType, "price" -> DoubleType, "volume" -> LongType)
      )

      def test(priceTSRdd: TimeSeriesRDD, volumeTSRdd: TimeSeriesRDD): Unit = {
        val joinedTSRdd = priceTSRdd.leftJoin(volumeTSRdd, "0ns", Seq("id", "group"))
        assertEquals(joinedTSRdd, resultsTSRdd)
      }

      withPartitionStrategy(priceTSRddWithGroup, volumeTSRddWithGroup)(DEFAULT)(test)
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

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

import java.util.concurrent.TimeUnit

import com.twosigma.flint.timeseries.PartitionStrategy.{ ExtendEnd, MultiTimestampUnnormailzed, OneTimestampTightBound }
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.window.ShiftTimeWindow
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.tagobjects.Slow
import org.scalatest.prop.PropertyChecks

import scala.util.Random

class SummarizeWindowsSpec extends MultiPartitionSuite with TimeSeriesTestData with PropertyChecks {

  override val defaultResourceDir: String = "/timeseries/summarizewindows"

  private val volumeSchema = Schema("id" -> IntegerType, "volume" -> LongType, "v2" -> DoubleType)
  private val volumeWithGroupSchema = Schema(
    "id" -> IntegerType, "group" -> IntegerType, "volume" -> LongType, "v2" -> DoubleType
  )

  private def toTSRdd(clock: Seq[Int]): TimeSeriesRDD = {
    val clockDF = sqlContext.createDataFrame(
      sc.parallelize(clock).map { v => Row.fromSeq(Seq(v.toLong)) }, Schema()
    )
    TimeSeriesRDD.fromDF(clockDF)(isSorted = true, TimeUnit.NANOSECONDS)
  }

  "SummarizeWindows" should "pass `SummarizeSingleColumn` test." in {
    val resultsTSRdd = fromCSV(
      "SummarizeSingleColumn.results",
      Schema.append(volumeSchema, "volume_sum" -> DoubleType)
    )

    def test(rdd: TimeSeriesRDD): Unit = {
      val summarizedTSRdd = rdd.summarizeWindows(Windows.pastAbsoluteTime("100ns"), Summarizers.sum("volume"))
      assertEquals(summarizedTSRdd, resultsTSRdd)
    }

    {
      val volumeTSRdd = fromCSV("Volume.csv", volumeSchema)
      withPartitionStrategy(volumeTSRdd)(DEFAULT)(test)
    }
  }

  it should "pass `SummarizeSingleColumnPerKey` test." in {
    val resultsTSRdd = fromCSV(
      "SummarizeSingleColumnPerKey.results",
      Schema.append(volumeSchema, "volume_sum" -> DoubleType)
    )

    def test(rdd: TimeSeriesRDD): Unit = {
      val summarizedTSRdd = rdd.summarizeWindows(
        Windows.pastAbsoluteTime("100ns"), Summarizers.sum("volume"), Seq("id")
      )
      assertEquals(summarizedTSRdd, resultsTSRdd)
    }

    {
      val volumeTSRdd = fromCSV("Volume.csv", volumeSchema)
      withPartitionStrategy(volumeTSRdd)(DEFAULT)(test)
    }
  }

  it should "pass `SummarizeSingleColumnPerSeqOfKeys` test." in {
    val resultsTSRdd = fromCSV(
      "SummarizeSingleColumnPerSeqOfKeys.results",
      Schema.append(volumeWithGroupSchema, "volume_sum" -> DoubleType)
    )

    def test(volumeTSRdd: TimeSeriesRDD): Unit = {
      val summarizedTSRdd = volumeTSRdd.summarizeWindows(
        Windows.pastAbsoluteTime("100ns"), Summarizers.sum("volume"), Seq("id", "group")
      )
      assertEquals(summarizedTSRdd, resultsTSRdd)
    }

    {
      val volumeTSRdd = fromCSV("VolumeWithIndustryGroup.csv", volumeWithGroupSchema)
      withPartitionStrategy(volumeTSRdd)(NONE)(test)
    }
  }

  it should "pass `SummarizeWindowCountOverTwoTimeSeries` test." in {
    val resultsTSRdd = fromCSV(
      "SummarizeWindowCountOverTwoTimeSeries.results",
      Schema("count" -> LongType)
    )

    def test(left: TimeSeriesRDD, right: TimeSeriesRDD): Unit = {
      val summarizedTSRdd = left.summarizeWindows(Windows.pastAbsoluteTime("500ns"), Summarizers.count(), Seq(), right)
      assertEquals(summarizedTSRdd, resultsTSRdd)
    }

    {
      val clock1 = fromCSV("Clock1.csv", Schema())
      val clock2 = fromCSV("Clock2.csv", Schema())
      withPartitionStrategy(clock1, clock2)(DEFAULT)(test)
    }

  }

  it should "pass `SummarizeWindowCountOverSingleTimeSeries` test." in {
    val resultsTSRdd = fromCSV(
      "SummarizeWindowCountOverSingleTimeSeries.results",
      Schema("count" -> LongType)
    )

    def test(rdd: TimeSeriesRDD): Unit = {
      val summarizedTSRdd = rdd.summarizeWindows(Windows.pastAbsoluteTime("5ns"), Summarizers.count())
      assertEquals(summarizedTSRdd, resultsTSRdd)
    }

    {
      val clock = fromCSV("Clock.csv", Schema())
      withPartitionStrategy(clock)(DEFAULT)(test)
    }
  }

  it should "pass `SummarizeWindowCountOverSingleRandomTimeSeries` test." in {
    (1 to 10).foreach { seed =>
      val n = 100
      val step = 10
      val rand = new Random(seed)
      val clock = Seq.fill(n)(rand.nextInt(step * n)).sorted
      val clockTSRdd = toTSRdd(clock)

      val window = Windows.pastAbsoluteTime(s"$step ns")

      val summarized1 = clockTSRdd.summarizeWindows(
        window, Summarizers.count()
      ).rdd.map(_.getAs[Long]("count"))

      val summarized2 = clockTSRdd.summarizeWindows(
        window, Summarizers.count(), Seq(), clockTSRdd
      ).rdd.map(_.getAs[Long]("count"))

      val results = clock.map {
        t1 =>
          val (b, e) = window.of(t1.toLong)
          clock.count { t2 => t2 >= b && t2 <= e }
      }
      assert(summarized1.collect().deep == results.toArray.deep)
      assert(summarized2.collect().deep == results.toArray.deep)
    }
  }

  it should "pass `SummarizeWindowCountOverTwoRandomTimeSeries` test." in {
    (1 to 10).foreach { seed =>
      val n = 100
      val step = 10
      val rand = new Random(seed)
      val clock1 = Seq.fill(n)(rand.nextInt(step * n)).sorted
      val clock2 = Seq.fill(n)(rand.nextInt(step * n)).sorted
      val clockTSRdd1 = toTSRdd(clock1)
      val clockTSRdd2 = toTSRdd(clock2)

      val window = Windows.pastAbsoluteTime(s"$step ns")

      val summarized = clockTSRdd1.summarizeWindows(
        window, Summarizers.count(), Seq(), clockTSRdd2
      ).rdd.map(_.getAs[Long]("count"))

      val results = clock1.map {
        t1 =>
          val (b, e) = window.of(t1.toLong)
          clock2.count { t2 => t2 >= b && t2 <= e }
      }
      assert(summarized.collect().deep == results.toArray.deep)
    }
  }

  {
    val cycleWidth = cycleMetaData1.cycleWidth
    val intervalWidth = cycleMetaData1.intervalWidth
    val params = for (
      windowFn <- Seq(Windows.pastAbsoluteTime _, Windows.futureAbsoluteTime _);
      width <- Seq(
        0, cycleWidth / 2, cycleWidth, cycleWidth * 2, intervalWidth / 2, intervalWidth, intervalWidth * 2
      ).map(w => s"${w}ns");
      key <- Seq(Seq.empty, Seq("id"))
    ) yield Seq(windowFn(width), key)

    def addWindows(rdd: TimeSeriesRDD, param: Seq[Any]): TimeSeriesRDD = {
      val window = param(0).asInstanceOf[ShiftTimeWindow]
      val key = param(1).asInstanceOf[Seq[String]]
      rdd.addWindows(window, key)
    }

    def gen(): TimeSeriesRDD = cycleData1

    withPartitionStrategyAndParams(gen)(DEFAULT)(params)(addWindows)
  }

  it should "pass cycle data property test with other rdd" in {
    val testData1 = cycleData1
    val testData2 = cycleData2
    val cycleWidth = cycleMetaData1.cycleWidth
    val intervalWidth = cycleMetaData1.intervalWidth

    def addWindows(
      windowFn: (String) => ShiftTimeWindow,
      windowWidth: Long,
      key: Seq[String]
    )(rdd1: TimeSeriesRDD, rdd2: TimeSeriesRDD): TimeSeriesRDD = {
      val window = windowFn(s"${windowWidth}ns")
      rdd1.addWindows(window, key = key, otherRdd = rdd2)
    }

    for (
      windowFn <- Seq(Windows.pastAbsoluteTime _, Windows.futureAbsoluteTime _);
      width <- Seq(
        0, cycleWidth / 2, cycleWidth, cycleWidth * 2, intervalWidth / 2, intervalWidth, intervalWidth * 2
      );
      key <- Seq(Seq.empty, Seq("id"))
    ) {
      withPartitionStrategyCompare(
        testData1, testData2
      )(
        // Ideally we want to run DEFAULT partition strategies, but it's too freaking slow!
        NONE :+ MultiTimestampUnnormailzed
      )(
          addWindows(windowFn, width, key)
        )
    }
  }
}

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

import java.util.concurrent.TimeUnit

import com.twosigma.flint.timeseries.PartitionStrategy.{ ExtendEnd, MultiTimestampUnnormailzed, OneTimestampTightBound }
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.summarizer.LagSumSummarizerFactory
import com.twosigma.flint.timeseries.window.ShiftTimeWindow
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.tagobjects.Slow
import org.scalatest.prop.PropertyChecks

import scala.util.Random

class SummarizeWindowsSpec extends MultiPartitionSuite with TimeSeriesTestData with PropertyChecks with TimeTypeSuite {

  override val defaultResourceDir: String = "/timeseries/summarizewindows"

  private val volumeSchema = Schema("id" -> IntegerType, "volume" -> LongType, "v2" -> DoubleType)
  private val volumeWithGroupSchema = Schema(
    "id" -> IntegerType, "group" -> IntegerType, "volume" -> LongType, "v2" -> DoubleType
  )

  private val valueSchema = Schema(
    "value" -> DoubleType
  )

  private def toTSRdd(clock: Seq[Int]): TimeSeriesRDD = {
    val clockDF = sqlContext.createDataFrame(
      sc.parallelize(clock).map { v => Row.fromSeq(Seq(v.toLong)) }, Schema()
    )
    TimeSeriesRDD.fromDF(clockDF)(isSorted = true, TimeUnit.NANOSECONDS)
  }

  "SummarizeWindows" should "pass `SummarizeSingleColumn` test." in {
    withAllTimeType {
      val resultsTSRdd = fromCSV(
        "SummarizeSingleColumn.results",
        Schema.append(volumeSchema, "volume_sum" -> DoubleType)
      )

      def test(rdd: TimeSeriesRDD): Unit = {
        val summarizedTSRdd = rdd.summarizeWindows(Windows.pastAbsoluteTime("100s"), Summarizers.sum("volume"))
        assertEquals(summarizedTSRdd, resultsTSRdd)
      }

      {
        val volumeTSRdd = fromCSV("Volume.csv", volumeSchema)
        withPartitionStrategy(volumeTSRdd)(DEFAULT)(test)
      }
    }
  }

  it should "pass `SummarizeSingleColumnPerKey` test." in {
    withAllTimeType {
      val resultsTSRdd = fromCSV(
        "SummarizeSingleColumnPerKey.results",
        Schema.append(volumeSchema, "volume_sum" -> DoubleType)
      )

      def test(rdd: TimeSeriesRDD): Unit = {
        val summarizedTSRdd = rdd.summarizeWindows(
          Windows.pastAbsoluteTime("100s"), Summarizers.sum("volume"), Seq("id")
        )
        assertEquals(summarizedTSRdd, resultsTSRdd)
      }

      {
        val volumeTSRdd = fromCSV("Volume.csv", volumeSchema)
        withPartitionStrategy(volumeTSRdd)(DEFAULT)(test)
      }
    }
  }

  it should "pass `SummarizeSingleColumnPerSeqOfKeys` test." in {
    withAllTimeType {
      val resultsTSRdd = fromCSV(
        "SummarizeSingleColumnPerSeqOfKeys.results",
        Schema.append(volumeWithGroupSchema, "volume_sum" -> DoubleType)
      )

      def test(volumeTSRdd: TimeSeriesRDD): Unit = {
        val summarizedTSRdd = volumeTSRdd.summarizeWindows(
          Windows.pastAbsoluteTime("100s"), Summarizers.sum("volume"), Seq("id", "group")
        )
        assertEquals(summarizedTSRdd, resultsTSRdd)
      }

      {
        val volumeTSRdd = fromCSV("VolumeWithIndustryGroup.csv", volumeWithGroupSchema)
        withPartitionStrategy(volumeTSRdd)(NONE)(test)
      }
    }
  }

  it should "pass `SummarizeWindowCountOverSingleTimeSeries` test." in {
    withAllTimeType {
      val resultsTSRdd = fromCSV(
        "SummarizeWindowCountOverSingleTimeSeries.results",
        Schema("count" -> LongType)
      )

      def test(rdd: TimeSeriesRDD): Unit = {
        val summarizedTSRdd = rdd.summarizeWindows(Windows.pastAbsoluteTime("5s"), Summarizers.count())
        assertEquals(summarizedTSRdd, resultsTSRdd)
      }

      {
        val clock = fromCSV("Clock.csv", Schema())
        withPartitionStrategy(clock)(DEFAULT)(test)
      }
    }
  }

  it should "pass `SummarizeWindowSumOverSingleTimeSeries` test." in {
    withAllTimeType {
      val resultsTSRdd = fromCSV(
        "SummarizeWindowSumOverSingleTimeSeries.results",
        Schema.append(valueSchema, "lagSum" -> DoubleType, "sum" -> DoubleType)
      )

      def test(rdd: TimeSeriesRDD): Unit = {
        val summarizedTSRdd = rdd.summarizeWindows(
          Windows.pastAbsoluteTime("3s"),
          LagSumSummarizerFactory("value", "3s")
        )
        assertEquals(summarizedTSRdd, resultsTSRdd)
      }

      {
        val valueTSRDD = fromCSV("Value.csv", valueSchema)
        withPartitionStrategy(valueTSRDD)(DEFAULT)(test)
      }
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

      val results = clock.map {
        t1 =>
          val (b, e) = window.of(t1.toLong)
          clock.count { t2 => t2 >= b && t2 <= e }
      }
      assert(summarized1.collect().deep == results.toArray.deep)
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

    withPartitionStrategyAndParams(gen)("addWindows")(DEFAULT)(params)(addWindows)

  }
}

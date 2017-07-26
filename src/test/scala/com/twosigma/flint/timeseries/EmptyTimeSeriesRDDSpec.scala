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

import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{ col, lit }
import com.twosigma.flint.timeseries.Summarizers
import com.twosigma.flint.timeseries.Clocks
import com.twosigma.flint.timeseries.Windows
import com.twosigma.flint.timeseries.summarize.summarizer.LagSumSummarizerFactory

class EmptyTimeSeriesRDDSpec extends TimeSeriesSuite {
  "TimeSeriesRDD" should "support operations on empty TimeSeriesRDD" in {
    def assertEmpty(tsrdd: TimeSeriesRDD): Unit = {
      assert(tsrdd.count() == 0)
      assert(tsrdd.orderedRdd.getPartitionRanges.isEmpty)
      assert(tsrdd.orderedRdd.getNumPartitions == 0)
      assert(tsrdd.orderedRdd.partitions.isEmpty)
    }

    val schema = StructType(Seq(
      StructField("time", LongType),
      StructField("id", LongType),
      StructField("v1", DoubleType),
      StructField("v2", DoubleType)
    ))

    val emptyDFs: Seq[DataFrame] = Seq(
      sc.range(0, 1000L).map{ i =>
        Row(i, i, i.toDouble, i.toDouble)
      }.filter(r => r.getLong(0) > 2000L),
      sc.emptyRDD[Row]
    ).map(sqlContext.createDataFrame(_, schema))

    val emptyTSRdds = emptyDFs.flatMap {
      df =>
        Seq(
          TimeSeriesRDD.fromDF(df)(isSorted = true, TimeUnit.NANOSECONDS),
          TimeSeriesRDD.fromDF(df)(isSorted = false, TimeUnit.NANOSECONDS)
        )
    }

    val df2 = sqlContext.createDataFrame(
      sc.range(0, 1000L).map { i => Row(i, i, i.toDouble, i.toDouble) },
      schema
    )
    val tsrdd2 = TimeSeriesRDD.fromDF(df2)(isSorted = true, TimeUnit.NANOSECONDS)

    val summarizers = Seq(
      Summarizers.count(),
      Summarizers.sum("v1"),
      Summarizers.weightedMeanTest("v1", "v2"),
      Summarizers.mean("v1"),
      Summarizers.stddev("v1"),
      Summarizers.variance("v1"),
      Summarizers.covariance("v1", "v2"),
      Summarizers.zScore("v1", true),
      Summarizers.nthMoment("v1", 1),
      Summarizers.nthCentralMoment("v1", 1),
      Summarizers.correlation("v1", "v2"),
      Summarizers.OLSRegression("v1", Seq("v2")),
      Summarizers.quantile("v1", Seq(0.1, 0.25, 0.5, 0.95)),
      Summarizers.compose(Summarizers.mean("v1"), Summarizers.stddev("v1")),
      Summarizers.stack(Summarizers.mean("v1"), Summarizers.mean("v1")),
      Summarizers.exponentialSmoothing("v1"),
      Summarizers.min("v1"),
      Summarizers.max("v1"),
      Summarizers.product("v1"),
      Summarizers.dotProduct("v1", "v2"),
      Summarizers.geometricMean("v1"),
      Summarizers.skewness("v1"),
      Summarizers.kurtosis("v1")
    )

    val overlappableSummarizers = Seq(
      LagSumSummarizerFactory("v1", "100ns"),
      Summarizers.driscollKraayRegression("v1", Seq("v2"))
    )

    for (tsrdd <- emptyTSRdds) {
      assertEmpty(tsrdd)
      assert(tsrdd.toDF.count() == 0)
      assert(tsrdd.collect().isEmpty)

      // Unary operators
      assertEmpty(tsrdd.repartition(10))
      assertEmpty(tsrdd.coalesce(10))
      assertEmpty(tsrdd.keepRows { _ => true })
      assertEmpty(tsrdd.deleteRows { _ => true })
      assertEmpty(tsrdd.keepColumns("id"))
      assertEmpty(tsrdd.deleteColumns("id"))
      assertEmpty(tsrdd.renameColumns("id" -> "id2"))

      assertEmpty(tsrdd.addColumns("v" -> DoubleType -> { _ => 0.0 }))
      assertEmpty(tsrdd.addColumnsForCycle("v" -> DoubleType -> { _: Seq[Row] => Seq.empty }))
      for (summarizer <- summarizers) {
        assertEmpty(tsrdd.summarizeCycles(summarizer))
        assertEmpty(tsrdd.summarizeCycles(summarizer, key = "id"))
        assertEmpty(tsrdd.summarizeIntervals(Clocks.uniform(sc, "1day"), summarizer))
        assertEmpty(tsrdd.summarizeIntervals(Clocks.uniform(sc, "1day"), summarizer, key = Seq("id")))
        assertEmpty(tsrdd.summarize(summarizer))
        assertEmpty(tsrdd.summarize(summarizer, key = Seq("id")))
        assertEmpty(tsrdd.addSummaryColumns(summarizer))
        assertEmpty(tsrdd.addSummaryColumns(summarizer, key = Seq("id")))
      }

      for (summarizer <- overlappableSummarizers) {
        assertEmpty(tsrdd.summarize(summarizer))
        assertEmpty(tsrdd.summarize(summarizer, key = Seq("id")))
      }

      assertEmpty(tsrdd.cast("id" -> IntegerType))
      assertEmpty(tsrdd.shift(Windows.pastAbsoluteTime("100ns")))
      assertEmpty(tsrdd.shift(Windows.futureAbsoluteTime("100ns")))
      assertEmpty(tsrdd.setTime({ _ => 1000L }, null))

      tsrdd.validate()

      // Binary operators

      // Join

      assertEmpty(tsrdd.leftJoin(tsrdd, tolerance = "100ns", rightAlias = "right"))
      assertEmpty(tsrdd.futureLeftJoin(tsrdd, tolerance = "100ns", rightAlias = "right"))

      assertEmpty(tsrdd.leftJoin(tsrdd2, tolerance = "100ns", rightAlias = "right"))
      assertEmpty(tsrdd.futureLeftJoin(tsrdd2, tolerance = "100ns", rightAlias = "right"))

      val expectedJoined = tsrdd2.addColumns(
        "right_id" -> LongType -> { _ => null },
        "right_v1" -> DoubleType -> { _ => null },
        "right_v2" -> DoubleType -> { _ => null }
      )

      assertIdentical(
        tsrdd2.leftJoin(tsrdd, tolerance = "100ns", rightAlias = "right"),
        expectedJoined
      )

      assertIdentical(
        tsrdd2.futureLeftJoin(tsrdd, tolerance = "100ns", rightAlias = "right"),
        expectedJoined
      )

      // Merge
      assertEmpty(tsrdd.merge(tsrdd))
      assertIdentical(tsrdd2, tsrdd2.merge(tsrdd))
      assertIdentical(tsrdd2, tsrdd.merge(tsrdd2))

      // Empty table window join non-empty table
      for (summarizer <- summarizers) {
        assertEmpty(tsrdd.summarizeWindows(Windows.pastAbsoluteTime("100ns"), summarizer))
        assertEmpty(tsrdd.summarizeWindows(Windows.pastAbsoluteTime("100ns"), summarizer, key = Seq("id")))

        assertEmpty(tsrdd.summarizeWindows(Windows.pastAbsoluteTime("100ns"), summarizer, otherRdd = tsrdd2))
        assertEmpty(
          tsrdd.summarizeWindows(
            Windows.pastAbsoluteTime("100ns"), summarizer, key = Seq("id"), otherRdd = tsrdd2
          )
        )
      }

      // Empty table self window with overlappable
      for (summarizer <- overlappableSummarizers) {
        assertEmpty(tsrdd.summarizeWindows(Windows.pastAbsoluteTime("100ns"), summarizer))
        assertEmpty(tsrdd.summarizeWindows(Windows.pastAbsoluteTime("100ns"), summarizer, key = Seq("id")))
      }

      // Non-empty table window join empty table.
      assertIdentical(
        tsrdd2.summarizeWindows(
          Windows.pastAbsoluteTime("100ns"), Summarizers.count(), otherRdd = tsrdd
        ),
        tsrdd2.addColumns("count" -> LongType -> { _ => 0L })
      )

      assertIdentical(
        tsrdd2.summarizeWindows(
          Windows.pastAbsoluteTime("100ns"), Summarizers.count(), key = Seq("id"), otherRdd = tsrdd
        ),
        tsrdd2.addColumns("count" -> LongType -> { _ => 0L })
      )
    }
  }
}

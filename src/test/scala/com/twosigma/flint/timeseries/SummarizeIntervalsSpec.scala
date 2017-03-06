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
import org.apache.spark.sql.types.{ DoubleType, LongType, IntegerType }

class SummarizeIntervalsSpec extends MultiPartitionSuite {

  override val defaultResourceDir: String = "/timeseries/summarizeintervals"

  "SummarizeInterval" should "pass `SummarizeSingleColumn` test." ignore {
    val clockTSRdd = fromCSV("Clock.csv", Schema())
    val resultTSRdd = fromCSV("SummarizeSingleColumn.results", Schema("volume_sum" -> DoubleType))

    def test(rdd: TimeSeriesRDD): Unit = {
      val summarizedVolumeTSRdd = rdd.summarizeIntervals(clockTSRdd, Summarizers.sum("volume"))
      assert(summarizedVolumeTSRdd.collect().deep == resultTSRdd.collect().deep)
    }

    {
      val volumeTSRdd = fromCSV(
        "Volume.csv", Schema("id" -> IntegerType, "volume" -> LongType, "v2" -> DoubleType)
      )
      withPartitionStrategy(volumeTSRdd)(DEFAULT)(test)
    }

  }

  it should "pass `SummarizeSingleColumnPerKey` test, i.e. with additional a single key." ignore {
    val clockTSRdd = fromCSV("Clock.csv", Schema())
    val resultTSRdd = fromCSV(
      "SummarizeSingleColumnPerKey.results",
      Schema("id" -> IntegerType, "volume_sum" -> DoubleType)
    )

    val result2TSRdd = fromCSV(
      "SummarizeV2PerKey.results",
      Schema("id" -> IntegerType, "v2_sum" -> DoubleType)
    )

    def test(rdd: TimeSeriesRDD): Unit = {
      val summarizedVolumeTSRdd = rdd.summarizeIntervals(clockTSRdd, Summarizers.sum("volume"), Seq("id"))

      // TODO: we should do this instead of the following 3 asserts
      // assert(summarizedVolumeTSRdd.collect().deep == resultTSRdd.collect().deep)
      assert(summarizedVolumeTSRdd.keepRows(_.getAs[Int]("id") == 3).collect().deep ==
        resultTSRdd.keepRows(_.getAs[Int]("id") == 3).collect().deep)
      assert(summarizedVolumeTSRdd.keepRows(_.getAs[Int]("id") == 7).collect().deep ==
        resultTSRdd.keepRows(_.getAs[Int]("id") == 7).collect().deep)
      assert(summarizedVolumeTSRdd.keepColumns(TimeSeriesRDD.timeColumnName).collect().deep ==
        resultTSRdd.keepColumns(TimeSeriesRDD.timeColumnName).collect().deep)

      val summarizedV2TSRdd = rdd.summarizeIntervals(clockTSRdd, Summarizers.sum("v2"), Seq("id"))
      assert(summarizedV2TSRdd.keepRows(_.getAs[Int]("id") == 3).collect().deep ==
        result2TSRdd.keepRows(_.getAs[Int]("id") == 3).collect().deep)
      assert(summarizedV2TSRdd.keepRows(_.getAs[Int]("id") == 7).collect().deep ==
        result2TSRdd.keepRows(_.getAs[Int]("id") == 7).collect().deep)
      assert(summarizedV2TSRdd.keepColumns(TimeSeriesRDD.timeColumnName).collect().deep ==
        result2TSRdd.keepColumns(TimeSeriesRDD.timeColumnName).collect().deep)
    }

    {
      val volumeTSRdd = fromCSV(
        "Volume.csv", Schema("id" -> IntegerType, "volume" -> LongType, "v2" -> DoubleType)
      )
      withPartitionStrategy(volumeTSRdd)(DEFAULT)(test)
    }

  }

  it should "pass `SummarizeSingleColumnPerSeqOfKeys` test, i.e. with additional a sequence of keys." ignore {
    val clockTSRdd = fromCSV("Clock.csv", Schema())
    val resultTSRdd = fromCSV(
      "SummarizeSingleColumnPerSeqOfKeys.results",
      Schema("id" -> IntegerType, "group" -> IntegerType, "volume_sum" -> DoubleType)
    )

    def test(rdd: TimeSeriesRDD): Unit = {
      val summarizedVolumeTSRdd = rdd.summarizeIntervals(
        clockTSRdd,
        Summarizers.sum("volume"), Seq("id", "group")
      )

      // TODO: we should do this instead of the following 3 asserts
      // assert(summarizedVolumeTSRdd.collect().deep == resultTSRdd.collect().deep)
      assert(summarizedVolumeTSRdd.keepRows(_.getAs[Int]("id") == 3).collect().deep ==
        resultTSRdd.keepRows(_.getAs[Int]("id") == 3).collect().deep)
      assert(summarizedVolumeTSRdd.keepRows(_.getAs[Int]("id") == 7).collect().deep ==
        resultTSRdd.keepRows(_.getAs[Int]("id") == 7).collect().deep)
      assert(summarizedVolumeTSRdd.keepColumns(TimeSeriesRDD.timeColumnName).collect().deep ==
        resultTSRdd.keepColumns(TimeSeriesRDD.timeColumnName).collect().deep)
    }

    {
      val volumeTSRdd = fromCSV(
        "VolumeWithIndustryGroup.csv",
        Schema("id" -> IntegerType, "group" -> IntegerType, "volume" -> LongType, "v2" -> DoubleType)
      )
      withPartitionStrategy(volumeTSRdd)(DEFAULT)(test)
    }
  }
}

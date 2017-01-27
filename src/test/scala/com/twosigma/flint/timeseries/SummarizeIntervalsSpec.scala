/*
 *  Copyright 2015-2016 TWO SIGMA OPEN SOURCE, LLC
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
import com.twosigma.flint.{ SpecUtils, SharedSparkContext }
import org.apache.spark.sql.types.{ DoubleType, LongType, IntegerType, StructType }
import org.scalatest.FlatSpec

class SummarizeIntervalsSpec extends FlatSpec with SharedSparkContext {

  private val defaultPartitionParallelism: Int = 5

  private val resourceDir: String = "/timeseries/summarizeintervals"

  private def from(filename: String, schema: StructType): TimeSeriesRDD =
    SpecUtils.withResource(s"$resourceDir/$filename") { source =>
      CSV.from(
        sqlContext,
        s"file://$source",
        header = true,
        sorted = true,
        schema = schema
      ).repartition(defaultPartitionParallelism)
    }

  "SummarizeInterval" should "pass `SummarizeSingleColumn` test." in {
    val volumeTSRdd = from(
      "Volume.csv",
      Schema("tid" -> IntegerType, "volume" -> LongType, "v2" -> DoubleType)
    )
    val clockTSRdd = from("Clock.csv", Schema())
    val resultTSRdd = from("SummarizeSingleColumn.results", Schema("volume_sum" -> DoubleType))
    val summarizedVolumeTSRdd = volumeTSRdd.summarizeIntervals(clockTSRdd, Summarizers.sum("volume"))

    assert(summarizedVolumeTSRdd.collect().deep == resultTSRdd.collect().deep)
  }

  it should "pass `SummarizeSingleColumnPerKey` test, i.e. with additional a single key." in {
    val volumeTSRdd = from(
      "Volume.csv",
      Schema("tid" -> IntegerType, "volume" -> LongType, "v2" -> DoubleType)
    )
    val clockTSRdd = from("Clock.csv", Schema())
    val resultTSRdd = from(
      "SummarizeSingleColumnPerKey.results",
      Schema("tid" -> IntegerType, "volume_sum" -> DoubleType)
    )
    val summarizedVolumeTSRdd = volumeTSRdd.summarizeIntervals(clockTSRdd, Summarizers.sum("volume"), Seq("tid"))

    // TODO: we should do this instead of the following 3 asserts
    // assert(summarizedVolumeTSRdd.collect().deep == resultTSRdd.collect().deep)
    assert(summarizedVolumeTSRdd.keepRows(_.getAs[Int]("tid") == 3).collect().deep ==
      resultTSRdd.keepRows(_.getAs[Int]("tid") == 3).collect().deep)
    assert(summarizedVolumeTSRdd.keepRows(_.getAs[Int]("tid") == 7).collect().deep ==
      resultTSRdd.keepRows(_.getAs[Int]("tid") == 7).collect().deep)
    assert(summarizedVolumeTSRdd.keepColumns(TimeSeriesRDD.timeColumnName).collect().deep ==
      resultTSRdd.keepColumns(TimeSeriesRDD.timeColumnName).collect().deep)

    val result2TSRdd = from(
      "SummarizeV2PerKey.results",
      Schema("tid" -> IntegerType, "v2_sum" -> DoubleType)
    )
    val summarizedV2TSRdd = volumeTSRdd.summarizeIntervals(clockTSRdd, Summarizers.sum("v2"), Seq("tid"))
    assert(summarizedV2TSRdd.keepRows(_.getAs[Int]("tid") == 3).collect().deep ==
      result2TSRdd.keepRows(_.getAs[Int]("tid") == 3).collect().deep)
    assert(summarizedV2TSRdd.keepRows(_.getAs[Int]("tid") == 7).collect().deep ==
      result2TSRdd.keepRows(_.getAs[Int]("tid") == 7).collect().deep)
    assert(summarizedV2TSRdd.keepColumns(TimeSeriesRDD.timeColumnName).collect().deep ==
      result2TSRdd.keepColumns(TimeSeriesRDD.timeColumnName).collect().deep)
  }

  it should "pass `SummarizeSingleColumnPerSeqOfKeys` test, i.e. with additional a sequence of keys." in {
    val volumeTSRdd = from(
      "VolumeWithIndustryGroup.csv",
      Schema("tid" -> IntegerType, "group" -> IntegerType, "volume" -> LongType, "v2" -> DoubleType)
    )
    val clockTSRdd = from("Clock.csv", Schema())
    val resultTSRdd = from(
      "SummarizeSingleColumnPerSeqOfKeys.results",
      Schema("tid" -> IntegerType, "group" -> IntegerType, "volume_sum" -> DoubleType)
    )
    val summarizedVolumeTSRdd = volumeTSRdd.summarizeIntervals(
      clockTSRdd,
      Summarizers.sum("volume"), Seq("tid", "group")
    )

    // TODO: we should do this instead of the following 3 asserts
    // assert(summarizedVolumeTSRdd.collect().deep == resultTSRdd.collect().deep)
    assert(summarizedVolumeTSRdd.keepRows(_.getAs[Int]("tid") == 3).collect().deep ==
      resultTSRdd.keepRows(_.getAs[Int]("tid") == 3).collect().deep)
    assert(summarizedVolumeTSRdd.keepRows(_.getAs[Int]("tid") == 7).collect().deep ==
      resultTSRdd.keepRows(_.getAs[Int]("tid") == 7).collect().deep)
    assert(summarizedVolumeTSRdd.keepColumns(TimeSeriesRDD.timeColumnName).collect().deep ==
      resultTSRdd.keepColumns(TimeSeriesRDD.timeColumnName).collect().deep)
  }
}

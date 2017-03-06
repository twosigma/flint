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

class SummarizeCyclesSpec extends MultiPartitionSuite {

  override val defaultResourceDir: String = "/timeseries/summarizecycles"
  private val volumeSchema = Schema("id" -> IntegerType, "volume" -> LongType, "v2" -> DoubleType)
  private val volume2Schema = volumeSchema
  private val volumeWithGroupSchema = Schema(
    "id" -> IntegerType, "group" -> IntegerType, "volume" -> LongType, "v2" -> DoubleType
  )

  "SummarizeCycles" should "pass `SummarizeSingleColumn` test." in {
    val resultTSRdd = fromCSV("SummarizeSingleColumn.results", Schema("volume_sum" -> DoubleType))

    def test(rdd: TimeSeriesRDD): Unit = {
      val summarizedVolumeTSRdd = rdd.summarizeCycles(Summarizers.sum("volume"))
      assert(summarizedVolumeTSRdd.collect().deep == resultTSRdd.collect().deep)
    }

    {
      val volumeTSRdd = fromCSV("Volume.csv", volumeSchema)
      withPartitionStrategy(volumeTSRdd)(DEFAULT)(test)
    }
  }

  it should "pass `SummarizeSingleColumnPerKey` test, i.e. with additional a single key." in {
    val resultTSRdd = fromCSV(
      "SummarizeSingleColumnPerKey.results",
      Schema("id" -> IntegerType, "volume_sum" -> DoubleType)
    )

    def test(rdd: TimeSeriesRDD): Unit = {
      val summarizedVolumeTSRdd = rdd.summarizeCycles(Summarizers.sum("volume"), Seq("id"))

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
      val volumeTSRdd = fromCSV("Volume2.csv", volume2Schema)
      withPartitionStrategy(volumeTSRdd)(DEFAULT)(test)
    }
  }

  it should "pass `SummarizeSingleColumnPerSeqOfKeys` test, i.e. with additional a sequence of keys." in {
    val resultTSRdd = fromCSV(
      "SummarizeSingleColumnPerSeqOfKeys.results",
      Schema("id" -> IntegerType, "group" -> IntegerType, "volume_sum" -> DoubleType)
    )

    def test(rdd: TimeSeriesRDD): Unit = {
      val summarizedVolumeTSRdd = rdd.summarizeCycles(Summarizers.sum("volume"), Seq("id", "group"))

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
      val volumeTSRdd = fromCSV("VolumeWithIndustryGroup.csv", volumeWithGroupSchema)
      withPartitionStrategy(volumeTSRdd)(DEFAULT)(test)
    }
  }
}

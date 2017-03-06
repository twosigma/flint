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
import org.apache.spark.sql.types.{ LongType, IntegerType, DoubleType }

class LeftJoinSpec extends MultiPartitionSuite {
  override val defaultResourceDir: String = "/timeseries/leftjoin"

  "LeftJoin" should "pass `JoinOnTime` test." in {
    val volumeTSRdd = fromCSV("Volume.csv", Schema("id" -> IntegerType, "volume" -> LongType))
    val resultsTSRdd = fromCSV(
      "JoinOnTime.results", Schema("id" -> IntegerType, "price" -> DoubleType, "volume" -> LongType)
    )

    def test(priceTSRdd: TimeSeriesRDD, volumneTSRdd: TimeSeriesRDD): Unit = {
      val joinedTSRdd = priceTSRdd.leftJoin(volumeTSRdd, "0ns", Seq("id"))
      assert(resultsTSRdd.schema == joinedTSRdd.schema)
      assert(resultsTSRdd.collect().deep == joinedTSRdd.collect().deep)
    }

    {
      val priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
      withPartitionStrategy(priceTSRdd, volumeTSRdd)(DEFAULT)(test)
    }
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
      assert(resultsTSRdd.schema == joinedTSRdd.schema)
      assert(resultsTSRdd.collect().deep == joinedTSRdd.collect().deep)
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

    def test(rdd1: TimeSeriesRDD, rdd2: TimeSeriesRDD): Unit = {
      val joinedTSRdd = rdd1.leftJoin(rdd2, "0ns", Seq("id", "group"))
      assert(resultsTSRdd.schema == joinedTSRdd.schema)
      assert(resultsTSRdd.collect().deep == joinedTSRdd.collect().deep)
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
}

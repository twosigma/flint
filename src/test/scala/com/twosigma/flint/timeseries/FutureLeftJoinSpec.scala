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
import org.apache.spark.sql.types._

class FutureLeftJoinSpec extends TimeSeriesSuite {

  override val defaultResourceDir: String = "/timeseries/futureleftjoin"

  "FutureLeftJoin" should "pass `JoinOnTime` test." in {
    val priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
    val volumeTSRdd = fromCSV("Volume.csv", Schema("id" -> IntegerType, "volume" -> LongType))
    val resultsTSRdd = fromCSV(
      "JoinOnTime.results",
      Schema("id" -> IntegerType, "price" -> DoubleType, "volume" -> LongType, "time2" -> LongType)
    )
    val joinedTSRdd = priceTSRdd.futureLeftJoin(
      right = volumeTSRdd.addColumns(
        "time2" -> LongType -> { _.getAs[Long](TimeSeriesRDD.timeColumnName) }
      ),
      tolerance = "100ns",
      key = Seq("id"),
      strictLookahead = true
    )
    assert(resultsTSRdd.schema == joinedTSRdd.schema)
    assert(resultsTSRdd.collect().deep == joinedTSRdd.collect().deep)
  }

  it should "pass `JoinOnTimeAndMultipleKeys` test." in {
    val priceTSRdd = fromCSV(
      "PriceWithIndustryGroup.csv",
      Schema("id" -> IntegerType, "group" -> IntegerType, "price" -> DoubleType)
    )
    val volumeTSRdd = fromCSV(
      "VolumeWithIndustryGroup.csv",
      Schema("id" -> IntegerType, "group" -> IntegerType, "volume" -> LongType)
    )
    val resultsTSRdd = fromCSV(
      "JoinOnTimeAndMultipleKeys.results",
      Schema("id" -> IntegerType, "group" -> IntegerType, "price" -> DoubleType, "volume" -> LongType)
    )
    val joinedTSRdd = priceTSRdd.leftJoin(volumeTSRdd, "0ns", Seq("id", "group"))
    assert(resultsTSRdd.schema == joinedTSRdd.schema)
    assert(resultsTSRdd.collect().deep == joinedTSRdd.collect().deep)
  }
}

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
import com.twosigma.flint.{ SpecUtils, SharedSparkContext }
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ DoubleType, IntegerType }
import org.scalatest.FlatSpec

class SummarizerSpec extends FlatSpec with SharedSparkContext {

  "SummarizerFactory" should "support alias." in {
    SpecUtils.withResource("/timeseries/csv/Price.csv") { source =>
      val expectedSchema = Schema("C1" -> IntegerType, "C2" -> DoubleType)
      val timeseriesRdd = CSV.from(sqlContext, "file://" + source, sorted = true, schema = expectedSchema)
      assert(timeseriesRdd.schema == expectedSchema)
      val result: Row = timeseriesRdd.summarize(Summarizers.count().prefix("alias")).first()
      assert(result.getAs[Long]("alias_count") == timeseriesRdd.count())
    }
  }
}

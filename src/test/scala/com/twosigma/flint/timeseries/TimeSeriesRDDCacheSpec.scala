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
import com.twosigma.flint.{ SharedSparkContext, SpecUtils }
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ DoubleType, IntegerType }
import org.scalatest.concurrent.Timeouts
import org.scalatest.FlatSpec
import org.scalatest.tagobjects.Slow
import org.scalatest.time.{ Second, Span }

class TimeSeriesRDDCacheSpec extends FlatSpec with SharedSparkContext with Timeouts {

  "TimeSeriesRDD" should "correctly cache data" taggedAs Slow in {
    SpecUtils.withResource("/timeseries/csv/Price.csv") { source =>
      val priceSchema = Schema("tid" -> IntegerType, "price" -> DoubleType)
      val timeSeriesRdd = CSV.from(sqlContext, "file://" + source, sorted = true, schema = priceSchema)

      val slowTimeSeriesRdd = timeSeriesRdd.addColumns("new_column" -> DoubleType -> {
        row: Row =>
          Thread.sleep(500L)
          row.getAs[Double]("price") + 1.0
      })

      slowTimeSeriesRdd.cache()
      assert(slowTimeSeriesRdd.count() == 12)

      // this test succeeds only if the rdd is correctly cached
      failAfter(Span(1, Second)) {
        assert(slowTimeSeriesRdd.collect().length == 12)
      }
    }
  }
}

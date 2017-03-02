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

package com.twosigma.flint.timeseries.summarize.summarizer

import com.twosigma.flint.timeseries.{ TimeSeriesSuite, Summarizers, CSV, TimeSeriesRDD }
import com.twosigma.flint.timeseries.row.Schema
import org.apache.spark.sql.types.{ DoubleType, IntegerType, StructType }

class CompositeSummarizerSpec extends TimeSeriesSuite {
  // Reuse mean summarizer data
  override val defaultResourceDir: String = "/timeseries/summarize/summarizer/meansummarizer"

  "CompositeSummarizer" should "compute `mean` and `stddev` correctly" in {
    val priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
    val result = priceTSRdd.summarize(
      Summarizers.compose(Summarizers.mean("price"), Summarizers.stddev("price"))
    )
    val row = result.first()

    assert(row.getAs[Double]("price_mean") === 3.25)
    assert(row.getAs[Double]("price_stddev") === 1.8027756377319946)
  }

  it should "throw exception for conflicting output columns" in {
    intercept[Exception] {
      val priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
      priceTSRdd.summarize(Summarizers.compose(Summarizers.mean("price"), Summarizers.mean("price")))
    }
  }

  it should "handle conflicting output columns using prefix" in {
    val priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
    val result = priceTSRdd.summarize(
      Summarizers.compose(Summarizers.mean("price"), Summarizers.mean("price").prefix("prefix"))
    )

    val row = result.first()

    assert(row.getAs[Double]("price_mean") === 3.25)
    assert(row.getAs[Double]("prefix_price_mean") === 3.25)
  }
}

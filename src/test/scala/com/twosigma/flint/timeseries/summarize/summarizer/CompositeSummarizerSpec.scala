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

import com.twosigma.flint.timeseries.{ CSV, Summarizers, TimeSeriesRDD, TimeSeriesSuite }
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.SummarizerSuite
import org.apache.spark.sql.types.{ DoubleType, IntegerType, StructType }

class CompositeSummarizerSpec extends SummarizerSuite {
  // Reuse mean summarizer data
  override val defaultResourceDir: String = "/timeseries/summarize/summarizer/meansummarizer"

  var priceTSRdd: TimeSeriesRDD = _

  lazy val init: Unit = {
    priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
  }

  "CompositeSummarizer" should "compute `mean` and `stddev` correctly" in {
    init
    val result = priceTSRdd.summarize(
      Summarizers.compose(Summarizers.mean("price"), Summarizers.stddev("price"))
    )
    val row = result.first()

    assert(row.getAs[Double]("price_mean") === 3.25)
    assert(row.getAs[Double]("price_stddev") === 1.8027756377319946)
  }

  it should "throw exception for conflicting output columns" in {
    init
    intercept[Exception] {
      priceTSRdd.summarize(Summarizers.compose(Summarizers.mean("price"), Summarizers.mean("price")))
    }
  }

  it should "handle conflicting output columns using prefix" in {
    init
    val result = priceTSRdd.summarize(
      Summarizers.compose(Summarizers.mean("price"), Summarizers.mean("price").prefix("prefix"))
    )

    val row = result.first()

    assert(row.getAs[Double]("price_mean") === 3.25)
    assert(row.getAs[Double]("prefix_price_mean") === 3.25)
  }

  it should "handle null values" in {
    init
    val inputWithNull = insertNullRows(priceTSRdd, "price")
    val row = inputWithNull.summarize(
      Summarizers.compose(
        Summarizers.count(),
        Summarizers.count("id"),
        Summarizers.count("price")
      )
    ).first()

    val count = priceTSRdd.count()
    assert(row.getAs[Long]("count") == 2 * count)
    assert(row.getAs[Long]("id_count") == 2 * count)
    assert(row.getAs[Long]("price_count") == count)
  }
}

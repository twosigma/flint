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

import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.{ TimeSeriesSuite, Summarizers }
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ DoubleType, IntegerType }

class StandardDeviationSummarizerSpec extends TimeSeriesSuite {
  // It is by intention to reuse the files
  override val defaultResourceDir: String = "/timeseries/summarize/summarizer/meansummarizer"

  "StandardDeviationSummarizer" should "compute `stddev` correctly" in {
    val priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType)).addColumns(
      "price2" -> DoubleType -> { r: Row => r.getAs[Double]("price") },
      "price3" -> DoubleType -> { r: Row => -r.getAs[Double]("price") },
      "price4" -> DoubleType -> { r: Row => r.getAs[Double]("price") * 2 },
      "price5" -> DoubleType -> { r: Row => 0d }
    )

    val result = priceTSRdd.summarize(Summarizers.stddev("price")).first()
    assert(result.getAs[Double]("price_stddev") === 1.802775638)
  }
}

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
import com.twosigma.flint.timeseries.summarize.SummarizerSuite
import com.twosigma.flint.timeseries.{ Summarizers, TimeSeriesSuite }
import org.apache.spark.sql.types.{ DoubleType, IntegerType }

class MeanSummarizerSpec extends SummarizerSuite {

  override val defaultResourceDir: String = "/timeseries/summarize/summarizer/meansummarizer"

  "MeanSummarizer" should "compute `mean` correctly" in {
    val priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
    val result = priceTSRdd.summarize(Summarizers.mean("price")).first()
    assert(result.getAs[Double]("price_mean") === 3.25)
  }

  it should "pass summarizer property test" in {
    summarizerPropertyTest(AllProperties)(Summarizers.mean("x1"))
    summarizerPropertyTest(AllProperties)(Summarizers.mean("x2"))
  }
}

/*
 *  Copyright 2018 TWO SIGMA OPEN SOURCE, LLC
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

package com.twosigma.flint.timeseries.summarize.summarizer.subtractable

import com.twosigma.flint.timeseries.Summarizers
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.SummarizerSuite
import org.apache.spark.sql.types.{ DoubleType, IntegerType }

class DotProductSummarizerSpec extends SummarizerSuite {

  override val defaultResourceDir: String = "/timeseries/summarize/summarizer/dotproductsummarizer"

  "DotProductSummarizer" should "compute dot product correctly" in {
    val priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
    val results = priceTSRdd.summarize(Summarizers.dotProduct("price", "price"), Seq("id")).collect()
    assert(results.find(_.getAs[Int]("id") == 3).head.getAs[Double]("price_price_dotProduct") === 72.25)
    assert(results.find(_.getAs[Int]("id") == 7).head.getAs[Double]("price_price_dotProduct") === 90.25)
  }

  it should "pass summarizer property test" in {
    summarizerPropertyTest(AllPropertiesAndSubtractable)(Summarizers.dotProduct("x1", "x2"))
  }
}

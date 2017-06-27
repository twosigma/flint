/*
 *  Copyright 2017 TWO SIGMA OPEN SOURCE, LLC
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

import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.SummarizerSuite
import com.twosigma.flint.timeseries.Summarizers
import org.apache.spark.sql.types.{ DoubleType, IntegerType }

class StandardizedMomentSummarizerSpec extends SummarizerSuite {

  override val defaultResourceDir: String = "/timeseries/summarize/summarizer/standardizedmomentsummarizer"

  "SkewnessSummarizer" should "compute skewness correctly" in {
    val priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
    val results = priceTSRdd.summarize(Summarizers.skewness("price"))
    assert(results.collect().head.getAs[Double]("price_skewness") === 0.0)
  }

  it should "ignore null values" in {
    val priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
    assertEquals(
      priceTSRdd.summarize(Summarizers.skewness("price")),
      insertNullRows(priceTSRdd, "price").summarize(Summarizers.skewness("price"))
    )
  }

  it should "pass summarizer property test" in {
    summarizerPropertyTest(AllPropertiesAndSubtractable)(Summarizers.skewness("x1"))
  }

  "KurtosisSummarizer" should "compute kurtosis correctly" in {
    val priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
    val results = priceTSRdd.summarize(Summarizers.kurtosis("price"))
    assert(results.collect().head.getAs[Double]("price_kurtosis") === -1.2167832167832167)
  }

  it should "ignore null values" in {
    val priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
    assertEquals(
      priceTSRdd.summarize(Summarizers.kurtosis("price")),
      insertNullRows(priceTSRdd, "price").summarize(Summarizers.kurtosis("price"))
    )
  }

  it should "pass summarizer property test" in {
    summarizerPropertyTest(AllPropertiesAndSubtractable)(Summarizers.kurtosis("x1"))
  }
}

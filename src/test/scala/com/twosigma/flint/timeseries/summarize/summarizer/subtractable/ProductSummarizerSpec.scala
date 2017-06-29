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

import com.twosigma.flint.timeseries.{ Summarizers, Windows }
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.SummarizerSuite
import org.apache.spark.sql.types.{ DoubleType, IntegerType }

class ProductSummarizerSpec extends SummarizerSuite {

  override val defaultResourceDir: String = "/timeseries/summarize/summarizer/productsummarizer"

  "ProductSummarizer" should "compute product correctly" in {
    val priceTSRdd = fromCSV("Price.csv", Schema(
      "id" -> IntegerType,
      "price" -> DoubleType,
      "priceWithZero" -> DoubleType,
      "priceWithNegatives" -> DoubleType
    ))
    val results = priceTSRdd.summarize(Summarizers.product("price"), Seq("id")).collect()
    assert(results.find(_.getAs[Int]("id") == 3).head.getAs[Double]("price_product") === 324.84375)
    assert(results.find(_.getAs[Int]("id") == 7).head.getAs[Double]("price_product") === 360.0)
  }

  it should "compute product with a zero correctly" in {
    val priceTSRdd = fromCSV("Price.csv", Schema(
      "id" -> IntegerType,
      "price" -> DoubleType,
      "priceWithZero" -> DoubleType,
      "priceWithNegatives" -> DoubleType
    ))
    var results = priceTSRdd.summarize(Summarizers.product("priceWithZero")).collect()
    assert(results.head.getAs[Double]("priceWithZero_product") === 0.0)

    // Test that having a zero exit the window still computes correctly.
    results = priceTSRdd.coalesce(1).summarizeWindows(
      Windows.pastAbsoluteTime("50 ns"),
      Summarizers.product("priceWithZero")
    ).collect()
    assert(results.head.getAs[Double]("priceWithZero_product") === 0.0)
    assert(results.last.getAs[Double]("priceWithZero_product") === 742.5)
  }

  it should "compute product with negative values correctly" in {
    val priceTSRdd = fromCSV("Price.csv", Schema(
      "id" -> IntegerType,
      "price" -> DoubleType,
      "priceWithZero" -> DoubleType,
      "priceWithNegatives" -> DoubleType
    ))
    val results = priceTSRdd.summarize(Summarizers.product("priceWithNegatives"), Seq("id")).collect()
    assert(results.find(_.getAs[Int]("id") == 3).head.getAs[Double]("priceWithNegatives_product") === -324.84375)
    assert(results.find(_.getAs[Int]("id") == 7).head.getAs[Double]("priceWithNegatives_product") === 360.0)
  }

  it should "pass summarizer property test" in {
    summarizerPropertyTest(AllPropertiesAndSubtractable)(Summarizers.product("x1"))
  }
}

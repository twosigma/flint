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

class GeometricMeanSummarizerSpec extends SummarizerSuite {

  override val defaultResourceDir: String = "/timeseries/summarize/summarizer/geometricmeansummarizer"

  "GeometricMeanSummarizer" should "compute geometric mean correctly" in {
    val priceTSRdd = fromCSV("Price.csv", Schema(
      "id" -> IntegerType,
      "price" -> DoubleType,
      "priceWithZero" -> DoubleType,
      "priceWithNegatives" -> DoubleType
    ))
    val results = priceTSRdd.summarize(Summarizers.geometricMean("price"), Seq("id")).collect()
    assert(results.find(_.getAs[Int]("id") == 3).head.getAs[Double]("price_geometricMean") === 2.621877636494)
    assert(results.find(_.getAs[Int]("id") == 7).head.getAs[Double]("price_geometricMean") === 2.667168275340)
  }

  it should "compute geometric mean with a zero correctly" in {
    val priceTSRdd = fromCSV("Price.csv", Schema(
      "id" -> IntegerType,
      "price" -> DoubleType,
      "priceWithZero" -> DoubleType,
      "priceWithNegatives" -> DoubleType
    ))
    var results = priceTSRdd.summarize(Summarizers.geometricMean("priceWithZero")).collect()
    assert(results.head.getAs[Double]("priceWithZero_geometricMean") === 0.0)

    // Test that having a zero exit the window still computes correctly.
    results = priceTSRdd.coalesce(1).summarizeWindows(
      Windows.pastAbsoluteTime("50 ns"),
      Summarizers.geometricMean("priceWithZero")
    ).collect()
    assert(results.head.getAs[Double]("priceWithZero_geometricMean") === 0.0)
    assert(results.last.getAs[Double]("priceWithZero_geometricMean") === 5.220043408524)
  }

  it should "compute geometric mean with negative values correctly" in {
    val priceTSRdd = fromCSV("Price.csv", Schema(
      "id" -> IntegerType,
      "price" -> DoubleType,
      "priceWithZero" -> DoubleType,
      "priceWithNegatives" -> DoubleType
    ))
    val results = priceTSRdd.summarize(Summarizers.geometricMean("priceWithNegatives"), Seq("id")).collect()
    assert(results.find(_.getAs[Int]("id") == 3).head.getAs[Double]("priceWithNegatives_geometricMean")
      === -2.621877636494)
    assert(results.find(_.getAs[Int]("id") == 7).head.getAs[Double]("priceWithNegatives_geometricMean")
      === 2.667168275340)
  }

  it should "pass summarizer property test" in {
    summarizerPropertyTest(AllPropertiesAndSubtractable)(Summarizers.geometricMean("x1"))
  }
}

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
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ DoubleType, IntegerType }

class CovarianceSummarizerSpec extends SummarizerSuite {

  // It is by intention to reuse the files from correlation summarizer
  override val defaultResourceDir: String = "/timeseries/summarize/summarizer/correlationsummarizer"

  "CovarianceSummarizer" should "`computeCovariance` correctly" in {
    val priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
    val forecastTSRdd = fromCSV("Forecast.csv", Schema("id" -> IntegerType, "forecast" -> DoubleType))
    val input = priceTSRdd.leftJoin(forecastTSRdd, key = Seq("id")).addColumns(
      "price2" -> DoubleType -> { r: Row => r.getAs[Double]("price") },
      "price3" -> DoubleType -> { r: Row => -r.getAs[Double]("price") },
      "price4" -> DoubleType -> { r: Row => r.getAs[Double]("price") * 2 },
      "price5" -> DoubleType -> { r: Row => 0d }
    )

    var results = input.summarize(Summarizers.covariance("price", "price2"), Seq("id")).collect()
    assert(results.find(_.getAs[Int]("id") == 7).head.getAs[Double]("price_price2_covariance") === 3.368055556)
    assert(results.find(_.getAs[Int]("id") == 3).head.getAs[Double]("price_price2_covariance") === 2.534722222)

    results = input.summarize(Summarizers.covariance("price", "price3"), Seq("id")).collect()
    assert(results.find(_.getAs[Int]("id") == 7).head.getAs[Double]("price_price3_covariance") === -3.368055556)
    assert(results.find(_.getAs[Int]("id") == 3).head.getAs[Double]("price_price3_covariance") === -2.534722222)

    results = input.summarize(Summarizers.covariance("price", "price4"), Seq("id")).collect()
    assert(results.find(_.getAs[Int]("id") == 7).head.getAs[Double]("price_price4_covariance") === 6.736111111)
    assert(results.find(_.getAs[Int]("id") == 3).head.getAs[Double]("price_price4_covariance") === 5.069444444)

    results = input.summarize(Summarizers.covariance("price", "price5"), Seq("id")).collect()
    assert(results.find(_.getAs[Int]("id") == 7).head.getAs[Double]("price_price5_covariance") === 0d)
    assert(results.find(_.getAs[Int]("id") == 3).head.getAs[Double]("price_price5_covariance") === 0d)

    results = input.summarize(Summarizers.covariance("price", "forecast"), Seq("id")).collect()
    assert(results.find(_.getAs[Int]("id") == 7).head.getAs[Double]("price_forecast_covariance") === -0.190277778)
    assert(results.find(_.getAs[Int]("id") == 3).head.getAs[Double]("price_forecast_covariance") === -3.783333333)
  }

  it should "pass summarizer property test" in {
    summarizerPropertyTest(AllProperties)(Summarizers.covariance("x1", "x2"))
    summarizerPropertyTest(AllProperties)(Summarizers.covariance("x0", "x3"))
  }
}

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

package com.twosigma.flint.timeseries.summarize.summarizer

import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.SummarizerSuite
import com.twosigma.flint.timeseries.{ Summarizers, TimeSeriesRDD }
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class CorrelationSummarizerSpec extends SummarizerSuite {

  override val defaultResourceDir: String = "/timeseries/summarize/summarizer/correlationsummarizer"

  private var priceTSRdd: TimeSeriesRDD = null
  private var forecastTSRdd: TimeSeriesRDD = null
  private var input: TimeSeriesRDD = null

  private lazy val init: Unit = {
    priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
    forecastTSRdd = fromCSV("Forecast.csv", Schema("id" -> IntegerType, "forecast" -> DoubleType))
    input = priceTSRdd.leftJoin(forecastTSRdd, key = Seq("id")).addColumns(
      "price2" -> DoubleType -> { r: Row => r.getAs[Double]("price") },
      "price3" -> DoubleType -> { r: Row => -r.getAs[Double]("price") },
      "price4" -> DoubleType -> { r: Row => r.getAs[Double]("price") * 2 },
      "price5" -> DoubleType -> { r: Row => 0d }
    )
  }

  "CorrelationSummarizer" should "compute correlation correctly" in {
    init

    var results = input.summarize(Summarizers.correlation("price", "price2"), Seq("id")).collect()
    assert(results(0).getAs[Double](s"price_price2_correlation") === 1.0)
    assert(results(1).getAs[Double](s"price_price2_correlation") === 1.0)

    results = input.summarize(Summarizers.correlation("price", "price3"), Seq("id")).collect()
    assert(results(0).getAs[Double]("price_price3_correlation") === -1.0)
    assert(results(1).getAs[Double]("price_price3_correlation") === -1.0)

    results = input.summarize(Summarizers.correlation("price", "price4"), Seq("id")).collect()
    assert(results(0).getAs[Double]("price_price4_correlation") === 1.0)
    assert(results(1).getAs[Double]("price_price4_correlation") === 1.0)

    results = input.summarize(Summarizers.correlation("price", "price5"), Seq("id")).collect()
    assert(results(0).getAs[Double]("price_price5_correlation").isNaN)
    assert(results(1).getAs[Double]("price_price5_correlation").isNaN)

    results = input.summarize(Summarizers.correlation("price", "forecast"), Seq("id")).collect()
    assert(results.find(_.getAs[Int]("id") == 7).head.getAs[Double]("price_forecast_correlation") ===
      -0.021896121374023046)
    assert(results.find(_.getAs[Int]("id") == 7).head.getAs[Double]("price_forecast_correlationTStat") ===
      -0.04380274440368827)
    assert(results.find(_.getAs[Int]("id") == 3).head.getAs[Double]("price_forecast_correlation") ===
      -0.47908485866330514)
    assert(results.find(_.getAs[Int]("id") == 3).head.getAs[Double]("price_forecast_correlationTStat") ===
      -1.0915971793294055)

    results = input.summarize(Summarizers.correlation("price", "price3", "forecast"), Seq("id")).collect()
    assert(results.find(_.getAs[Int]("id") == 7).head.getAs[Double]("price_price3_correlation") ===
      -1.0)
    assert(results.find(_.getAs[Int]("id") == 7).head.getAs[Double]("price_price3_correlationTStat") ==
      Double.NegativeInfinity)
    assert(results.find(_.getAs[Int]("id") == 7).head.getAs[Double]("price_forecast_correlation") ===
      -0.021896121374023046)
    assert(results.find(_.getAs[Int]("id") == 7).head.getAs[Double]("price3_forecast_correlation") ===
      0.021896121374023046)
    assert(results.find(_.getAs[Int]("id") == 7).head.getAs[Double]("price_forecast_correlationTStat") ===
      -0.04380274440368827)
    assert(results.find(_.getAs[Int]("id") == 7).head.getAs[Double]("price3_forecast_correlationTStat") ===
      0.04380274440368827)

    results = input.summarize(Summarizers.correlation(Seq("price", "price3"), Seq("forecast")), Seq("id")).collect()
    assert(results.find(_.getAs[Int]("id") == 7).head.getAs[Double]("price_forecast_correlation") ===
      -0.021896121374023046)
    assert(results.find(_.getAs[Int]("id") == 7).head.getAs[Double]("price3_forecast_correlation") ===
      0.021896121374023046)
    assert(results.find(_.getAs[Int]("id") == 7).head.getAs[Double]("price_forecast_correlationTStat") ===
      -0.04380274440368827)
    assert(results.find(_.getAs[Int]("id") == 7).head.getAs[Double]("price3_forecast_correlationTStat") ===
      0.04380274440368827)
    intercept[RuntimeException] {
      input.summarize(Summarizers.correlation(Seq("price", "price3"), Seq("forecast", "price3")))
    }
  }

  it should "ignore null values" in {
    init
    val inputWithNull = insertNullRows(input, "price", "forecast")

    assertEquals(
      inputWithNull.summarize(Summarizers.correlation("price", "forecast")),
      input.summarize(Summarizers.correlation("price", "forecast"))
    )

    assertEquals(
      inputWithNull.summarize(Summarizers.correlation("price", "forecast"), Seq("id")),
      input.summarize(Summarizers.correlation("price", "forecast"), Seq("id"))
    )
  }

  it should "pass summarizer property test" in {
    summarizerPropertyTest(AllProperties)(Summarizers.correlation("x1", "x2"))
    summarizerPropertyTest(AllProperties)(Summarizers.correlation("x0", "x3"))
  }
}

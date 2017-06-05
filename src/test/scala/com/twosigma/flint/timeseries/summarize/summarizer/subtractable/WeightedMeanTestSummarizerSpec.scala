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
import com.twosigma.flint.timeseries.{ Summarizers, TimeSeriesRDD, Windows }
import org.apache.spark.sql.types._

class WeightedMeanTestSummarizerSpec extends SummarizerSuite {
  override val defaultResourceDir: String = "/timeseries/summarize/summarizer/weightedmeantestsummarizer"

  private var priceTSRdd: TimeSeriesRDD = _
  private var forecastTSRdd: TimeSeriesRDD = _
  private var joinedRdd: TimeSeriesRDD = _

  private lazy val init = {
    priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
    forecastTSRdd = fromCSV("Forecast.csv", Schema("id" -> IntegerType, "forecast" -> DoubleType))
    joinedRdd = priceTSRdd.leftJoin(forecastTSRdd, key = Seq("id"))
  }

  "WeightedMeanTestSummarizer" should "compute `WeightedMean` correctly" in {
    init
    val result = joinedRdd.summarize(Summarizers.weightedMeanTest("price", "forecast")).first

    assert(result.getAs[Double]("price_forecast_weightedMean") === 0.11695906432748544)
    assert(result.getAs[Double]("price_forecast_weightedStandardDeviation") === 4.373623725800579)
    assert(result.getAs[Double]("price_forecast_weightedTStat") === 0.0788230123405099)
    assert(result.getAs[Long]("price_forecast_observationCount") == 12L)
  }

  it should "compute `WeightedMean` correctly for a window" in {
    init
    val lastWindowRdd = joinedRdd.deleteRows(r => r.getAs[Long]("time") < 1150L)
    val expectedResult = lastWindowRdd.summarize(Summarizers.weightedMeanTest("price", "forecast")).first
    val results = joinedRdd.summarizeWindows(
      Windows.pastAbsoluteTime("100 ns"),
      Summarizers.weightedMeanTest("price", "forecast")
    ).collect()
    val lastResult = results(results.length - 1)

    assert(lastResult.getAs[Double]("price_forecast_weightedMean") ===
      expectedResult.getAs[Double]("price_forecast_weightedMean"))
    assert(lastResult.getAs[Double]("price_forecast_weightedStandardDeviation") ===
      expectedResult.getAs[Double]("price_forecast_weightedStandardDeviation"))
    assert(lastResult.getAs[Double]("price_forecast_weightedTStat") ===
      expectedResult.getAs[Double]("price_forecast_weightedTStat"))
    assert(lastResult.getAs[Long]("price_forecast_observationCount") ==
      expectedResult.getAs[Long]("price_forecast_observationCount"))
  }

  it should "ignore null values" in {
    init
    assertEquals(
      joinedRdd.summarize(Summarizers.weightedMeanTest("price", "forecast")),
      insertNullRows(joinedRdd, "price").summarize(Summarizers.weightedMeanTest("price", "forecast"))
    )

    assertEquals(
      joinedRdd.summarize(Summarizers.weightedMeanTest("price", "forecast")),
      insertNullRows(joinedRdd, "price", "forecast").summarize(Summarizers.weightedMeanTest("price", "forecast"))
    )
  }

  it should "pass summarizer property test" in {
    summarizerPropertyTest(AllPropertiesAndSubtractable)(Summarizers.weightedMeanTest("x1", "x2"))
  }
}

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
import com.twosigma.flint.{ SpecUtils, SharedSparkContext }
import com.twosigma.flint.timeseries.{ Summarizers, CSV, TimeSeriesRDD }
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ IntegerType, DoubleType, StructType }
import org.scalactic.TolerantNumerics
import org.scalatest.FlatSpec

class CorrelationSummarizerSpec extends FlatSpec with SharedSparkContext {

  private implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(1.0e-8)

  private val defaultPartitionParallelism: Int = 5

  private val resourceDir: String = "/timeseries/summarize/summarizer/correlationsummarizer"

  private def from(filename: String, schema: StructType): TimeSeriesRDD =
    SpecUtils.withResource(s"$resourceDir/$filename") { source =>
      CSV.from(
        sqlContext,
        s"file://$source",
        header = true,
        sorted = true,
        schema = schema
      ).repartition(defaultPartitionParallelism)
    }

  "CorrelationSummarizer" should "`computeCorrelation` correctly" in {
    val priceTSRdd = from("Price.csv", Schema("tid" -> IntegerType, "price" -> DoubleType))
    val forecastTSRdd = from("Forecast.csv", Schema("tid" -> IntegerType, "forecast" -> DoubleType))

    val input = priceTSRdd.leftJoin(forecastTSRdd, key = Seq("tid")).addColumns(
      "price2" -> DoubleType -> { r: Row => r.getAs[Double]("price") },
      "price3" -> DoubleType -> { r: Row => -r.getAs[Double]("price") },
      "price4" -> DoubleType -> { r: Row => r.getAs[Double]("price") * 2 },
      "price5" -> DoubleType -> { r: Row => 0d }
    )

    var results = input.summarize(Summarizers.correlation("price", "price2"), Seq("tid")).collect()
    assert(results(0).getAs[Double]("price_price2_correlation") === 1.0)
    assert(results(1).getAs[Double]("price_price2_correlation") === 1.0)

    results = input.summarize(Summarizers.correlation("price", "price3"), Seq("tid")).collect()
    assert(results(0).getAs[Double]("price_price3_correlation") === -1.0)
    assert(results(1).getAs[Double]("price_price3_correlation") === -1.0)

    results = input.summarize(Summarizers.correlation("price", "price4"), Seq("tid")).collect()
    assert(results(0).getAs[Double]("price_price4_correlation") === 1.0)
    assert(results(1).getAs[Double]("price_price4_correlation") === 1.0)

    results = input.summarize(Summarizers.correlation("price", "price5"), Seq("tid")).collect()
    assert(results(0).getAs[Double]("price_price5_correlation").isNaN)
    assert(results(1).getAs[Double]("price_price5_correlation").isNaN)

    results = input.summarize(Summarizers.correlation("price", "forecast"), Seq("tid")).collect()
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price_forecast_correlation") === -0.021896121374023046)
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price_forecast_correlationTStat") === -0.04380274440368827)
    assert(results.find(_.getAs[Int]("tid") == 3).head.getAs[Double]("price_forecast_correlation") === -0.47908485866330514)
    assert(results.find(_.getAs[Int]("tid") == 3).head.getAs[Double]("price_forecast_correlationTStat") === -1.0915971793294055)

    results = input.summarize(Summarizers.correlation("price", "price3", "forecast"), Seq("tid")).collect()
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price_price3_correlation") === -1.0)
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price_price3_correlationTStat") == Double.NegativeInfinity)
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price_forecast_correlation") === -0.021896121374023046)
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price3_forecast_correlation") === 0.021896121374023046)
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price_forecast_correlationTStat") === -0.04380274440368827)
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price3_forecast_correlationTStat") === 0.04380274440368827)

    results = input.summarize(Summarizers.correlation(Seq("price", "price3"), Seq("forecast")), Seq("tid")).collect()
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price_forecast_correlation") === -0.021896121374023046)
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price3_forecast_correlation") === 0.021896121374023046)
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price_forecast_correlationTStat") === -0.04380274440368827)
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price3_forecast_correlationTStat") === 0.04380274440368827)
    intercept[RuntimeException] {
      input.summarize(Summarizers.correlation(Seq("price", "price3"), Seq("forecast", "price3")))
    }
  }
}

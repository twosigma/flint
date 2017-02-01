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
import org.apache.spark.sql.types.{ DoubleType, IntegerType, StructType }
import org.scalactic.TolerantNumerics
import org.scalatest.FlatSpec

class CovarianceSummarizerSpec extends FlatSpec with SharedSparkContext {

  private implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(1.0e-8)

  private val defaultPartitionParallelism: Int = 5

  // It is by intention to reuse the files from correlation summarizer
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

  "CovarianceSummarizer" should "`computeCovariance` correctly" in {
    val priceTSRdd = from("Price.csv", Schema("tid" -> IntegerType, "price" -> DoubleType))
    val forecastTSRdd = from("Forecast.csv", Schema("tid" -> IntegerType, "forecast" -> DoubleType))
    val input = priceTSRdd.leftJoin(forecastTSRdd, key = Seq("tid")).addColumns(
      "price2" -> DoubleType -> { r: Row => r.getAs[Double]("price") },
      "price3" -> DoubleType -> { r: Row => -r.getAs[Double]("price") },
      "price4" -> DoubleType -> { r: Row => r.getAs[Double]("price") * 2 },
      "price5" -> DoubleType -> { r: Row => 0d }
    )

    var results = input.summarize(Summarizers.covariance("price", "price2"), Seq("tid")).collect()
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price_price2_covariance") === 3.368055556)
    assert(results.find(_.getAs[Int]("tid") == 3).head.getAs[Double]("price_price2_covariance") === 2.534722222)

    results = input.summarize(Summarizers.covariance("price", "price3"), Seq("tid")).collect()
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price_price3_covariance") === -3.368055556)
    assert(results.find(_.getAs[Int]("tid") == 3).head.getAs[Double]("price_price3_covariance") === -2.534722222)

    results = input.summarize(Summarizers.covariance("price", "price4"), Seq("tid")).collect()
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price_price4_covariance") === 6.736111111)
    assert(results.find(_.getAs[Int]("tid") == 3).head.getAs[Double]("price_price4_covariance") === 5.069444444)

    results = input.summarize(Summarizers.covariance("price", "price5"), Seq("tid")).collect()
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price_price5_covariance") === 0d)
    assert(results.find(_.getAs[Int]("tid") == 3).head.getAs[Double]("price_price5_covariance") === 0d)

    results = input.summarize(Summarizers.covariance("price", "forecast"), Seq("tid")).collect()
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price_forecast_covariance") === -0.190277778)
    assert(results.find(_.getAs[Int]("tid") == 3).head.getAs[Double]("price_forecast_covariance") === -3.783333333)
  }
}

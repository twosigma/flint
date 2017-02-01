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
import org.apache.spark.sql.types.{ DoubleType, IntegerType, StructType }
import org.scalactic.TolerantNumerics
import org.scalatest.FlatSpec

class WeightedMeanTestSummarizerSpec extends FlatSpec with SharedSparkContext {

  private implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(1.0e-8)

  private val defaultPartitionParallelism: Int = 5

  private val resourceDir: String = "/timeseries/summarize/summarizer/weightedmeantestsummarizer"

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

  "WeightedMeanTestSummarizer" should "compute `WeightedMean` correctly" in {
    val priceTSRdd = from("Price.csv", Schema("tid" -> IntegerType, "price" -> DoubleType))
    val forecastTSRdd = from("Forecast.csv", Schema("tid" -> IntegerType, "forecast" -> DoubleType))
    val result = priceTSRdd.leftJoin(
      forecastTSRdd, key = Seq("tid")
    ).summarize(Summarizers.weightedMeanTest("price", "forecast")).first

    assert(result.getAs[Double]("price_forecast_weightedMean") === 0.11695906432748544)
    assert(result.getAs[Double]("price_forecast_weightedStandardDeviation") === 4.373623725800579)
    assert(result.getAs[Double]("price_forecast_weightedTStat") === 0.0788230123405099)
    assert(result.getAs[Long]("price_forecast_observationCount") == 12L)
  }
}

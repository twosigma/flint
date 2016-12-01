/*
 *  Copyright 2015-2016 TWO SIGMA OPEN SOURCE, LLC
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

import com.twosigma.flint.timeseries.Summarizers
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.{ SpecUtils, SharedSparkContext }
import com.twosigma.flint.timeseries.{ CSV, TimeSeriesRDD }
import org.apache.spark.sql.types.{ DoubleType, IntegerType, StructType }
import org.scalactic.TolerantNumerics
import org.scalatest.FlatSpec

class CompositeSummarizerSpec extends FlatSpec with SharedSparkContext {
  private implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(1.0e-8)

  private val defaultPartitionParallelism: Int = 5

  // Reuse mean summarizer data
  private val resourceDir: String = "/timeseries/summarize/summarizer/meansummarizer"

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

  "CompositeSummarizer" should "compute `mean` and `stddev` correctly" in {
    val priceTSRdd = from("Price.csv", Schema("tid" -> IntegerType, "price" -> DoubleType))
    val result = priceTSRdd.summarize(
      Summarizers.compose(Summarizers.mean("price"), Summarizers.stddev("price"))
    )
    val row = result.first()

    assert(row.getAs[Double]("price_mean") == 3.25)
    assert(row.getAs[Double]("price_stddev") == 1.8027756377319946)
  }

  it should "throw exception for conflicting output columns" in {
    intercept[Exception] {
      val priceTSRdd = from("Price.csv", Schema("tid" -> IntegerType, "price" -> DoubleType))
      priceTSRdd.summarize(Summarizers.compose(Summarizers.mean("price"), Summarizers.mean("price")))
    }
  }

  it should "handle conflicting output columns using prefix" in {
    val priceTSRdd = from("Price.csv", Schema("tid" -> IntegerType, "price" -> DoubleType))
    val result = priceTSRdd.summarize(
      Summarizers.compose(Summarizers.mean("price"), Summarizers.mean("price").prefix("prefix"))
    )

    val row = result.first()

    assert(row.getAs[Double]("price_mean") == 3.25)
    assert(row.getAs[Double]("prefix_price_mean") == 3.25)
  }
}

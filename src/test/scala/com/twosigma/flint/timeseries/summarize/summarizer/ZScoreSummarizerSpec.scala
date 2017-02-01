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
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ IntegerType, DoubleType, StructType }
import org.scalactic.TolerantNumerics
import org.scalatest.FlatSpec

class ZScoreSummarizerSpec extends FlatSpec with SharedSparkContext {

  private implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(1.0e-8)

  private val defaultPartitionParallelism: Int = 5

  private val resourceDir: String = "/timeseries/summarize/summarizer/zscoresummarizer"

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

  "ZScoreSummarizer" should "compute in-sample `zScore` correctly" in {
    val priceTSRdd = from("Price.csv", Schema("tid" -> IntegerType, "price" -> DoubleType))
    val expectedSchema = Schema("price_zScore" -> DoubleType)
    val expectedResults = Array[Row](new GenericRowWithSchema(Array(0L, 1.5254255396193801), expectedSchema))
    val results = priceTSRdd.summarize(Summarizers.zScore("price", true))
    assert(results.schema == expectedSchema)
    assert(results.collect().deep == expectedResults.deep)
  }

  it should "compute out-of-sample `zScore` correctly" in {
    val priceTSRdd = from("Price.csv", Schema("tid" -> IntegerType, "price" -> DoubleType))
    val expectedSchema = Schema("price_zScore" -> DoubleType)
    val expectedResults = Array[Row](new GenericRowWithSchema(Array(0L, 1.8090680674665818), expectedSchema))
    val results = priceTSRdd.summarize(Summarizers.zScore("price", false))
    assert(results.schema == expectedSchema)
    assert(results.collect().deep == expectedResults.deep)
  }
}

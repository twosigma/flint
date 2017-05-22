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
import com.twosigma.flint.timeseries.Summarizers
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ DoubleType, IntegerType }

class ZScoreSummarizerSpec extends SummarizerSuite {

  override val defaultResourceDir: String = "/timeseries/summarize/summarizer/zscoresummarizer"

  "ZScoreSummarizer" should "compute in-sample `zScore` correctly" in {
    val priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
    val expectedSchema = Schema("price_zScore" -> DoubleType)
    val expectedResults = Array[Row](new GenericRowWithSchema(Array(0L, 1.5254255396193801), expectedSchema))
    val results = priceTSRdd.summarize(Summarizers.zScore("price", true))
    assert(results.schema == expectedSchema)
    assert(results.collect().deep == expectedResults.deep)
  }

  it should "compute out-of-sample `zScore` correctly" in {
    val priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
    val expectedSchema = Schema("price_zScore" -> DoubleType)
    val expectedResults = Array[Row](new GenericRowWithSchema(Array(0L, 1.8090680674665818), expectedSchema))
    val results = priceTSRdd.summarize(Summarizers.zScore("price", false))
    assert(results.schema == expectedSchema)
    assert(results.collect().deep == expectedResults.deep)
  }

  it should "ignore null values" in {
    val priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
    assertEquals(
      priceTSRdd.summarize(Summarizers.zScore("price", true)),
      insertNullRows(priceTSRdd, "price").summarize(Summarizers.zScore("price", true))
    )
  }

  it should "pass summarizer property test" in {
    summarizerPropertyTest(AllProperties)(Summarizers.zScore("x1", true))
    summarizerPropertyTest(AllProperties)(Summarizers.zScore("x2", false))
  }
}

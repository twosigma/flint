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

class StackSummarizerSpec extends SummarizerSuite {
  // Reuse mean summarizer data
  override val defaultResourceDir: String = "/timeseries/summarize/summarizer/meansummarizer"

  var priceTSRdd: TimeSeriesRDD = _

  lazy val init: Unit = {
    priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
  }

  "StackSummarizer" should "stack three summarizers correctly" in {
    init
    val predicate1: Int => Boolean = id => id == 3
    val predicate2: Int => Boolean = id => id == 7
    val result = priceTSRdd.summarize(
      Summarizers.stack(
        Summarizers.sum("price").where(predicate1)("id"),
        Summarizers.sum("price").where(predicate2)("id"),
        Summarizers.sum("price")
      )
    )
    val rows = result.first().getAs[Seq[Row]](StackSummarizer.stackColumn)

    assert(result.schema === Schema("stack" -> ArrayType(StructType(StructField("price_sum", DoubleType) :: Nil))))
    assert(rows(0).getAs[Double]("price_sum") === 18.5)
    assert(rows(1).getAs[Double]("price_sum") === 20.5)
    assert(rows(2).getAs[Double]("price_sum") === 39.0)
  }

  it should "throw exception for non-identical output columns" in {
    init
    intercept[Exception] {
      priceTSRdd.summarize(Summarizers.stack(Summarizers.mean("price"), Summarizers.sum("price")))
    }
  }
}

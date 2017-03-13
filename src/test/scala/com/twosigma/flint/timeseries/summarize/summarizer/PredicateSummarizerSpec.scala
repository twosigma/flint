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

import com.twosigma.flint.timeseries.Summarizers
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.SummarizerSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ DoubleType, IntegerType }

class PredicateSummarizerSpec extends SummarizerSuite {
  override val defaultResourceDir: String = "/timeseries/summarize/summarizer/meansummarizer"

  "PredicateSummarizer" should "return the same results as filtering TSRDD first" in {
    val priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
    val summarizer = Summarizers.compose(Summarizers.mean("price"), Summarizers.stddev("price"))

    val predicate: Int => Boolean = id => id == 3
    val resultWithPredicate = priceTSRdd.summarize(summarizer.where(predicate)("id")).first()

    val filteredTSRDD = priceTSRdd.keepRows {
      row: Row => row.getAs[Int]("id") == 3
    }
    val filteredResults = filteredTSRDD.summarize(summarizer).first()

    assert(resultWithPredicate.getAs[Double]("price_mean") === filteredResults.getAs[Double]("price_mean"))
    assert(resultWithPredicate.getAs[Double]("price_stddev") === filteredResults.getAs[Double]("price_stddev"))
  }

  it should "pass summarizer property test" in {
    val predicate: Double => Boolean = num => num > 0
    summarizerPropertyTest(AllProperties)(Summarizers.sum("x1").where(predicate)("x2"))
  }
}

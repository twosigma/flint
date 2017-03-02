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

import com.twosigma.flint.timeseries._
import com.twosigma.flint.timeseries.row.Schema
import org.apache.spark.sql.types.{ DoubleType, IntegerType }
import org.apache.spark.sql.Row

class ExponentialSmoothingSummarizerSpec extends TimeSeriesSuite {

  override val defaultPartitionParallelism: Int = 10

  override val defaultResourceDir = "/timeseries/summarize/summarizer/exponentialsmoothingsummarizer"

  "ExponentialSmoothingSummarizer" should "smooth correctly" in {
    val timeSeriesRdd = fromCSV(
      "Price.csv",
      Schema("id" -> IntegerType, "price" -> DoubleType, "expected" -> DoubleType)
    )
    val result1 = timeSeriesRdd.addSummaryColumns(Summarizers.exponentialSmoothing(
      xColumn = "price",
      timestampsToPeriods = (a, b) => (b - a) / 100.0
    ), Seq("id"))
    result1.rdd.collect().foreach(row => {
      val predVal = row.getAs[Double](ExponentialSmoothingSummarizer.esColumn)
      val trueVal = row.getAs[Double]("expected")
      assert(predVal === trueVal)
    })
  }

  "ExponentialSmoothingSummarizer" should "smooth sin correctly" in {
    def getSinRDDWithID(id: Int, constant: Double = 1.0): TimeSeriesRDD = {
      var rdd = Clocks.uniform(sc, "1d")
      rdd = rdd.addColumns("value" -> DoubleType -> { (row: Row) => constant })
      rdd = rdd.addColumns("id" -> IntegerType -> { (row: Row) => id })
      rdd.addColumns("expected" -> DoubleType -> { (row: Row) => constant })
    }

    var rdd = getSinRDDWithID(1, 1.0).merge(getSinRDDWithID(2, 2.0)).merge(getSinRDDWithID(3, 3.0))
    val nanosToDays = (t1: Long, t2: Long) => (t2 - t1) / (24 * 60 * 60 * 1e9)
    rdd = rdd.addSummaryColumns(Summarizers.exponentialSmoothing(
      xColumn = "value",
      timestampsToPeriods = nanosToDays
    ), Seq("id"))
    rdd.rdd.collect().foreach(row => {
      val predVal = row.getAs[Double](ExponentialSmoothingSummarizer.esColumn)
      val trueVal = row.getAs[Double]("expected")
      assert(predVal === trueVal)
    })
  }
}

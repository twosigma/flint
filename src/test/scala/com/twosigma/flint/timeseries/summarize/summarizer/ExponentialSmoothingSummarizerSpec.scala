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

import com.twosigma.flint.timeseries._
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.SummarizerSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ DoubleType, IntegerType }
import org.scalactic.{ Equality, TolerantNumerics }

class ExponentialSmoothingSummarizerSpec extends SummarizerSuite {

  override val defaultPartitionParallelism: Int = 10

  override val defaultResourceDir = "/timeseries/summarize/summarizer/exponentialsmoothingsummarizer"

  private var price1: TimeSeriesRDD = _
  private var price2: TimeSeriesRDD = _

  private lazy val init = {
    price1 = fromCSV(
      "Price.csv",
      Schema(
        "id" -> IntegerType,
        "price" -> DoubleType,
        "expected" -> DoubleType,
        "expected_core_previous" -> DoubleType,
        "expected_core_current" -> DoubleType,
        "expected_core_linear" -> DoubleType,
        "expected_convolution_previous" -> DoubleType,
        "expected_convolution_current" -> DoubleType,
        "expected_convolution_linear" -> DoubleType,
        "expected_legacy_previous" -> DoubleType,
        "expected_legacy_current" -> DoubleType,
        "expected_legacy_linear" -> DoubleType
      )
    )

    price2 = fromCSV(
      "window.csv",
      dateFormat = "yyyyMMdd HH:mm:ss.SSS"
    )
  }

  private def test(
    primingPeriods: Double,
    exponentialSmoothingType: String,
    exponentialSmoothingConvention: String
  ): Unit = {
    init
    val results = price1.addSummaryColumns(Summarizers.exponentialSmoothing(
      xColumn = "price",
      timestampsToPeriods = (a, b) => (b - a) / 100.0,
      alpha = 0.5,
      primingPeriods = primingPeriods,
      exponentialSmoothingType = exponentialSmoothingType,
      exponentialSmoothingConvention = exponentialSmoothingConvention
    ))

    results.rdd.collect().foreach{ row =>
      val predVal = row.getAs[Double]("price_ema")
      val trueVal = row.getAs[Double](s"expected_${exponentialSmoothingConvention}_$exponentialSmoothingType")
      if (predVal.isNaN) {
        assert(trueVal.isNaN)
      } else {
        assert(predVal === trueVal)
      }
    }
  }

  "ExponentialSmoothingSummarizer" should "smooth correctly" in {
    init
    val results = price1.addSummaryColumns(Summarizers.exponentialSmoothing(
      xColumn = "price",
      timestampsToPeriods = (a, b) => (b - a) / 100.0
    ), Seq("id"))
    results.rdd.collect().foreach(row => {
      val predVal = row.getAs[Double]("price_ema")
      val trueVal = row.getAs[Double]("expected")
      assert(predVal === trueVal)
    })
  }

  it should "decay using half life correctly" in {
    init
    val results = price1.addSummaryColumns(Summarizers.emaHalfLife(
      xColumn = "price",
      halfLifeDuration = "100ns"
    ))
    results.rdd.collect().foreach(row => {
      val predVal = row.getAs[Double]("price_ema")
      val trueVal = row.getAs[Double]("expected_legacy_previous")
      if (predVal.isNaN) {
        assert(trueVal.isNaN)
      } else {
        assert(predVal === trueVal)
      }
    })
  }

  it should "interpolate using previous point core correctly" in {
    test(0, "previous", "core")
  }

  it should "interpolate using current point core correctly" in {
    test(0, "current", "core")
  }

  it should "interpolate linearly using core correctly" in {
    test(0, "linear", "core")
  }

  it should "interpolate using previous point convolution correctly" in {
    test(0, "previous", "convolution")
  }

  it should "interpolate using current point convolution correctly" in {
    test(0, "current", "convolution")
  }

  it should "interpolate linearly using convolution correctly" in {
    test(0, "linear", "convolution")
  }

  it should "interpolate using previous point legacy correctly" in {
    // primingPeriods should be ignored for the legacy convention.
    test(1, "previous", "legacy")
  }

  it should "interpolate using current point legacy correctly" in {
    // primingPeriods should be ignored for the legacy convention.
    test(2, "current", "legacy")
  }

  it should "interpolate linearly using legacy correctly" in {
    // primingPeriods should be ignored for the legacy convention.
    test(3, "linear", "legacy")
  }

  it should "smooth sin correctly" in {
    def getSinRDDWithID(id: Int, constant: Double = 1.0): TimeSeriesRDD = {
      var rdd = Clocks.uniform(sc, "1d", beginDateTime = "1960-01-01", endDateTime = "2030-01-01")
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
      val predVal = row.getAs[Double]("value_ema")
      val trueVal = row.getAs[Double]("expected")
      assert(predVal === trueVal)
    })
  }

  it should "pass summarizer property test" in {
    val primingPeriods = Seq(0.0, 1.0)
    val exponentialSmoothingTypes = Seq("current", "previous", "linear")
    val exponentialSmoothingConventions = Seq("core", "convolution")
    for (pp <- primingPeriods; est <- exponentialSmoothingTypes; esc <- exponentialSmoothingConventions) {
      summarizerPropertyTest(AllProperties)(Summarizers.exponentialSmoothing(
        xColumn = "x1",
        timestampsToPeriods = (a, b) => (b - a) / 100.0,
        primingPeriods = pp,
        exponentialSmoothingType = est,
        exponentialSmoothingConvention = esc
      ))
    }
  }

  it should "summarizeWindows correctly" in {
    init
    val window = Windows.pastAbsoluteTime("1 day")

    for (
      smoothingConversion <- Seq("core", "convolution", "legacy");
      smoothingType <- Seq("previous", "current", "linear")
    ) {
      val summarizer1 = Summarizers.emaHalfLife(
        "v",
        "60 minutes",
        exponentialSmoothingType = smoothingType,
        exponentialSmoothingConvention = smoothingConversion
      )
      val result = price2.summarizeWindows(window, summarizer1)

      result.collect().foreach {
        row: Row =>
          val result = row.getAs[Double]("v_ema")
          val expected = row.getAs[Double](s"expected_${smoothingConversion}_$smoothingType")
          if (result.isNaN || expected.isNaN) {
            assert(result.isNaN)
            assert(expected.isNaN)
          } else {
            assert(result === expected)
          }
      }
    }
  }
}

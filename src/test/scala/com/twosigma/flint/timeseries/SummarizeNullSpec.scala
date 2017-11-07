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

package com.twosigma.flint.timeseries

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ DoubleType, IntegerType }

class SummarizeNullSpec extends TimeSeriesSuite with TimeSeriesTestData {
  it should "ignore null values in summarize" in {
    assertEquals(
      forecastData.summarize(Summarizers.count("forecast")),
      insertNullRows(forecastData, "forecast").summarize(Summarizers.count("forecast"))
    )
  }

  it should "ignore null values in summarize with key" in {
    assertEquals(
      forecastData.summarize(Summarizers.count("forecast"), Seq("id")),
      insertNullRows(forecastData, "forecast").summarize(Summarizers.count("forecast"), Seq("id"))
    )
  }

  it should "handle null values in key" in {
    val nullKeys = forecastData.addColumns("id" -> IntegerType -> { r: Row =>
      val id = r.getAs[Int]("id")
      if (id == 3) {
        null
      } else {
        id
      }
    })
    val result = nullKeys.summarize(Summarizers.count("forecast"), Seq("id"))
    assert(
      result.keepRows{ r: Row => r.getAs[Int]("id") == 7 }.first().getAs[Long]("forecast_count") == 6
    )
    assert(
      result.keepRows{ r: Row => r.getAs[Any]("id") == null }.first().getAs[Long]("forecast_count") == 6
    )
  }

  it should "summarize empty key" in {
    val emptyCycle = forecastData.addColumns("forecast" -> DoubleType -> { r: Row =>
      val id = r.getAs[Int]("id")
      val forecast = r.getAs[Double]("forecast")
      if (id == 3) {
        null
      } else {
        forecast
      }
    })

    val result = emptyCycle.summarize(Summarizers.count("forecast"), Seq("id"))
    assert(
      result.keepRows{ r: Row => r.getAs[Int]("id") == 7 }.first().getAs[Long]("forecast_count") == 6
    )

    assert(
      result.keepRows{ r: Row => r.getAs[Int]("id") == 3 }.first().getAs[Long]("forecast_count") == 0
    )
  }

  it should "summarize empty cycle" in {
    val emptyCycle = forecastData.addColumns("forecast" -> DoubleType -> { r: Row =>
      val time = r.getAs[Long]("time")
      val forecast = r.getAs[Double]("forecast")
      if (time == 1050L) {
        null
      } else {
        forecast
      }
    })

    val result = emptyCycle.summarizeCycles(Summarizers.count("forecast"))
    assert(
      result.keepRows{ r: Row => r.getAs[Long]("time") == 1050L }.first().getAs[Long]("forecast_count") == 0
    )
  }

  it should "summarize empty interval" in {

    val clock = Clocks.uniform(sc, "100ns", beginDateTime = "1970-01-01", endDateTime = "1970-01-01 00:00:00.001")

    val emptyInterval = forecastData.addColumns("forecast" -> DoubleType -> { r: Row =>
      val time = r.getAs[Long]("time")
      val forecast = r.getAs[Double]("forecast")
      if (time >= 1000L && time < 1100L) {
        null
      } else {
        forecast
      }
    })

    val result = emptyInterval.summarizeIntervals(clock, Summarizers.count("forecast"))

    assert(
      result.keepRows{ r: Row => r.getAs[Long]("time") == 1100L }.first().getAs[Long]("forecast_count") == 0
    )
  }

  it should "addSummaryColumns with null correctly" in {
    val withEmptyRows = forecastData.addColumns("forecast" -> DoubleType -> { r: Row =>
      val time = r.getAs[Long]("time")
      val forecast = r.getAs[Double]("forecast")
      if (time >= 1100L && time < 1200L) {
        null
      } else {
        forecast
      }
    })

    val result = withEmptyRows.addSummaryColumns(Summarizers.count("forecast"))
    val stableSqlContext = this.sqlContext
    import stableSqlContext.implicits._
    assert(result.toDF.select("forecast_count").as[Long].collect().toList == List(1, 2, 3, 4, 4, 4, 4, 4, 5, 6, 7, 8))
  }
}

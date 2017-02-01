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

import com.twosigma.flint.{ SharedSparkContext, SpecUtils }
import com.twosigma.flint.timeseries.{ CSV, Clocks, Summarizers, TimeSeriesRDD }
import com.twosigma.flint.timeseries.row.Schema
import org.apache.spark.sql.types.{ DoubleType, IntegerType, StructType }
import org.apache.spark.sql.Row

import scala.io.Source
import org.scalactic.TolerantNumerics
import org.scalatest.FlatSpec
import play.api.libs.json.{ JsNull, JsString, JsValue, Json }

class ExponentialSmoothingSummarizerSpec extends FlatSpec with SharedSparkContext {

  val defaultPartitionParallelism: Int = 10

  val resourceDir = "/timeseries/summarize/summarizer/exponentialsmoothingsummarizer"

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

  private def meta(filename: String): JsValue =
    SpecUtils.withResource(s"$resourceDir/${filename}", suffix = "json") { source =>
      Json.parse(Source.fromFile(source).mkString)
    }

  implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(1.0E-8)

  def assertEquals(a: Array[Double], b: Array[Double]) {
    assert(a.corresponds(b)(_ === _))
  }

  "ExponentialSmoothingSummarizer" should "smooth correctly" in {
    val timeSeriesRdd = from("Price.csv", Schema("tid" -> IntegerType, "price" -> DoubleType, "expected" -> DoubleType))
    val result1 = timeSeriesRdd.addSummaryColumns(Summarizers.exponentialSmoothing(
      xColumn = "price",
      timestampsToPeriods = (a, b) => (b - a) / 100.0
    ), Seq("tid"))
    result1.rdd.collect().foreach(row => {
      val predVal = row.getAs[Double](ExponentialSmoothingSummarizer.esColumn)
      val trueVal = row.getAs[Double]("expected")
      assert(predVal === trueVal)
    })
  }

  "ExponentialSmoothingSummarizer" should "smooth sin correctly" in {
    def getSinRDDWithTID(tid: Int, constant: Double = 1.0): TimeSeriesRDD = {
      var rdd = Clocks.uniform(sc, "1d")
      rdd = rdd.addColumns("value" -> DoubleType -> { (row: Row) => constant })
      rdd = rdd.addColumns("tid" -> IntegerType -> { (row: Row) => tid })
      rdd.addColumns("expected" -> DoubleType -> { (row: Row) => constant })
    }

    var rdd = getSinRDDWithTID(1, 1.0).merge(getSinRDDWithTID(2, 2.0)).merge(getSinRDDWithTID(3, 3.0))
    val nanosToDays = (t1: Long, t2: Long) => (t2 - t1) / (24 * 60 * 60 * 1e9)
    rdd = rdd.addSummaryColumns(Summarizers.exponentialSmoothing(
      xColumn = "value",
      timestampsToPeriods = nanosToDays
    ), Seq("tid"))
    rdd.rdd.collect().foreach(row => {
      val predVal = row.getAs[Double](ExponentialSmoothingSummarizer.esColumn)
      val trueVal = row.getAs[Double]("expected")
      assert(predVal === trueVal)
    })
  }
}

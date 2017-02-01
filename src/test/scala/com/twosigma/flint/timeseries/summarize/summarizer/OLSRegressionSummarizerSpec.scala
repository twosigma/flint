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
import com.twosigma.flint.timeseries.{ Summarizers, CSV, TimeSeriesRDD }
import org.apache.spark.sql.types.DoubleType
import org.scalactic.TolerantNumerics
import org.scalatest.FlatSpec

import scala.collection.mutable

class OLSRegressionSummarizerSpec extends FlatSpec with SharedSparkContext {

  val defaultPartitionParallelism: Int = 10

  val resourceDir = "/timeseries/summarize/summarizer/olsregressionsummarizer"

  private def from(filename: String): TimeSeriesRDD = SpecUtils.withResource(s"$resourceDir/${filename}") { source =>
    CSV.from(
      sqlContext,
      s"file://$source",
      header = true,
      dateFormat = "yyyyMMdd",
      codec = "gzip",
      sorted = true
    ).repartition(defaultPartitionParallelism)
  }

  implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(1.0E-8)

  private def assertEquals(a: Array[Double], b: Array[Double]): Unit = assert(a.corresponds(b)(_ === _))

  "OLSRegressionSummarizer" should "regression with or without intercept correctly " in {
    val tsRdd = from("data.csv")
    val count = tsRdd.count()
    var result = tsRdd.summarize(Summarizers.OLSRegression("y", Seq("x1", "x2"), "w", true)).first()
    assert(result.getAs[Double](OLSRegressionSummarizer.interceptColumn) === 3.117181999992637)
    assert(result.getAs[Boolean](OLSRegressionSummarizer.hasInterceptColumn) == true)
    assert(result.getAs[Long](OLSRegressionSummarizer.samplesColumn) == count)
    assert(result.getAs[Double](OLSRegressionSummarizer.rColumn) === 0.23987985194607062)
    assert(result.getAs[Double](OLSRegressionSummarizer.rSquaredColumn) === 0.05754234336966876)
    assert(result.getAs[Double](OLSRegressionSummarizer.stdErrOfInterceptColumn) === 0.5351305295407137)
    assert(result.getAs[Double](OLSRegressionSummarizer.tStatOfInterceptColumn) === 5.825087203804313)
    assertEquals(
      result.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.betaColumn).toArray,
      Array(0.28007101558427594, 1.3162178418611101)
    )
    assertEquals(
      result.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.stdErrOfBetaColumn).toArray,
      Array(0.5870869011202909, 0.5582749581661886)
    )
    assertEquals(
      result.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.tStatOfBetaColumn).toArray,
      Array(0.4770520600099199, 2.3576515883581814)
    )

    result = tsRdd.summarize(Summarizers.OLSRegression("y", Seq("x1", "x2"), "w", false)).first()
    assert(result.getAs[Double](OLSRegressionSummarizer.interceptColumn) === 0.0)
    assert(result.getAs[Boolean](OLSRegressionSummarizer.hasInterceptColumn) == false)
    assert(result.getAs[Long](OLSRegressionSummarizer.samplesColumn) == count)
    assert(result.getAs[Double](OLSRegressionSummarizer.rColumn) === 0.19129580479059843)
    assert(result.getAs[Double](OLSRegressionSummarizer.rSquaredColumn) === 0.036594084930482745)
    assertEquals(
      result.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.betaColumn).toArray,
      Array(-0.18855696254850499, 1.2397406248059233)
    )
    assertEquals(
      result.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.stdErrOfBetaColumn).toArray,
      Array(0.672195067165334, 0.6451152214049083)
    )
    assertEquals(
      result.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.tStatOfBetaColumn).toArray,
      Array(-0.28050929225597476, 1.9217351934528257)
    )
  }

  it should "return NaN beta for singular matrix" in {
    val tsRdd = from("data.csv").addColumns("x3" -> DoubleType -> { _ => 0.0 })
    val result = tsRdd.summarize(Summarizers.OLSRegression("y", Seq("x3"), "w", false)).first()
    assert(
      result.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.betaColumn)(0).isNaN
    )
  }
}

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

package com.twosigma.flint.timeseries.summarize.summarizer.subtractable

import com.twosigma.flint.timeseries.Summarizers
import com.twosigma.flint.timeseries.summarize.SummarizerSuite
import com.twosigma.flint.timeseries.summarize.summarizer.OLSRegressionSummarizer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DoubleType

import scala.collection.mutable
import scala.util.Random

class OLSRegressionSummarizerSpec extends SummarizerSuite {

  override val defaultResourceDir = "/timeseries/summarize/summarizer/olsregressionsummarizer"

  private val dateFormat = "yyyyMMdd"

  "OLSRegressionSummarizer" should "regression with or without intercept correctly " in {
    val tsRdd = fromCSV("data.csv", dateFormat = dateFormat)
    val count = tsRdd.count()
    var result = tsRdd.summarize(Summarizers.OLSRegression("y", Seq("x1", "x2"), "w", true)).first()
    assert(result.getAs[Double](OLSRegressionSummarizer.interceptColumn) === 3.117181999992637)
    assert(result.getAs[Boolean](OLSRegressionSummarizer.hasInterceptColumn) == true)
    assert(result.getAs[Long](OLSRegressionSummarizer.samplesColumn) == count)
    assert(result.getAs[Double](OLSRegressionSummarizer.rColumn) === 0.23987985194607062)
    assert(result.getAs[Double](OLSRegressionSummarizer.rSquaredColumn) === 0.05754234336966876)
    assert(result.getAs[Double](OLSRegressionSummarizer.stdErrOfInterceptColumn) === 0.5351305295407137)
    assert(result.getAs[Double](OLSRegressionSummarizer.tStatOfInterceptColumn) === 5.825087203804313)
    assert(result.getAs[Double](OLSRegressionSummarizer.conditionColumn) === 1.4264121300439514)
    assert(result.getAs[Double](OLSRegressionSummarizer.logLikelihoodColumn) === -312.11292022635649)
    assert(result.getAs[Double](OLSRegressionSummarizer.akaikeICColumn) === 630.225840453)
    assert(result.getAs[Double](OLSRegressionSummarizer.bayesICColumn) === 638.041351011)

    assertAlmostEquals(
      result.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.betaColumn).toArray,
      Array(0.28007101558427594, 1.3162178418611101)
    )
    assertAlmostEquals(
      result.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.stdErrOfBetaColumn).toArray,
      Array(0.5870869011202909, 0.5582749581661886)
    )
    assertAlmostEquals(
      result.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.tStatOfBetaColumn).toArray,
      Array(0.4770520600099199, 2.3576515883581814)
    )

    result = tsRdd.summarize(Summarizers.OLSRegression("y", Seq("x1", "x2"), "w", false)).first()
    assert(result.getAs[Double](OLSRegressionSummarizer.interceptColumn) === 0.0)
    assert(result.getAs[Boolean](OLSRegressionSummarizer.hasInterceptColumn) == false)
    assert(result.getAs[Long](OLSRegressionSummarizer.samplesColumn) == count)
    assert(result.getAs[Double](OLSRegressionSummarizer.rColumn) === 0.19129580479059843)
    assert(result.getAs[Double](OLSRegressionSummarizer.rSquaredColumn) === 0.036594084930482745)
    assert(result.getAs[Double](OLSRegressionSummarizer.conditionColumn) === 1.1509375418)
    assert(result.getAs[Double](OLSRegressionSummarizer.logLikelihoodColumn) === -327.11113940398695)
    assert(result.getAs[Double](OLSRegressionSummarizer.akaikeICColumn) === 658.222278808)
    assert(result.getAs[Double](OLSRegressionSummarizer.bayesICColumn) === 663.43261918)

    assertAlmostEquals(
      result.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.betaColumn).toArray,
      Array(-0.18855696254850499, 1.2397406248059233)
    )
    assertAlmostEquals(
      result.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.stdErrOfBetaColumn).toArray,
      Array(0.672195067165334, 0.6451152214049083)
    )
    assertAlmostEquals(
      result.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.tStatOfBetaColumn).toArray,
      Array(-0.28050929225597476, 1.9217351934528257)
    )
  }

  it should "return NaN beta for singular matrix" in {
    val tsRdd = fromCSV("data.csv", dateFormat = dateFormat).addColumns("x3" -> DoubleType -> { _ => 0.0 })
    val result = tsRdd.summarize(Summarizers.OLSRegression("y", Seq("x3"), "w", shouldIntercept = false)).first()
    assert(
      result.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.betaColumn)(0).isNaN
    )
    assert(
      result.getAs[Double](OLSRegressionSummarizer.conditionColumn).isNaN
    )

    val constantsCol =
      result.getAs[mutable.WrappedArray[String]](OLSRegressionSummarizer.constantsColumn)
    assert(constantsCol.length == 1)
    assert(constantsCol(0) == "x3")
  }

  it should "not return beta as NaN for a column that is almost const" in {
    // Randomly picks 5 number fromCSV "x2"
    val randomSamplesOfX2 =
      Random.shuffle(fromCSV("data.csv", dateFormat = dateFormat).collect().map(_.getAs[Double]("x2")).toList).take(5)
    randomSamplesOfX2.foreach {
      x2 =>
        val tsRdd = fromCSV("data.csv", dateFormat = dateFormat).addColumns(
          "x3" -> DoubleType -> {
            r: Row => if (r.getAs[Double]("x2") == x2) 1.0 else 0.0
          }
        )
        val result = tsRdd.summarize(
          Summarizers.OLSRegression(
            "y", Seq("x1", "x2", "x3"), "w", shouldIntercept = false, shouldIgnoreConstants = true
          )
        ).first()
        assert(
          !result.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.betaColumn)(0).isNaN
        )
    }
  }

  it should "ignore const column(s) with intercept" in {
    val tsRdd = fromCSV("data.csv", dateFormat = dateFormat).addColumns("x3" -> DoubleType -> { _ => 2.0 })
    val result1 = tsRdd.summarize(
      Summarizers.OLSRegression(
        "y", Seq("x1", "x2"), "w", shouldIntercept = true, shouldIgnoreConstants = false
      )
    ).first()

    val result2 = tsRdd.summarize(
      Summarizers.OLSRegression(
        "y", Seq("x1", "x3", "x2"), "w", shouldIntercept = true, shouldIgnoreConstants = true
      )
    ).first()

    assert(result1.getAs[Double](OLSRegressionSummarizer.interceptColumn) ===
      result2.getAs[Double](OLSRegressionSummarizer.interceptColumn))
    assert(result1.getAs[Boolean](OLSRegressionSummarizer.hasInterceptColumn) ===
      result2.getAs[Boolean](OLSRegressionSummarizer.hasInterceptColumn))
    assert(result1.getAs[Long](OLSRegressionSummarizer.samplesColumn) ==
      result2.getAs[Long](OLSRegressionSummarizer.samplesColumn))
    assert(result1.getAs[Double](OLSRegressionSummarizer.rColumn) ===
      result2.getAs[Double](OLSRegressionSummarizer.rColumn))
    assert(result1.getAs[Double](OLSRegressionSummarizer.rSquaredColumn) ===
      result2.getAs[Double](OLSRegressionSummarizer.rSquaredColumn))
    assert(result1.getAs[Double](OLSRegressionSummarizer.stdErrOfInterceptColumn) ===
      result2.getAs[Double](OLSRegressionSummarizer.stdErrOfInterceptColumn))
    assert(result1.getAs[Double](OLSRegressionSummarizer.tStatOfInterceptColumn) ===
      result2.getAs[Double](OLSRegressionSummarizer.tStatOfInterceptColumn))
    assert(result1.getAs[Double](OLSRegressionSummarizer.logLikelihoodColumn) ===
      result2.getAs[Double](OLSRegressionSummarizer.logLikelihoodColumn))
    assert(result1.getAs[Double](OLSRegressionSummarizer.akaikeICColumn) ===
      result2.getAs[Double](OLSRegressionSummarizer.akaikeICColumn))
    assert(result1.getAs[Double](OLSRegressionSummarizer.bayesICColumn) ===
      result2.getAs[Double](OLSRegressionSummarizer.bayesICColumn))
    assert(result1.getAs[Double](OLSRegressionSummarizer.conditionColumn) ===
      result2.getAs[Double](OLSRegressionSummarizer.conditionColumn))

    val beta1 = result1.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.betaColumn)
    val beta2 = result2.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.betaColumn)
    assert(beta2(0) == beta1(0))
    assert(beta2(1) == 0.0)
    assert(beta2(2) == beta1(1))

    val stdErr1 = result1.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.stdErrOfBetaColumn)
    val stdErr2 = result2.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.stdErrOfBetaColumn)
    assert(stdErr2(0) == stdErr1(0))
    assert(stdErr2(1) == 0.0)
    assert(stdErr2(2) == stdErr1(1))

    val tStat1 = result1.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.tStatOfBetaColumn)
    val tStat2 = result2.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.tStatOfBetaColumn)
    assert(tStat2(0) == tStat1(0))
    assert(tStat2(1).isNaN)
    assert(tStat2(2) == tStat1(1))
  }

  it should "ignore const column(s) without intercept" in {
    val tsRdd = fromCSV("data.csv", dateFormat = dateFormat).addColumns("x3" -> DoubleType -> { _ => 2.0 })
    val result1 = tsRdd.summarize(
      Summarizers.OLSRegression(
        "y", Seq("x1", "x2"), "w", shouldIntercept = false, shouldIgnoreConstants = false
      )
    ).first()

    val result2 = tsRdd.summarize(
      Summarizers.OLSRegression(
        "y", Seq("x1", "x3", "x2"), "w", shouldIntercept = false, shouldIgnoreConstants = true
      )
    ).first()

    assert(result1.getAs[Double](OLSRegressionSummarizer.interceptColumn) ===
      result2.getAs[Double](OLSRegressionSummarizer.interceptColumn))
    assert(result1.getAs[Boolean](OLSRegressionSummarizer.hasInterceptColumn) ===
      result2.getAs[Boolean](OLSRegressionSummarizer.hasInterceptColumn))
    assert(result1.getAs[Long](OLSRegressionSummarizer.samplesColumn) ==
      result2.getAs[Long](OLSRegressionSummarizer.samplesColumn))
    assert(result1.getAs[Double](OLSRegressionSummarizer.rColumn) ===
      result2.getAs[Double](OLSRegressionSummarizer.rColumn))
    assert(result1.getAs[Double](OLSRegressionSummarizer.rSquaredColumn) ===
      result2.getAs[Double](OLSRegressionSummarizer.rSquaredColumn))
    assert(result1.getAs[Double](OLSRegressionSummarizer.logLikelihoodColumn) ===
      result2.getAs[Double](OLSRegressionSummarizer.logLikelihoodColumn))
    assert(result1.getAs[Double](OLSRegressionSummarizer.akaikeICColumn) ===
      result2.getAs[Double](OLSRegressionSummarizer.akaikeICColumn))
    assert(result1.getAs[Double](OLSRegressionSummarizer.bayesICColumn) ===
      result2.getAs[Double](OLSRegressionSummarizer.bayesICColumn))
    assert(result2.getAs[Double](OLSRegressionSummarizer.stdErrOfInterceptColumn).isNaN)
    assert(result2.getAs[Double](OLSRegressionSummarizer.tStatOfInterceptColumn).isNaN)
    assert(result1.getAs[Double](OLSRegressionSummarizer.conditionColumn) ===
      result2.getAs[Double](OLSRegressionSummarizer.conditionColumn))

    val constantsCol1 =
      result1.getAs[mutable.WrappedArray[String]](OLSRegressionSummarizer.constantsColumn)
    assert(constantsCol1.length == 0)
    val constantsCol2 =
      result2.getAs[mutable.WrappedArray[String]](OLSRegressionSummarizer.constantsColumn)
    assert(constantsCol2.length == 1)
    assert(constantsCol2(0) == "x3")

    val beta1 = result1.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.betaColumn)
    val beta2 = result2.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.betaColumn)
    assert(beta2(0) == beta1(0))
    assert(beta2(1) == 0.0)
    assert(beta2(2) == beta1(1))

    val stdErr1 = result1.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.stdErrOfBetaColumn)
    val stdErr2 = result2.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.stdErrOfBetaColumn)
    assert(stdErr2(0) == stdErr1(0))
    assert(stdErr2(1) == 0.0)
    assert(stdErr2(2) == stdErr1(1))

    val tStat1 = result1.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.tStatOfBetaColumn)
    val tStat2 = result2.getAs[mutable.WrappedArray[Double]](OLSRegressionSummarizer.tStatOfBetaColumn)
    assert(tStat2(0) == tStat1(0))
    assert(tStat2(1).isNaN)
    assert(tStat2(2) == tStat1(1))
  }

  it should "ignore null values" in {
    val tsRdd = fromCSV("data.csv", dateFormat = dateFormat)

    assertEquals(
      tsRdd.summarize(Summarizers.OLSRegression("y", Seq("x1", "x2"), "w")),
      insertNullRows(tsRdd, "x1").summarize(Summarizers.OLSRegression("y", Seq("x1", "x2"), "w"))
    )

    assertEquals(
      tsRdd.summarize(Summarizers.OLSRegression("y", Seq("x1", "x2"), "w")),
      insertNullRows(tsRdd, "x1", "w").summarize(Summarizers.OLSRegression("y", Seq("x1", "x2"), "w"))
    )
  }

  it should "pass summarizer property test" in {
    summarizerPropertyTest(AllPropertiesAndSubtractable)(
      Summarizers.OLSRegression("x0", Seq("x1", "x2"), "x3", shouldIntercept = false)
    )
    summarizerPropertyTest(AllPropertiesAndSubtractable)(
      Summarizers.OLSRegression("x0", Seq("x1", "x2"), "x3", shouldIntercept = true)
    )
  }
}

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

package com.twosigma.flint.math.stats.regression

import org.scalatest.FlatSpec
import org.apache.commons.math3.stat.regression.{
  OLSMultipleLinearRegression => OLSMultipleLinearRegressionModel
}
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import org.scalactic.TolerantNumerics

import com.twosigma.flint.SharedSparkContext
import com.twosigma.flint.SpecUtils

import breeze.linalg.{ DenseMatrix, DenseVector }

class LinearRegressionModelSpec extends FlatSpec with SharedSparkContext {
  var modelWithIntercept: LinearRegressionModel = _
  var modelWithoutIntercept: LinearRegressionModel = _
  var data: RDD[WeightedLabeledPoint] = _

  val betaWithoutIntercept: DenseVector[Double] = DenseVector(1.0, 2.0)
  val intercept: Double = 3.0
  val beta: DenseVector[Double] = DenseVector.vertcat(DenseVector(intercept), betaWithoutIntercept)
  val n: Long = 100
  val additivePrecision: Double = 1.0e-6
  val errorScalar = 5.0
  val seed = 31415926L
  val benchmarkModelWithIntercept: OLSMultipleLinearRegressionModel = new OLSMultipleLinearRegressionModel()
  val benchmarkModelWithoutIntercept: OLSMultipleLinearRegressionModel = new OLSMultipleLinearRegressionModel()

  implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(additivePrecision)

  override def beforeAll() {
    super.beforeAll()

    SpecUtils.withResource("/stat/regression/linear_regression_data.csv") { source =>
      val dataFile = scala.io.Source.fromFile(source)
      data = sc.parallelize(dataFile.getLines().map(WeightedLabeledPoint.parse(_)).toSeq, 4)

      // Build the model expected to test.
      modelWithIntercept = OLSMultipleLinearRegression.regression(data)
      modelWithoutIntercept = OLSMultipleLinearRegression.regression(data, false)

      // Build the models expected to serve as benchmarks.
      val (x1, y1) = WeightedLabeledPoint.toArrays(data, true)
      benchmarkModelWithIntercept.setNoIntercept(true)
      benchmarkModelWithIntercept.newSampleData(y1, x1)
      val (x2, y2) = WeightedLabeledPoint.toArrays(data, false)
      benchmarkModelWithoutIntercept.setNoIntercept(true)
      benchmarkModelWithoutIntercept.newSampleData(y2, x2)
    }
  }

  def assertEquals(a: Array[Double], b: Array[Double]) {
    assert(a.corresponds(b)(_ === _))
  }

  "The model" should "estimate regression parameters correctly" in {
    assertEquals(
      modelWithIntercept.estimateRegressionParameters().toArray,
      benchmarkModelWithIntercept.estimateRegressionParameters
    )
    assertEquals(
      modelWithoutIntercept.estimateRegressionParameters().toArray,
      benchmarkModelWithoutIntercept.estimateRegressionParameters
    )
  }

  it should "predict for a single data correctly by using parameter" in {
    val point = data.first()
    var parameters = modelWithIntercept.estimateRegressionParameters()
    var benchmark = DenseVector.vertcat(DenseVector(1.0), point.features) dot parameters
    assert(modelWithIntercept.predict(point) === benchmark)

    parameters = modelWithoutIntercept.estimateRegressionParameters()
    benchmark = point.features dot parameters
    assert(modelWithoutIntercept.predict(point) === benchmark)
  }

  it should "return N correctly" in {
    assert(modelWithIntercept.getN == n)
    assert(modelWithoutIntercept.getN == n)
  }

  it should "return K correctly" in {
    assert(modelWithIntercept.getK == beta.length)
    assert(modelWithoutIntercept.getK == betaWithoutIntercept.length)
  }

  it should "estimate the variance of regression parameters correctly" in {
    var benchmarkVariance = benchmarkModelWithIntercept.estimateRegressionParametersVariance()
    var estimateVariance = modelWithIntercept.estimateRegressionParametersVariance()
    for (i <- 0 until beta.length; j <- 0 until beta.length) {
      assert(benchmarkVariance(i)(j) === estimateVariance(i, j))
    }

    benchmarkVariance = benchmarkModelWithoutIntercept.estimateRegressionParametersVariance()
    estimateVariance = modelWithoutIntercept.estimateRegressionParametersVariance()
    for (i <- 0 until betaWithoutIntercept.length; j <- 0 until betaWithoutIntercept.length) {
      assert(benchmarkVariance(i)(j) === estimateVariance(i, j))
    }
  }

  it should "estimate the variance of regressand correctly" in {
    assert(benchmarkModelWithIntercept.estimateRegressandVariance() ===
      modelWithIntercept.estimateRegressandVariance())
    assert(benchmarkModelWithoutIntercept.estimateRegressandVariance() ===
      modelWithoutIntercept.estimateRegressandVariance())
  }

  it should "estimate the variance of error correctly" in {
    assert(benchmarkModelWithIntercept.estimateErrorVariance() ===
      modelWithIntercept.estimateErrorVariance())
    assert(benchmarkModelWithoutIntercept.estimateErrorVariance() ===
      modelWithoutIntercept.estimateErrorVariance())
  }

  it should "estimate the standard error correctly" in {
    assert(benchmarkModelWithIntercept.estimateRegressionStandardError() ===
      modelWithIntercept.estimateRegressionStandardError())
    assert(benchmarkModelWithoutIntercept.estimateRegressionStandardError() ===
      modelWithoutIntercept.estimateRegressionStandardError())
  }

  it should "estimate the error variance correctly" in {
    assert(benchmarkModelWithIntercept.estimateErrorVariance() ===
      modelWithIntercept.estimateErrorVariance())
    assert(benchmarkModelWithoutIntercept.estimateErrorVariance() ===
      modelWithoutIntercept.estimateErrorVariance())
  }

  it should "estimate the standard error of regression parameters correctly" in {
    assertEquals(
      modelWithIntercept.estimateRegressionParametersStandardErrors().toArray,
      benchmarkModelWithIntercept.estimateRegressionParametersStandardErrors
    )
    assertEquals(
      modelWithoutIntercept.estimateRegressionParametersStandardErrors().toArray,
      benchmarkModelWithoutIntercept.estimateRegressionParametersStandardErrors
    )
  }

  /*
   * The following function are verified via python 3.4 + statsmodel 0.6.1:
   * Scala:
   * val writer = new PrintWriter(new File("/tmp/data.csv"))
   * data.collect.foreach { p => writer.write(s"${p}\n") }
   * writer.flush
   * writer.close
   *
   * Python:
   * import statsmodels.api as sm
   * import pandas as pd
   * data = pd.read_csv('/tmp/data.csv', header=None, names=['label', 'weight', 'x1', 'x2'])
   * results = sm.WLS(data['label'], sm.add_constant(data[['x1', 'x2']]),
   *                  weights = data['weight']).fit()
   */

  it should "estimate the p-value of regression parameters correctly" in {
    // results.pvalues
    assertEquals(
      modelWithIntercept.calculateRegressionParametersPValue().toArray,
      Array(7.424409376177721E-8, 0.6343981017797162, 0.020398884794682992)
    )
    assertEquals(
      modelWithoutIntercept.calculateRegressionParametersPValue().toArray,
      Array(0.779678, 0.057544)
    )
  }

  it should "estimate the t-stat of regression parameters with intercept correctly" in {
    // results.tvalues
    assertEquals(
      modelWithIntercept.calculateRegressionParametersTStat().toArray,
      Array(5.825087203804314, 0.47705206000992, 2.357651588358182)
    )
    assertEquals(
      modelWithoutIntercept.calculateRegressionParametersTStat().toArray,
      Array(-0.280509, 1.921735)
    )
  }

  it should "calculate the centered and uncentered tss correctly" in {
    // results.uncentered_tss
    assert(modelWithIntercept.calculateUncenteredTSS() === 4439.54941656)
    assert(modelWithoutIntercept.calculateUncenteredTSS() === 4439.54941656)
    // results.centered_tss
    assert(modelWithIntercept.calculateCenteredTSS() === 3362.12208417)
    assert(modelWithoutIntercept.calculateCenteredTSS() === 3362.12208417)
  }

  it should "calculate the R-squared of a model with an intercept correctly" in {
    // results.rsquared
    assert(modelWithIntercept.calculateRSquared() === 0.0575423433697)
    assert(modelWithoutIntercept.calculateRSquared() === 0.0365940849305)
  }

  it should "calculate the sum of squared residues correctly" in {
    // results.ssr
    assert(modelWithIntercept.calculateSumOfSquaredResidue() === 3168.65770075)
    assert(modelWithoutIntercept.calculateSumOfSquaredResidue() === 4277.08816816)
  }

  it should "calculate the eigenvalues of Gramian matrix correctly" in {
    // results.eigenvals
    assertEquals(
      modelWithIntercept.calculateEigenvaluesOfGramianMatrix().toArray,
      Array(88.26381875450733, 104.31877096649953, 125.90058171543004)
    )
    assertEquals(
      modelWithoutIntercept.calculateEigenvaluesOfGramianMatrix().toArray,
      Array(93.96551756, 108.1484418)
    )
  }

  it should "calculate the White's (1980) heteroskedasticity robust estimator correctly" in {
    // results.cov_HC0
    var benchmark = DenseMatrix(
      (0.31466598, -0.06663163, -0.06194965),
      (-0.06663163, 0.59539918, -0.09442646),
      (-0.06194965, -0.09442646, 0.38442097)
    )
    assertEquals(modelWithIntercept.calculateHC0().toArray, benchmark.toArray)
    benchmark = DenseMatrix(
      (0.6279273, -0.10545145),
      (-0.10545145, 0.65404582)
    )
    assertEquals(modelWithoutIntercept.calculateHC0().toArray, benchmark.toArray)
  }

  it should "calculate the White's (1980) heteroskedasticity robust standard errors correctly" in {
    // results.HC0_se
    assertEquals(
      modelWithIntercept.calculateStandardErrorsOfHC0().toArray,
      Array(0.560951, 0.771621, 0.620017)
    )
    assertEquals(
      modelWithoutIntercept.calculateStandardErrorsOfHC0().toArray,
      Array(0.792419, 0.808731)
    )
  }

  it should "calculate the MacKinnon and White's (1985) heteroskedasticity robust estimator correctly" in {
    // results.cov_HC1
    var benchmark = DenseMatrix(
      (0.32439791, -0.0686924, -0.06386562),
      (-0.0686924, 0.61381359, -0.09734687),
      (-0.06386562, -0.09734687, 0.39631028)
    )
    assertEquals(modelWithIntercept.calculateHC1().toArray, benchmark.toArray)
    benchmark = DenseMatrix(
      (0.64074214, -0.10760352),
      (-0.10760352, 0.66739369)
    )
    assertEquals(modelWithoutIntercept.calculateHC1().toArray, benchmark.toArray)

  }

  it should "calculate the MacKinnon and White's (1985) heteroskedasticity robust standard errors correctly" in {
    // results.HC1_se
    assertEquals(
      modelWithIntercept.calculateStandardErrorsOfHC1().toArray,
      Array(0.569559, 0.783463, 0.629532)
    )
    assertEquals(
      modelWithoutIntercept.calculateStandardErrorsOfHC1().toArray,
      Array(0.800464, 0.816942)
    )
  }

  it should "calculate the log-likelihood correctly" in {
    assert(modelWithIntercept.estimateLogLikelihood() === -312.1129202263564)
    assert(modelWithoutIntercept.estimateLogLikelihood() === -327.111139403987)
  }

  it should "calculate the BIC correctly" in {
    assert(modelWithIntercept.estimateBayesianInformationCriterion() === 638.04135101067732)
    assert(modelWithoutIntercept.estimateBayesianInformationCriterion() === 663.4326191799502)
  }

  it should "calculate the AIC correctly" in {
    assert(modelWithIntercept.estimateAkaikeInformationCriterion() === 630.22584045271299)
    assert(modelWithoutIntercept.estimateAkaikeInformationCriterion() === 658.222278807974)
  }
}

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

package com.twosigma.flint.rdd.function.summarize.summarizer

import breeze.linalg.{ diag, inv, DenseVector, DenseMatrix, MatrixSingularException }
import scala.util.{ Success, Failure }
import scala.util.control.Exception._

case class OLSRegressionState(
  var count: Long,
  var matrixOfXX: DenseMatrix[Double],
  var vectorOfXY: DenseVector[Double],
  var sumOfYSquared: Double,
  var sumOfWeights: Double,
  var sumOfY: Double
)

case class OLSRegressionOutput(
  val count: Long,
  val beta: Array[Double], // beta without intercept
  val intercept: Double,
  val hasIntercept: Boolean,
  val stdErrOfBeta: Array[Double],
  val stdErrOfIntercept: Double,
  val rSquared: Double,
  val r: Double,
  val tStatOfIntercept: Double,
  val tStatOfBeta: Array[Double]
)

/**
 * @param dimensionOfX    The dimension of raw data input as predictors which doesn't include the intercept.
 * @param shouldIntercept Whether should include intercept in the regression.
 * @param isWeighted      Whether should use given weight. All predictors and responses will be multiplied by
 *                        sqrt-weight if it is true.
 */
class OLSRegressionSummarizer(
  dimensionOfX: Int,
  shouldIntercept: Boolean,
  isWeighted: Boolean
) extends Summarizer[RegressionRow, OLSRegressionState, OLSRegressionOutput] {

  import RegressionSummarizer._

  private val k: Int = if (shouldIntercept) {
    dimensionOfX + 1
  } else {
    dimensionOfX
  }

  override def zero(): OLSRegressionState = OLSRegressionState(
    count = 0L,
    matrixOfXX = DenseMatrix.zeros[Double](k, k),
    vectorOfXY = DenseVector.zeros[Double](k),
    0.0,
    0.0,
    0.0
  )

  override def merge(u1: OLSRegressionState, u2: OLSRegressionState): OLSRegressionState = {
    // Always create a new state
    val mergedU = zero()

    mergedU.count = u1.count + u2.count
    mergedU.matrixOfXX = u1.matrixOfXX + u2.matrixOfXX
    mergedU.vectorOfXY = u1.vectorOfXY + u2.vectorOfXY
    mergedU.sumOfYSquared = u1.sumOfYSquared + u2.sumOfYSquared
    mergedU.sumOfWeights = u1.sumOfWeights + u2.sumOfWeights
    mergedU.sumOfY = u1.sumOfY + u2.sumOfY

    mergedU
  }

  override def render(u: OLSRegressionState): OLSRegressionOutput =
    catching(classOf[MatrixSingularException]).withTry {
      inv(u.matrixOfXX)
    } match {
      case Success(matrixOfBetaVariance) =>
        val vectorOfBeta = matrixOfBetaVariance * u.vectorOfXY
        val beta = vectorOfBeta.toArray
        val residualSumOfSquares = computeResidualSumOfSquares(vectorOfBeta, u.sumOfYSquared, u.vectorOfXY, u.matrixOfXX)
        val errorVariance = residualSumOfSquares / (u.count - k)
        val vectorOfStdErrs = diag(matrixOfBetaVariance).map { betaVar => Math.sqrt(errorVariance * betaVar) }
        val stdErrs = vectorOfStdErrs.toArray
        val vectorOfTStat = vectorOfBeta :/ vectorOfStdErrs
        val tStat = vectorOfTStat.toArray

        val (intercept, primeBeta, stdErrOfIntercept, stdErrOfPrimeBeta, tStatOfIntercept, tStatOfPrimeBeta) =
          if (shouldIntercept) {
            (beta(0), beta.tail, stdErrs(0), stdErrs.tail, tStat(0), tStat.tail)
          } else {
            (0.0, beta, Double.NaN, stdErrs, Double.NaN, tStat)
          }

        val rSquared = computeRSquared(u.sumOfYSquared, u.sumOfWeights, u.sumOfY, residualSumOfSquares, shouldIntercept)

        OLSRegressionOutput(
          count = u.count,
          beta = primeBeta,
          intercept = intercept,
          hasIntercept = shouldIntercept,
          stdErrOfBeta = stdErrOfPrimeBeta,
          stdErrOfIntercept = stdErrOfIntercept,
          rSquared = rSquared,
          r = Math.sqrt(rSquared),
          tStatOfBeta = tStatOfPrimeBeta,
          tStatOfIntercept = tStatOfIntercept
        )

      case Failure(_) =>
        OLSRegressionOutput(
          count = u.count,
          beta = Array.fill(dimensionOfX)(Double.NaN),
          intercept = Double.NaN,
          hasIntercept = shouldIntercept,
          stdErrOfBeta = Array.fill(dimensionOfX)(Double.NaN),
          stdErrOfIntercept = Double.NaN,
          rSquared = Double.NaN,
          r = Double.NaN,
          tStatOfIntercept = Double.NaN,
          tStatOfBeta = Array.fill(dimensionOfX)(Double.NaN)
        )
    }

  override def add(u: OLSRegressionState, t: RegressionRow): OLSRegressionState = {
    val (xt, yt, yw) = RegressionSummarizer.transform(t, shouldIntercept, isWeighted)
    val vectorOfXt = DenseVector(xt)
    val matrixOfXt = vectorOfXt.asDenseMatrix
    u.matrixOfXX += matrixOfXt.t * matrixOfXt
    u.vectorOfXY += vectorOfXt * yt
    u.sumOfYSquared += yt * yt
    u.count += 1L
    u.sumOfWeights += yw._2
    u.sumOfY += yw._1 * yw._2
    u
  }
}

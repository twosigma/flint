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

package com.twosigma.flint.rdd.function.summarize.summarizer

import breeze.linalg._
import scala.util.{ Success, Failure }
import scala.util.control.Exception._

case class OLSRegressionState(
  var count: Long,
  var matrixOfXX: DenseMatrix[Double],
  var vectorOfXY: DenseVector[Double],
  var sumOfYSquared: Double,
  var sumOfWeights: Double,
  var sumOfLogWeights: Double,
  var sumOfY: Double,
  var rawPrimeConstXt: Option[Array[Double]],
  var primeConstCoordinates: Array[Int]
)

case class OLSRegressionOutput(
  count: Long,
  beta: Array[Double], // beta without intercept
  intercept: Double,
  hasIntercept: Boolean,
  stdErrOfBeta: Array[Double],
  stdErrOfIntercept: Double,
  rSquared: Double,
  r: Double,
  tStatOfIntercept: Double,
  tStatOfBeta: Array[Double],
  logLikelihood: Double,
  akaikeIC: Double,
  bayesIC: Double,
  cond: Double,
  constantsCoordinates: Array[Int]
)

/**
 * @param dimensionOfX          The dimension of raw data input as predictors which doesn't include the intercept.
 * @param shouldIntercept       Whether should include intercept in the regression.
 * @param isWeighted            Whether should use given weight. All predictors and responses will be multiplied by
 *                              sqrt-weight if it is true.
 * @param shouldIgnoreConstants Whether the regression should ignore columns of X that are constants.
 *                              When true, the scalar fields of regression result are the same as if
 *                              the constant columns are removed from X. The output beta, tStat, stdErr
 *                              still have the same dimension as that of rows in X. However, entries
 *                              corresponding to constant columns will have 0.0 for beta and stdErr;
 *                              and Double.NaN for tStat. When false, the regression will throw an
 *                              exception if X has constant columns.
 */
class OLSRegressionSummarizer(
  dimensionOfX: Int,
  shouldIntercept: Boolean,
  isWeighted: Boolean,
  shouldIgnoreConstants: Boolean = false
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
    0.0,
    0.0,
    None,
    Array.empty[Int]
  )

  override def merge(
    u1: OLSRegressionState,
    u2: OLSRegressionState
  ): OLSRegressionState =
    if (u1.count == 0) {
      u2
    } else if (u2.count == 0) {
      u1
    } else {
      // Always create a new state
      val mergedU = zero()

      mergedU.count = u1.count + u2.count
      mergedU.matrixOfXX = u1.matrixOfXX + u2.matrixOfXX
      mergedU.vectorOfXY = u1.vectorOfXY + u2.vectorOfXY
      mergedU.sumOfYSquared = u1.sumOfYSquared + u2.sumOfYSquared
      mergedU.sumOfWeights = u1.sumOfWeights + u2.sumOfWeights
      mergedU.sumOfLogWeights = u1.sumOfLogWeights + u2.sumOfLogWeights
      mergedU.sumOfY = u1.sumOfY + u2.sumOfY

      // Both u1 and u2 are from non-empty partitions.
      mergedU.rawPrimeConstXt = u1.rawPrimeConstXt
      mergedU.primeConstCoordinates = u1.rawPrimeConstXt.get.zip(u2.rawPrimeConstXt.get).map {
        case (x1, x2) => x1 == x2
      }.zipWithIndex.filter(_._1).map(_._2).toSet.intersect(
        u1.primeConstCoordinates.toSet
      ).intersect(
        u2.primeConstCoordinates.toSet
      ).toArray

      mergedU
    }

  /**
   * @return a `dim`-dimension array where the value of `coordinates`(i)-th entry is `values`(i)
   *         otherwise `defaultValue`.
   */
  private def stretch(
    coordinates: IndexedSeq[Int],
    dim: Int
  )(
    values: Array[Double],
    defaultValue: Double
  ): Array[Double] = {
    assert(coordinates.length == values.length)
    val stretched = Array.fill[Double](dim)(defaultValue)
    var i = 0
    while (i < coordinates.length) {
      stretched(coordinates(i)) = values(i)
      i = i + 1
    }
    stretched
  }

  /**
   * @return a new state by taking the sub-matrix of matrixOfXX and sub-vector of vectorOfXY
   *         from the given state. Also return a function to stretch beta, tStat etc. back to their
   *         original raw dimension.
   */
  private def shrink(
    u: OLSRegressionState
  ): (OLSRegressionState, (Array[Double], Double) => Array[Double]) = {
    val dim = u.rawPrimeConstXt.fold(0) { _.length }
    if (shouldIgnoreConstants) {
      val primCoordinates =
        ((0 until dim).toSet -- u.primeConstCoordinates).toIndexedSeq.sorted
      var coordinates = primCoordinates
      if (shouldIntercept) {
        coordinates = 0 +: primCoordinates.map(_ + 1)
      }
      val shrunk = u.copy(
        matrixOfXX = u.matrixOfXX(coordinates, coordinates).toDenseMatrix,
        vectorOfXY = u.vectorOfXY(coordinates).toDenseVector
      )
      (shrunk, stretch(primCoordinates, dim))
    } else {
      (u, stretch(0 until dim, dim))
    }
  }

  private def solve(u: OLSRegressionState): OLSRegressionOutput = {
    val condOfMatrixOfXX = cond(u.matrixOfXX)
    // May throw MatrixSingularException
    val matrixOfBetaVariance = inv(u.matrixOfXX)
    val vectorOfBeta = matrixOfBetaVariance * u.vectorOfXY
    val beta = vectorOfBeta.toArray
    val residualSumOfSquares =
      computeResidualSumOfSquares(
        vectorOfBeta,
        u.sumOfYSquared,
        u.vectorOfXY,
        u.matrixOfXX
      )
    val errorVariance = residualSumOfSquares / (u.count - beta.length)
    val vectorOfStdErrs = diag(matrixOfBetaVariance).map { betaVar =>
      Math.sqrt(errorVariance * betaVar)
    }
    val stdErrs = vectorOfStdErrs.toArray
    val vectorOfTStat = vectorOfBeta :/ vectorOfStdErrs
    val tStat = vectorOfTStat.toArray

    val (intercept,
      primeBeta,
      stdErrOfIntercept,
      stdErrOfPrimeBeta,
      tStatOfIntercept,
      tStatOfPrimeBeta) =
      if (shouldIntercept) {
        (beta(0), beta.tail, stdErrs(0), stdErrs.tail, tStat(0), tStat.tail)
      } else {
        (0.0, beta, Double.NaN, stdErrs, Double.NaN, tStat)
      }

    val logLikelihood =
      computeLogLikelihood(u.count, u.sumOfLogWeights, residualSumOfSquares)
    val akaikeIC =
      computeAkaikeIC(vectorOfBeta, logLikelihood, shouldIntercept)
    val bayesIC =
      computeBayesIC(vectorOfBeta, logLikelihood, u.count, shouldIntercept)
    val rSquared = computeRSquared(
      u.sumOfYSquared,
      u.sumOfWeights,
      u.sumOfY,
      residualSumOfSquares,
      shouldIntercept
    )

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
      tStatOfIntercept = tStatOfIntercept,
      logLikelihood = logLikelihood,
      akaikeIC = akaikeIC,
      bayesIC = bayesIC,
      cond = condOfMatrixOfXX,
      constantsCoordinates = u.primeConstCoordinates
    )
  }

  override def render(u: OLSRegressionState): OLSRegressionOutput = {
    val (shrunkState, stretchFn) = shrink(u)
    catching(classOf[MatrixSingularException]).withTry {
      solve(shrunkState)
    } match {
      case Success(o) =>
        o.copy(
          beta = stretchFn(o.beta, 0.0),
          stdErrOfBeta = stretchFn(o.stdErrOfBeta, 0.0),
          tStatOfBeta = stretchFn(o.tStatOfBeta, Double.NaN)
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
          tStatOfBeta = Array.fill(dimensionOfX)(Double.NaN),
          logLikelihood = Double.NaN,
          akaikeIC = Double.NaN,
          bayesIC = Double.NaN,
          cond = Double.NaN,
          constantsCoordinates = u.primeConstCoordinates
        )
    }
  }

  override def add(
    u: OLSRegressionState,
    t: RegressionRow
  ): OLSRegressionState = {
    val (xt, yt, yw) =
      RegressionSummarizer.transform(t, shouldIntercept, isWeighted)
    var i = 0
    // Update matrixOfXX
    while (i < xt.length) {
      var j = i
      while (j < xt.length) {
        val xij = xt(i) * xt(j)
        u.matrixOfXX.update(i, j, u.matrixOfXX(i, j) + xij)
        u.matrixOfXX.update(j, i, u.matrixOfXX(i, j))
        j += 1
      }
      i += 1
    }

    // Update vectorOfXY
    i = 0
    while (i < xt.length) {
      u.vectorOfXY.update(i, u.vectorOfXY(i) + xt(i) * yt)
      i += 1
    }

    u.sumOfYSquared += yt * yt
    u.count += 1L
    u.sumOfWeights += yw._2
    u.sumOfLogWeights += math.log(yw._2)
    u.sumOfY += yw._1 * yw._2

    if (u.rawPrimeConstXt.isEmpty) {
      // Should only be set once if this partition is non-empty.
      u.rawPrimeConstXt = Some(t.x)
      u.primeConstCoordinates = t.x.indices.toArray
    } else {
      val rawPrimeConstXt = u.rawPrimeConstXt.get
      var diffFound = false
      var i = 0
      while (i < u.primeConstCoordinates.length) {
        val c = u.primeConstCoordinates(i)
        if (t.x(c) != rawPrimeConstXt(c)) {
          u.primeConstCoordinates(i) = -1
          diffFound = true
        }
        i = i + 1
      }
      // Only have to do the filtering at most k times.
      if (diffFound) {
        u.primeConstCoordinates = u.primeConstCoordinates.filterNot(_ == -1)
      }
    }

    u
  }
}

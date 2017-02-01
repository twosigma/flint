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

import breeze.linalg.{ DenseVector, DenseMatrix, inv, diag, eigSym }
import org.apache.commons.math3.distribution.TDistribution
import org.apache.spark.rdd.RDD

trait LinearRegression {

  /**
   * Return the number of data points.
   *
   * @return the n.
   */
  def getN: Long

  /**
   * Return the dimension of data, i.e. the number of columns of the design matrix of X.
   *
   * @return the k.
   */
  def getK: Int

  /**
   * Return the regression coefficients using linear regression.
   *
   * @return the regression coefficients.
   */
  def estimateRegressionParameters(): DenseVector[Double]

  /**
   * Calculate the weighted mean of y, i.e.
   * <p><code>&Sigma; w<sub>i</sub>y<sub>i</sub> / &Sigma; w<sub>i</sub> </code>.
   * </p>
   *
   * @return the weighted mean of y.
   */
  def calculateWeightedMeanOfY(): Double

  /**
   * Calculate the mean of y, i.e.
   * <p><code>&Sigma; y<sub>i</sub> / n</code>.
   * </p>
   *
   * @return the mean of y.
   */
  def calculateMeanOfY(): Double

  /**
   * Calculate the mean of X, i.e.
   * <p><code>&Sigma; x<sub>i</sub> / n </code>
   * </p>
   *
   * @return the mean of X.
   */
  def calculateMeanOfX(): DenseVector[Double]

  /**
   * Calculates the total weighted sum of squares centered about the weighted mean.
   *
   * @return the total weighted sum of squares centered about the weighted mean.
   */
  def calculateCenteredTSS(): Double

  /**
   * Calculates the total weighted sum of uncentered squares.
   *
   * @return the total weighted sum of uncentered squares.
   */
  def calculateUncenteredTSS(): Double

  /**
   * Returns the variance of the regressand, i.e. <code>var(y)</code>.
   *
   * @return the variance of regressand.
   */
  def estimateRegressandVariance(): Double

  /**
   * Estimates the variance-covariance matrix of the regression parameters.
   * <p><code>var(b) = (X<sup>T</sup>X)<sup>-1</sup></code>
   * </p>
   *
   * @return the variance-covariance matrix of regression parameters.
   */
  def estimateRegressionParametersVariance(): DenseMatrix[Double]

  /**
   * Estimates the variance of the error.
   *
   * @return the variance of the error.
   */
  def estimateErrorVariance(): Double

  /**
   * Estimates the standard error of the regression parameters.
   *
   * @return the standard error of regression parameters.
   */
  def estimateRegressionParametersStandardErrors(): DenseVector[Double]

  /**
   * Estimates the log-likelihood of the regression parameters given the data u sing the MLE estimate of the variance
   * of the error given the data and assuming no prior on the regression parameters
   *
   * @return the log-likelihood of the regression parameters
   */
  def estimateLogLikelihood(): Double

  /**
   * Estimates the Bayesian information criterion of the regression parameters, i.e.
   * <pre><code> BIC(M;Y,X) = -2 ln L(M;Y,X) + k ln n </code>
   * </pre>
   *
   * @return the Bayesian information criterion of the regression parameters
   */
  def estimateBayesianInformationCriterion(): Double

  /**
   * Estimates the Akaike information criterion of the regression parameters, i.e.
   * <pre><code> AIC(M;Y,X) = -2 ln L(M;Y,X) + 2 k </code>
   * </pre>
   *
   * @return the Akaike information criterion of the regression parameters
   */
  def estimateAkaikeInformationCriterion(): Double

  /**
   * Calculates the Gramian matrix of X, i.e.
   * <p><code>Gramian(X) = X<sup>T</sup>X</code>
   * </p>
   *
   * @return the Gramian matrix of X.
   */
  def calculateGramianMatrix(): DenseMatrix[Double]

  /**
   * Calculates the eigenvalues of Gramian matrix of X, i.e.
   * <p><code>eigen(X<sup>T</sup>X)</code>
   * </p>
   *
   * @return the eigenvalues of Gramian matrix.
   */
  def calculateEigenvaluesOfGramianMatrix(): DenseVector[Double]

  /**
   * Estimates the standard error of the regression.
   *
   * @return the standard error of regression
   */
  def estimateRegressionStandardError(): Double = Math.sqrt(estimateErrorVariance())

  /**
   * Calculate the t stat of regression parameters.
   *
   * @return the t stat of regression parameters.
   */
  def calculateRegressionParametersTStat(): DenseVector[Double] = {
    val standardErrors = estimateRegressionParametersStandardErrors()
    val parameters = estimateRegressionParameters()
    parameters :/ standardErrors
  }

  /**
   * Calculate the p value of regression parameters.
   *
   * @return the t value of regression parameters.
   */
  def calculateRegressionParametersPValue(): DenseVector[Double] = {
    val tDist = new TDistribution(1.0 * (getN - getK))
    val tStat = calculateRegressionParametersTStat()
    tStat.map { t => 2.0 * (1.0 - tDist.cumulativeProbability(Math.abs(t))) }
  }
}

/**
 * The linear regression model can be represented in matrix-notation.
 * <pre>
 * y = X * b + u
 * </pre>
 * where y is an <code>n-vector</code> <b>regressand</b> obtained from WeightedLabeledPoint's
 * labels scaled by square root of weights respectively, X is a <code>[n,k]</code> matrix obtained
 * by aligning  WeightedLabeledPoint's features scaled by square root of weights respectively,
 * whose <code>k</code> columns are called <b>regressors</b>, b is <code>k-vector</code> of
 * <b>regression parameters</b> and <code>u</code> is an <code>n-vector</code>
 * of <b>error terms</b> or <b>residuals</b>.
 *
 * @param input RDD of WeightedLabeledPoint.
 * @param intercept Whether to use intercept.
 * @param n The number of points.
 * @param xx The k-by-k matrix of X^TX.
 * @param xy The k-vector of X^Ty.
 * @param swx The k-vector of X^Tw.
 * @param srwsl The square root weighted sum of labels.
 * @param ssrw The sum of square root of weights.
 * @param wsl The weighted sum of labels.
 * @param sw The sum of weights of WeightedLabeledPoint(s).
 * @param lw The sum of log weights of WeightedLabeledPoint(s).
 */
case class LinearRegressionModel(input: RDD[WeightedLabeledPoint], intercept: Boolean, n: Long,
  xx: DenseMatrix[Double], xy: DenseVector[Double], swx: DenseVector[Double], srwsl: Double,
  ssrw: Double, wsl: Double, sw: Double, lw: Double) extends LinearRegression {

  // Lazy variables will be calculated once only.
  protected lazy val beta: DenseVector[Double] = calculateBeta()
  protected lazy val standardErrorOfBeta: DenseVector[Double] = calculateRegressionParametersStandardErrors()
  protected lazy val varianceOfBeta: DenseMatrix[Double] = calculateBetaVariance()
  protected lazy val varianceOfY: Double = calculateVarianceOfY()
  protected lazy val varianceOfError: Double = calculateErrorVariance()
  protected lazy val (sumOfSquaredResidue, sumOfSquaredDeviationOfY) = calculateResidues(input, beta)
  protected lazy val (centeredTSS, uncenteredTSS) = calculateTSS(input)
  protected lazy val eigenvalues = eigSym.justEigenvalues(xx)
  protected lazy val hc0 = calculateHCRobustEstimator(input, xx, beta)
  protected lazy val logLikelihood: Double = calculateLogLikelihood()
  protected lazy val akaikeIC: Double = calculateAkaikeInformationCriterion()
  protected lazy val bayesIC: Double = calculateBayesianInformationCriterion()

  def getN: Long = n
  def getK: Int = beta.length
  def calculateMeanOfY(): Double = srwsl / n
  def calculateWeightedMeanOfY(): Double = srwsl / ssrw
  def calculateMeanOfX(): DenseVector[Double] = swx :/ n.toDouble
  def calculateGramianMatrix(): DenseMatrix[Double] = xx
  def calculateEigenvaluesOfGramianMatrix(): DenseVector[Double] = eigenvalues
  def calculateCenteredTSS(): Double = centeredTSS
  def calculateUncenteredTSS(): Double = uncenteredTSS
  def calculateSumOfSquaredResidue(): Double = sumOfSquaredResidue
  def estimateRegressionParameters(): DenseVector[Double] = beta
  def estimateRegressionParametersVariance(): DenseMatrix[Double] = varianceOfBeta
  def estimateRegressandVariance(): Double = varianceOfY
  def estimateErrorVariance(): Double = varianceOfError
  def estimateRegressionParametersStandardErrors(): DenseVector[Double] = standardErrorOfBeta
  def estimateLogLikelihood(): Double = logLikelihood
  def estimateAkaikeInformationCriterion(): Double = akaikeIC
  def estimateBayesianInformationCriterion(): Double = bayesIC

  /**
   * Calculate the White's (1980) heteroskedasticity robust estimator which is defined as
   * <pre><code>(X<sup>T</sup>X)<sup>-1</sup> diag(u)<sup>2</sup> (X<sup>T</sup>X).</code>
   * </pre>
   *
   * @return the White's (1980) heteroskedasticity robust estimator.
   */
  def calculateHC0(): DenseMatrix[Double] = hc0

  /**
   * Calculate the MacKinnon and White's (1985) heteroskedasticity robust estimator which is
   * defined as
   * <pre><code>
   * (n / (n - k))(X<sup>T</sup>X)<sup>-1</sup> diag(u)<sup>2</sup> (X<sup>T</sup>X).</code>
   * </pre>
   *
   * @return the MacKinnon and White's (1985) heteroskedasticity robust estimator.
   */
  def calculateHC1(): DenseMatrix[Double] = hc0 :* (1.0 * getN / (getN - getK))

  /**
   * Calculate the White's (1980) heteroskedasticity robust standard errors which is defined as
   * <pre><code>sqrt(diag(hc<sub>0</sub>))</code>
   * </pre>
   * where <code>hc<sub>1</sub></code> is the White's (1980) heteroskedasticity robust estimator.
   *
   * @return the White's (1980) heteroskedasticity robust standard errors.
   */
  def calculateStandardErrorsOfHC0(): DenseVector[Double] = diag(hc0).map(Math.sqrt _)

  /**
   * Calculate the MacKinnon and White's (1985) heteroskedasticity robust standard errors which
   * is defined as
   * <pre><code>sqrt(diag(hc<sub>1</sub>))</code>
   * </pre>
   * where <code>hc<sub>1</sub></code> is the the MacKinnon and White's (1985)
   * heteroskedasticity robust estimator.
   *
   * @return the MacKinnon and White's (1985) heteroskedasticity robust standard errors.
   */
  def calculateStandardErrorsOfHC1(): DenseVector[Double] = {
    val scalar: Double = 1.0 * getN / (getN - getK)
    diag(hc0).map { c => Math.sqrt(scalar * c) }
  }

  /**
   * R-squared of a model is defined here as
   * <pre><code> 1 - u<sup>T</sup>u / tss </sup></code>
   * </pre>
   * where tss is the total sum of squares and it is centered if the constant is included in the
   * model and uncentered if the constant is omitted.
   *
   * @return the R-squared of the linear model.
   */
  def calculateRSquared(): Double = {
    if (intercept) {
      1.0 - sumOfSquaredResidue / centeredTSS
    } else {
      1.0 - sumOfSquaredResidue / uncenteredTSS
    }
  }

  /**
   * Predict values for a single data point using the model trained.
   *
   * @param data A single data point
   * @return the prediction from the trained model.
   */
  def predict(data: WeightedLabeledPoint): Double = predict(data.features)

  /**
   * Predict values for a single data point using the model trained.
   *
   * @param data A single data point
   * @return the prediction from the trained model.
   */
  def predict(data: DenseVector[Double]): Double =
    if (intercept) data.t * beta(1 to -1) + beta(0) else data.t * beta

  /**
   * Predict values for the given data set using the model trained.
   *
   * @param data The RDD of data points to be predicted.
   * @return RDD[Double] where each entry contains the corresponding prediction.
   */
  def predict(data: RDD[WeightedLabeledPoint]): RDD[Double] = data.map(predict)

  /**
   * Calculates the regression coefficients using OLS.
   *
   * @return The beta.
   */
  protected def calculateBeta(): DenseVector[Double] = inv(xx) * xy

  /**
   * Calculates the variance-covariance matrix of the regression parameters.
   * <pre><code>var(b) = (X<sup>T</sup>X)<sup>-1</sup></code>
   * </pre>
   *
   * @return The beta variance-covariance matrix.
   */
  protected def calculateBetaVariance(): DenseMatrix[Double] = inv(xx)

  /**
   * Calculates the sum of squared residues and the sum of squared derivation of y.
   *
   * @param input The RDD of (label, weight, array of features) tuples.
   * @param beta The estimate regression parameters.
   * @return a pair of the sum of squared residues and the sum of squared derivation of y.
   */
  protected def calculateResidues(input: RDD[WeightedLabeledPoint], beta: DenseVector[Double]) = {
    // Materialize the lazy variable before RDD.map()
    val meanOfY = calculateMeanOfY()
    // 1. The sum of squared residues
    // 2. The sum of squared deviation
    input.treeAggregate((0.0, 0.0))(
      seqOp = (U, v) => {
      val w = Math sqrt v.weight
      val residue = if (intercept) {
        v.features.t * beta(1 to -1) + beta(0) - v.label
      } else {
        v.features.t * beta - v.label
      }
      val deviationOfY = w * v.label - meanOfY
      (U._1 + residue * residue * v.weight, U._2 + deviationOfY * deviationOfY)
    }, combOp = (U1, U2) => (U1._1 + U2._1, U1._2 + U2._2)
    )
  }

  /**
   * Calculates the total (weighted) sum of squares centered (and uncentered) about the mean.
   *
   * @param input The RDD of (label, weight, array of features) tuples.
   * @return the total (weighted) sum of squares centered (and uncentered) about the mean.
   */
  protected def calculateTSS(input: RDD[WeightedLabeledPoint]): (Double, Double) = {
    // Materialize the lazy variable before RDD.map()
    val weightedMeanOfY = wsl / sw
    // 1. centeredTSS
    // 2. uncenteredTSS
    input.treeAggregate((0.0, 0.0))(
      seqOp = (U, v) => {
      val c = v.label - weightedMeanOfY
      (U._1 + c * c * v.weight, U._2 + v.label * v.label * v.weight)
    }, combOp = (U1, U2) => (U1._1 + U2._1, U1._2 + U2._2)
    )
  }

  /**
   * Calculates the heteroscedasticity-consistent robust esitmator(s).
   * <p>
   * HC0, White's (1980) heteroskedasticity robust estimator.
   * <pre><code>(X<sup>T</sup> X)<sup>-1</sup> diag(u)<sup>2</sup> (X<sup>T</sup> X)</code>.
   * </pre>
   *
   * @param input The RDD of (label, weight, array of features) tuples.
   * @param xx The k-by-k matrix of X^TX.
   * @param beta The regression coefficients using.
   * @return the various heteroscedasticity-consistent robust estimator(s).
   */
  protected def calculateHCRobustEstimator(
    input: RDD[WeightedLabeledPoint],
    xx: DenseMatrix[Double], beta: DenseVector[Double]
  ) = {
    val invXX = inv(xx)
    val k = getK
    val sigma = input.treeAggregate(new DenseMatrix[Double](k, k))(
      seqOp = (U, v) => {
      var xe = if (intercept) {
        val x = DenseVector.vertcat(DenseVector(1.0), v.features)
        x * (v.features.t * beta(1 to -1) + beta(0) - v.label)
      } else {
        v.features * (v.features.t * beta - v.label)
      }
      xe = xe :* v.weight
      U += xe.asDenseMatrix.t * xe.asDenseMatrix
    }, combOp = (U1, U2) => { U1 += U2 }
    )
    invXX * sigma * invXX
  }

  /**
   * Calculates the variance of Y.
   *
   * @return the variance of y.
   */
  protected def calculateVarianceOfY(): Double = sumOfSquaredDeviationOfY / (getN - 1)

  /**
   * Calculates the variance of the error term.
   * Uses the formula
   * <pre><code> var(u) = u &middot; u / (n - k) </code>
   * </pre>
   * where n and k are the row and column dimensions of the design matrix X.
   *
   * @return error variance estimate.
   */
  protected def calculateErrorVariance(): Double = sumOfSquaredResidue / (getN - getK)

  /**
   * Calculates the standard error of regression parameters.
   *
   * @return the standard error of regression parameters.
   */
  protected def calculateRegressionParametersStandardErrors(): DenseVector[Double] = {
    val betaVariance = estimateRegressionParametersVariance()
    val sigma = calculateErrorVariance()
    diag(betaVariance).map { betaVar => Math.sqrt(sigma * betaVar) }
  }

  /**
   * Calculates the log-likelihood of the model given the input data and assuming the data are drawn as
   * <pre><code> Y = M(X) + &epsilon; </code>
   * </pre>
   * and
   * <pre><code> &epsilon; ~ N(0, &sigma;<sup>2</sup>)</code>
   * </pre>
   * where <code>&sigma;<sup>2</sup></code> is the maximum likelihood estimate of the error variance
   *
   * @return log-likelihood
   */
  protected def calculateLogLikelihood(): Double = {
    -0.5 * getN * (math.log(sumOfSquaredResidue) + 1 + math.log(2.0 * math.Pi / getN)) + 0.5 * lw
  }

  /**
   * Calculates the Bayesian information criterion of the linear model given the input data and assuming
   * no prior is applied to the parameters
   *
   * Bayesian information criterion is defined as
   * <pre><code> BIC(M;Y,X) = -2 ln L(M;Y,X) + k ln n </code>
   * </pre>
   * where we assume the data are drawn as
   * <pre><code> Y = M(X) + &epsilon; </code>
   * </pre>
   * and
   * <pre><code> &epsilon; ~ N(0, &sigma;<sup>2</sup>)</code>
   * </pre>
   * where <code>&sigma;<sup>2</sup></code> is the maximum likelihood estimate of the error variance,
   * <code> SSE(M) / n </code>
   *
   * @return Bayes information criterion
   */
  def calculateBayesianInformationCriterion(): Double = -2.0 * logLikelihood + getK * math.log(getN.toDouble)

  /**
   * Calculates the Akaike information criterion of the linear model given the input data and assuming no prior is
   * applied to the parameters
   *
   * Akaike information criterion is defined as
   * <pre><code> AIC(M;Y,X) = -2 ln L(M;Y,X) + 2 k </code>
   * </pre>
   * where we assume the data are drawn as
   * <pre><code> Y = M(X) + &epsilon; </code>
   * </pre>
   * and
   * <pre><code> &epsilon; ~ N(0, &sigma;<sup>2</sup>)</code>
   * </pre>
   * where <code>&sigma;<sup>2</sup></code> is the maximum likelihood estimate of the error variance,
   * <code> SSE(M) / n </code>
   *
   * @return Akaike information criterion
   */
  def calculateAkaikeInformationCriterion(): Double = -2.0 * logLikelihood + 2.0 * getK
}

object LinearRegressionModel {
}

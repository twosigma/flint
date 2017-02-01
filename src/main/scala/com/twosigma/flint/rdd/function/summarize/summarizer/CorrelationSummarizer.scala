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

import com.twosigma.flint.math.Kahan
import scala.math._

case class CorrelationState(
  var count: Long,
  val covariance: Kahan,
  var xMean: NthMomentState,
  var yMean: NthMomentState,
  var xVariance: NthCentralMomentState,
  var yVariance: NthCentralMomentState
)

case class CorrelationOutput(
  val covariance: Double,
  val correlation: Double,
  val tStat: Double,
  val observationCount: Long
)

// This summarizer uses mutable state
case class CorrelationSummarizer()
  extends Summarizer[(Double, Double), CorrelationState, CorrelationOutput] {

  val meanSummarizer = NthMomentSummarizer(1)
  val varianceSummarizer = NthCentralMomentSummarizer(2)

  override def zero(): CorrelationState = CorrelationState(
    0,
    Kahan(),
    meanSummarizer.zero(),
    meanSummarizer.zero(),
    varianceSummarizer.zero(),
    varianceSummarizer.zero()
  )

  override def add(u: CorrelationState, data: (Double, Double)): CorrelationState = {
    val (x, y) = data

    u.count += 1
    if (u.count > 1) {
      val xMean = meanSummarizer.render(u.xMean)
      val yMean = meanSummarizer.render(u.yMean)
      u.covariance.add(
        ((u.count - 1d) / u.count) * (x - xMean) * (y - yMean)
      )
    }

    u.xMean = meanSummarizer.add(u.xMean, x)
    u.yMean = meanSummarizer.add(u.yMean, y)
    u.xVariance = varianceSummarizer.add(u.xVariance, x)
    u.yVariance = varianceSummarizer.add(u.yVariance, y)

    u
  }

  override def merge(u1: CorrelationState, u2: CorrelationState): CorrelationState = {
    if (u1.count == 0) {
      u2
    } else if (u2.count == 0) {
      u1
    } else {
      u1.covariance.add(u2.covariance)

      val xMean1 = meanSummarizer.render(u1.xMean)
      val yMean1 = meanSummarizer.render(u1.yMean)
      val xMean2 = meanSummarizer.render(u2.xMean)
      val yMean2 = meanSummarizer.render(u2.yMean)

      val deltaX = xMean2 - xMean1
      val deltaY = yMean2 - yMean1
      u1.covariance.add(deltaX * deltaY * (u1.count * u2.count) / (u1.count + u2.count))

      u1.count += u2.count
      u1.xMean = meanSummarizer.merge(u1.xMean, u2.xMean)
      u1.yMean = meanSummarizer.merge(u1.yMean, u2.yMean)
      u1.xVariance = varianceSummarizer.merge(u1.xVariance, u2.xVariance)
      u1.yVariance = varianceSummarizer.merge(u1.yVariance, u2.yVariance)

      u1
    }
  }

  override def render(u: CorrelationState): CorrelationOutput = {
    if (u.count == 0) {
      CorrelationOutput(Double.NaN, Double.NaN, Double.NaN, 0)
    } else {
      val covariance = u.covariance.getValue() / u.count
      val xDev = sqrt(varianceSummarizer.render(u.xVariance).nthCentralMoment(2))
      val yDev = sqrt(varianceSummarizer.render(u.yVariance).nthCentralMoment(2))
      // correlation should be in [-1.0, 1.0] interval
      val correlation = max(-1.0, min(1.0, covariance / (xDev * yDev)))
      val tStat = correlation * sqrt(u.count - 2d) / sqrt(1.0 - correlation * correlation)

      CorrelationOutput(covariance, correlation, tStat, u.count)
    }
  }
}

/**
 * The state of [[MultiCorrelationSummarizer]].
 *
 * @param states Each element is the correlation summarizer state for a pair of column i and column j where i < j.
 */
case class MultiCorrelationState(val states: Array[(Int, Int, CorrelationState)])

/**
 * The output of [[MultiCorrelationSummarizer]].
 *
 * @param outputs Each element is the correlation output for a pair of column i and column j where i < j.
 */
case class MultiCorrelationOutput(val outputs: Array[(Int, Int, CorrelationOutput)])

/**
 * The summarizer to calculate the pairwise correlations between different columns.
 *
 * @param cols        The number of different columns.
 * @param pairIndexes All the pairs of columns indexes that are expected to calculate the correlations between.
 */
case class MultiCorrelationSummarizer(
  cols: Int,
  pairIndexes: IndexedSeq[(Int, Int)]
) extends Summarizer[Array[Double], MultiCorrelationState, MultiCorrelationOutput] {

  private val summarizer: CorrelationSummarizer = CorrelationSummarizer()

  override def zero(): MultiCorrelationState =
    MultiCorrelationState(pairIndexes.map { case (i, j) => (i, j, summarizer.zero()) }.toArray)

  override def add(mu: MultiCorrelationState, data: Array[Double]): MultiCorrelationState = {
    mu.states.foreach { case (i, j, u) => summarizer.add(u, (data(i), data(j))) }
    mu
  }

  override def merge(mu1: MultiCorrelationState, mu2: MultiCorrelationState): MultiCorrelationState =
    MultiCorrelationState(mu1.states.zip(mu2.states).map {
      case ((i1, j1, u1), (i2, j2, u2)) =>
        require(i1 == i2 && j1 == j2)
        (i1, j1, summarizer.merge(u1, u2))
    })

  override def render(mu: MultiCorrelationState): MultiCorrelationOutput =
    MultiCorrelationOutput(mu.states.map{ case (i, j, u) => (i, j, summarizer.render(u)) })
}

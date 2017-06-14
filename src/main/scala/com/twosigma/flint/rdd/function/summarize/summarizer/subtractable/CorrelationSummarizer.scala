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

package com.twosigma.flint.rdd.function.summarize.summarizer.subtractable

import com.twosigma.flint.math.Kahan

import scala.math._

case class CorrelationState(
  var count: Long,
  covariance: Kahan,
  var xMean: NthMomentState,
  var yMean: NthMomentState,
  var xVariance: NthCentralMomentState,
  var yVariance: NthCentralMomentState
)

case class CorrelationOutput(
  covariance: Double,
  correlation: Double,
  tStat: Double,
  observationCount: Long
)

// This summarizer uses mutable state
case class CorrelationSummarizer()
  extends LeftSubtractableSummarizer[(Double, Double), CorrelationState, CorrelationOutput] {

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

    u.count += 1L
    if (u.count > 1L) {
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

  override def subtract(u: CorrelationState, data: (Double, Double)): CorrelationState = {
    require(u.count != 0L)
    if (u.count == 1L) {
      zero()
    } else {
      val (x, y) = data

      u.xMean = meanSummarizer.subtract(u.xMean, x)
      u.yMean = meanSummarizer.subtract(u.yMean, y)
      u.xVariance = varianceSummarizer.subtract(u.xVariance, x)
      u.yVariance = varianceSummarizer.subtract(u.yVariance, y)

      val xMean = meanSummarizer.render(u.xMean)
      val yMean = meanSummarizer.render(u.yMean)
      u.covariance.add(
        -((u.count - 1d) / u.count) * (x - xMean) * (y - yMean)
      )
      u.count -= 1L

      u
    }
  }

  override def merge(u1: CorrelationState, u2: CorrelationState): CorrelationState = {
    if (u1.count == 0L) {
      u2
    } else if (u2.count == 0L) {
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
    if (u.count == 0L) {
      CorrelationOutput(Double.NaN, Double.NaN, Double.NaN, 0L)
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

/*
 *  Copyright 2018 TWO SIGMA OPEN SOURCE, LLC
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

case class WeightedCovarianceState(
  var count: Long, // number of observations
  var meanX: Double, // mean of x's
  var meanY: Double, // mean of y's
  sumWeight: Kahan, // sum of weights
  sumWeightSquare: Kahan, // sum of square of weights
  var coMoment: Double // sum of w_i * (x_i - meanX) * (y_i - meanY)
)

case class WeightedCovarianceOutput(
  covariance: Double,
  observationCount: Long
)

/**
 * This implementation follow the [[https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Covariance]].
 */
class WeightedCovarianceSummarizer
  extends FlippableSummarizer[(Double, Double, Double), WeightedCovarianceState, WeightedCovarianceOutput] {

  override def zero(): WeightedCovarianceState = WeightedCovarianceState(
    0L,
    0d,
    0d,
    new Kahan(),
    new Kahan(),
    0d
  )

  override def add(
    u: WeightedCovarianceState,
    t: (Double, Double, Double)
  ): WeightedCovarianceState = {

    val (x, y, w) = t
    u.count += 1L

    // Note that if the weight is negative, we don't take its absolute value.
    u.sumWeight.add(w)
    u.sumWeightSquare.add(w * w)
    val dx = x - u.meanX
    u.meanX += (w / u.sumWeight.value()) * dx
    u.meanY += (w / u.sumWeight.value()) * (y - u.meanY)
    u.coMoment += w * dx * (y - u.meanY)

    u
  }

  override def merge(
    u1: WeightedCovarianceState,
    u2: WeightedCovarianceState
  ): WeightedCovarianceState = {
    u1.count += u2.count

    val sumW1 = u1.sumWeight.value()
    val sumW2 = u2.sumWeight.value()

    val sumX1 = u1.meanX * sumW1
    val sumX2 = u2.meanX * sumW2
    val sumY1 = u1.meanY * sumW1
    val sumY2 = u2.meanY * sumW2

    // u1.sumWeight has been updated to be the total weight
    u1.sumWeight.add(u2.sumWeight)
    u1.sumWeightSquare.add(u2.sumWeightSquare)
    val sumW = u1.sumWeight.value()

    u1.coMoment += u2.coMoment + (u1.meanX - u2.meanX) * (u1.meanY - u2.meanY) * sumW1 * sumW2 / sumW

    u1.meanX = (sumX1 + sumX2) / sumW
    u1.meanY = (sumY1 + sumY2) / sumW

    u1
  }

  override def render(u: WeightedCovarianceState): WeightedCovarianceOutput =
    WeightedCovarianceOutput(
      covariance = u.coMoment / (u.sumWeight.value() - u.sumWeightSquare.value() / u.sumWeight.value()),
      observationCount = u.count
    )

}

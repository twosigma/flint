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

/**
 * Calculates the weighted mean, weighted deviation, weighted t-stat, and the count of observations.
 *
 * Implemented based on
 * [[http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Weighted_incremental_algorithm Weighted incrememtal algorithm]] and
 * [[http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm Parallel algorithm]]
 * and replaces all "n" with corresponding "SumWeight"
 *
 */
case class WeightedMeanTestState(
  val count: Long,
  val sumWeight: Kahan,
  val mean: Kahan,
  val sumSquareOfDiffFromMean: Kahan,
  val sumSquareOfWeights: Kahan
)

case class WeightedMeanTestOutput(
  val weighedMean: Double,
  val weightedStardardDeviation: Double,
  val weightedTstat: Double,
  val observationCount: Long
)

case class WeightedMeanTestSummarizer()
  extends Summarizer[(Double, Double), WeightedMeanTestState, WeightedMeanTestOutput] {
  override def zero(): WeightedMeanTestState = WeightedMeanTestState(0, Kahan(), Kahan(), Kahan(), Kahan())

  override def add(u: WeightedMeanTestState, data: (Double, Double)): WeightedMeanTestState = {
    val (rawValue, rawWeight) = data

    val value = rawValue * signum(rawWeight)
    val weight = abs(rawWeight)

    val oldSumWeight = u.sumWeight.getValue()
    u.sumWeight.add(weight)
    val delta = value - u.mean.getValue()
    val R = delta * weight / u.sumWeight.getValue()

    u.mean.add(R)
    u.sumSquareOfDiffFromMean.add(oldSumWeight * delta * R)
    u.sumSquareOfWeights.add(weight * weight)

    WeightedMeanTestState(
      u.count + 1,
      u.sumWeight,
      u.mean,
      u.sumSquareOfDiffFromMean,
      u.sumSquareOfWeights
    )
  }

  override def merge(u1: WeightedMeanTestState, u2: WeightedMeanTestState): WeightedMeanTestState = {
    if (u1.count == 0) {
      u2
    } else if (u2.count == 0) {
      u1
    } else {
      val delta = u2.mean.subtract(u1.mean)
      val oldSumWeight = u1.sumWeight.getValue()

      u1.sumWeight.add(u2.sumWeight)
      u1.mean.add(u2.sumWeight.getValue() * delta / u1.sumWeight.getValue())
      u1.sumSquareOfDiffFromMean.add(u2.sumSquareOfDiffFromMean)
      u1.sumSquareOfDiffFromMean.add(delta * delta * oldSumWeight * u2.sumWeight.getValue() / u1.sumWeight.getValue())
      u1.sumSquareOfWeights.add(u2.sumSquareOfWeights)

      WeightedMeanTestState(
        u1.count + u2.count,
        u1.sumWeight,
        u1.mean,
        u1.sumSquareOfDiffFromMean,
        u1.sumSquareOfWeights
      )
    }
  }

  override def render(u: WeightedMeanTestState): WeightedMeanTestOutput = {
    val sumOfWeights = u.sumWeight.getValue()
    val variance = u.sumSquareOfDiffFromMean.getValue() * sumOfWeights /
      (sumOfWeights * sumOfWeights - u.sumSquareOfWeights.getValue())
    val effectiveSampleSize = sumOfWeights * sumOfWeights / u.sumSquareOfWeights.getValue()
    val stdDev = sqrt(variance)
    val tStat = sqrt(effectiveSampleSize) * u.mean.getValue() / stdDev
    WeightedMeanTestOutput(u.mean.getValue(), stdDev, tStat, u.count)
  }
}

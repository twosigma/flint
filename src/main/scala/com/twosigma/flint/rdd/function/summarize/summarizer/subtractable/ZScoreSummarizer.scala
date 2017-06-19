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

/**
 * Computes the z-score with the option for out-of-sample calculation. Formulas are taken from
 * "Numerically Stable, Single-Pass, Parallel Statistics Algorithms" by Janine Bennett et al.
 *
 * http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.214.8508&rep=rep1&type=pdf
 */
case class ZScoreState(
  var count: Long,
  var value: Double,
  var meanState: WeightedMeanTestState
)

// This summarizer uses mutable state
case class ZScoreSummarizer(includeCurrentObservation: Boolean)
  extends LeftSubtractableSummarizer[Double, ZScoreState, Double] {

  // We only need the unweighted mean so we always pass in a weight of 1
  val meanSummarizer = WeightedMeanTestSummarizer()

  override def zero(): ZScoreState = ZScoreState(
    0L,
    Double.NaN,
    meanSummarizer.zero()
  )

  override def add(u: ZScoreState, value: Double): ZScoreState = {
    u.count += 1L

    if (includeCurrentObservation) {
      u.meanState = meanSummarizer.add(u.meanState, (value, 1))
    } else if (u.count > 1L) {
      u.meanState = meanSummarizer.add(u.meanState, (u.value, 1))
    }
    u.value = value

    u
  }

  override def subtract(u: ZScoreState, value: Double): ZScoreState = {
    u.count -= 1L
    u.meanState = meanSummarizer.subtract(u.meanState, (value, 1))
    if (u.count == 0L) {
      u.value = Double.NaN
    }
    u
  }

  override def merge(u1: ZScoreState, u2: ZScoreState): ZScoreState = {
    // Add u1's stored value if it was not already added
    if (u1.count > 0L && !includeCurrentObservation) {
      u1.meanState = meanSummarizer.add(u1.meanState, (u1.value, 1))
    }
    u1.meanState = meanSummarizer.merge(u1.meanState, u2.meanState)
    u1.count += u2.count
    // Use u2's value because it came later
    u1.value = u2.value

    u1
  }

  override def render(u: ZScoreState): Double = {
    var zScore = Double.NaN

    if (u.value != Double.NaN) {
      val meanTestOutput = meanSummarizer.render(u.meanState)
      if (meanTestOutput.observationCount > 1) {
        zScore = (u.value - meanTestOutput.weighedMean) / meanTestOutput.weightedStardardDeviation
      }
    }

    zScore
  }
}

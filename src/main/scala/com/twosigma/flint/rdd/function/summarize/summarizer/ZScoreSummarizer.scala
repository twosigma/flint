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

/**
 * Computes the z-score with the option for out-of-sample calculation. Formulas are taken from
 * "Numerically Stable, Single-Pass, Parallel Statistics Algorithms" by Janine Bennett et al.
 *
 * http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.214.8508&rep=rep1&type=pdf
 */
case class ZScoreState(
  val count: Long = 0,
  val value: Double = Double.NaN,
  val curMean: Double = Double.NaN,
  val curSSE: Double = Double.NaN,
  val prevMean: Double = Double.NaN,
  val prevSSE: Double = Double.NaN
)

case class ZScoreSummarizer(excludeCurrentObservation: Boolean) extends Summarizer[Double, ZScoreState, Double] {
  override def zero(): ZScoreState = ZScoreState()

  override def add(u: ZScoreState, value: Double): ZScoreState = {
    val count = u.count + 1

    if (count == 1) {
      val curMean = value
      val curSSE = 0.0

      ZScoreState(count, value, curMean, curSSE, Double.NaN, Double.NaN)
    } else {
      // formula II.1
      val prevMean = u.curMean
      val curMean = prevMean + (value - prevMean) / count

      // formula II.2
      val prevSSE = u.curSSE
      val curSSE = prevSSE + (value - prevMean) * (value - curMean)

      ZScoreState(count, value, curMean, curSSE, prevMean, prevSSE)
    }
  }

  /**
   * Combine two partial stats together.
   */
  private def mergeStats(thisMean: Double, thisSSE: Double, thisCount: Long,
    thatMean: Double, thatSSE: Double, thatCount: Long) = {
    val count = thisCount + thatCount

    // formula II.3
    val delta = thatMean - thisMean
    val mean = thisMean + thatCount * delta / count

    // formula II.4
    val sse = thisSSE + thatSSE + (thisCount * thatCount * Math.pow(delta, 2) / count)

    (mean, sse, count)
  }

  override def merge(u1: ZScoreState, u2: ZScoreState): ZScoreState = {
    if (u1.count == 0) {
      u2
    } else if (u2.count == 0) {
      u1
    } else {
      // the previous stats are the merger of this current and that previous
      val (prevMean, prevSSE, _) = mergeStats(u1.curMean, u1.curSSE, u1.count, u2.prevMean, u2.prevSSE, u2.count - 1)

      // the current stats are the merger of all current
      val (curMean, curSSE, count) = mergeStats(u1.curMean, u1.curSSE, u1.count, u2.curMean, u2.curSSE, u2.count)

      // use that's value since it came later
      val value = u2.value

      ZScoreState(count, value, curMean, curSSE, prevMean, prevSSE)
    }
  }

  override def render(u: ZScoreState): Double = {
    var zScore = Double.NaN

    if (u.value != Double.NaN) {
      if (excludeCurrentObservation) {
        if (u.count > 1) {
          // use unbiased sample standard deviation with current observation
          val standardDeviation = Math.sqrt(u.curSSE / (u.count - 1))
          zScore = (u.value - u.curMean) / standardDeviation
        }
      } else {
        if (u.count > 2) {
          // use unbiased sample standard deviation excluding current observation
          val standardDeviation = Math.sqrt(u.prevSSE / (u.count - 2))
          zScore = (u.value - u.prevMean) / standardDeviation
        }
      }
    }

    zScore
  }
}

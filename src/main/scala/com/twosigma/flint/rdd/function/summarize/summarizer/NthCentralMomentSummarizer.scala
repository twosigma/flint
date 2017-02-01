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
import org.apache.commons.math3.util.CombinatoricsUtils
import scala.math.pow

case class NthCentralMomentState(
  var count: Long,
  val mean: Kahan,
  val moments: Array[Kahan]
)

case class NthCentralMomentOutput(val count: Long, val moments: Array[Double]) {

  def nthCentralMoment(moment: Int): Double = moment match {
    case 1 => 0
    case _ => moments(moment - 2)
  }
}

// This summarizer uses mutable state
case class NthCentralMomentSummarizer(val moment: Int)
  extends Summarizer[Double, NthCentralMomentState, NthCentralMomentOutput] {
  require(moment > 0)
  val binomials: Array[Array[Double]] = {
    val b = Array.fill(moment - 1)(null: Array[Double])
    for (p <- 2 to moment) {
      val q = p - 2
      b(q) = Array.fill(q + 1)(0)
      for (k <- 1 to q) {
        b(q)(k - 1) = CombinatoricsUtils.binomialCoefficientDouble(p, k)
      }
    }
    b
  }

  override def zero(): NthCentralMomentState =
    NthCentralMomentState(0, Kahan(), Array.fill(moment - 1)(Kahan()))

  override def add(u: NthCentralMomentState, data: Double): NthCentralMomentState = {
    val prevMean = u.mean.getValue()
    val prevMoments = u.moments.map(_.getValue())

    u.count += 1
    if (u.count == 1) {
      u.mean.add(data)
    } else {
      u.mean.add((data - prevMean) / u.count)
    }

    if (u.count == 1) {
      u
    } else {
      val delta = data - prevMean
      val countDelta = (u.count - 1d) * delta / u.count
      var countDeltaRunningProduct = countDelta
      val oneMinusCount = 1d / (1d - u.count)
      var oneMinusCountRunningProduct = 1d
      val deltaOverCount = -delta / u.count

      for (p <- 2 to moment) {
        countDeltaRunningProduct *= countDelta
        oneMinusCountRunningProduct *= oneMinusCount

        var deltaOverCountRunningProduct = 1d
        for (k <- 1 to p - 2) {
          deltaOverCountRunningProduct *= deltaOverCount
          u.moments(p - 2).add(binomials(p - 2)(k - 1) * prevMoments(p - k - 2) * deltaOverCountRunningProduct)
        }

        u.moments(p - 2).add(countDeltaRunningProduct * (1 - oneMinusCountRunningProduct))
      }
      u
    }
  }

  override def merge(u1: NthCentralMomentState, u2: NthCentralMomentState): NthCentralMomentState = {
    if (u1.count == 0) {
      u2
    } else if (u2.count == 0) {
      u1
    } else {
      val newCount = u1.count + u2.count
      val meanDelta = u2.mean.getValue() - u1.mean.getValue()

      val prevMoments1 = u1.moments.map(_.getValue())

      u1.mean.add(u2.count * meanDelta / newCount)

      for (p <- 2 to moment) {
        combineMoments(p, newCount, meanDelta, prevMoments1, u1, u2)
      }

      u1.count = newCount
      u1
    }
  }

  // Update values in u1. u2 is unchanged
  private[this] def combineMoments(
    p: Int,
    newCount: Long,
    meanDelta: Double,
    prevMoments1: Array[Double],
    u1: NthCentralMomentState,
    u2: NthCentralMomentState
  ): Unit = {

    u1.moments(p - 2).add(u2.moments(p - 2))

    for (k <- 1 to p - 2) {
      u1.moments(p - 2).add(
        binomials(p - 2)(k - 1) *
          (pow(-u2.count.toDouble / newCount, k.toDouble) * prevMoments1(p - k - 2) +
            pow(u1.count.toDouble / newCount, k.toDouble) * u2.moments(p - k - 2).getValue()) *
            (pow(meanDelta, k.toDouble))
      )
    }

    u1.moments(p - 2).add(
      pow(meanDelta * (u1.count * u2.count) / newCount, p.toDouble) *
        (pow(u2.count.toDouble, 1d - p) - pow(-u1.count.toDouble, 1d - p))
    )
  }

  override def render(u: NthCentralMomentState): NthCentralMomentOutput = {
    val moments = u.moments.map(_.getValue() / u.count)
    NthCentralMomentOutput(u.count, moments)
  }
}

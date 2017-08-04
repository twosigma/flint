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

import com.twosigma.flint.timeseries.summarize.summarizer.ExponentialSmoothingConvention
import com.twosigma.flint.timeseries.summarize.summarizer.ExponentialSmoothingConvention.ExponentialSmoothingConvention
import com.twosigma.flint.timeseries.summarize.summarizer.ExponentialSmoothingType
import com.twosigma.flint.timeseries.summarize.summarizer.ExponentialSmoothingType.ExponentialSmoothingType

case class SmoothingRow(time: Long, x: Double)

/**
 * @param primaryESValue     Current primary smoothing series value
 * @param auxiliaryESValue   Current auxiliary smoothing series value
 * @param firstRow           The first row in the series
 * @param prevRow            The last row seen in the series
 * @param count              The count of rows
 */
case class ExponentialSmoothingState(
  var primaryESValue: Double,
  var auxiliaryESValue: Double,
  var firstRow: Option[SmoothingRow],
  var prevRow: Option[SmoothingRow],
  var count: Long
)

case class ExponentialSmoothingOutput(es: Double)

/**
 * Implemented based on
 * [[http://www.eckner.com/papers/Algorithms%20for%20Unevenly%20Spaced%20Time%20Series.pdf]]
 * The formulas can also be derived by calculating the convolution of the exponential function with the timeseries
 * given the interpolation scheme.
 *
 * In a distributed system, merging two EMA states is simply a matter of decaying the left state given the time
 * difference between the end of the left state and the end of the right state. Then, we also account for the periods
 * between the last left row and the first right row.
 *
 * @param alpha                           The proportion by which the average will decay over one period
 *                                        A period is a duration of time defined by the function provided for
 *                                        timestampsToPeriods. For instance, if the timestamps in the dataset are in
 *                                        nanoseconds, and the function provided in timestampsToPeriods is
 *                                        (t2 - t1) / nanosecondsInADay, then the summarizer will take the number of
 *                                        periods between rows to be the number of days elapsed between their
 *                                        timestamps.
 * @param primingPeriods                  Parameter used to find the initial decay parameter - taken to be the number
 *                                        of periods (defined above) elapsed before the first data point
 * @param timestampsToPeriods             Function that given two timestamps, returns how many periods should be
 *                                        considered to have passed between them
 * @param exponentialSmoothingType        Parameter used to determine the interpolation method for intervals between
 *                                        two rows
 * @param exponentialSmoothingConvention  Parameter used to determine the convolution convention.
 */
case class ExponentialSmoothingSummarizer(
  alpha: Double,
  primingPeriods: Double,
  timestampsToPeriods: (Long, Long) => Double,
  exponentialSmoothingType: ExponentialSmoothingType,
  exponentialSmoothingConvention: ExponentialSmoothingConvention
) extends FlippableSummarizer[SmoothingRow, ExponentialSmoothingState, ExponentialSmoothingOutput] {
  private val logDecayPerPeriod = math.log(1.0 - alpha)

  @inline
  private def decayForInterval(
    esValue: Double,
    periods: Double
  ): Double = {
    val timeOverTimeConstant = periods * logDecayPerPeriod
    val decay = math.exp(timeOverTimeConstant)
    decay * esValue
  }

  @inline
  private def interpolateForInterval(
    startVal: Double,
    endVal: Double,
    periods: Double
  ): Double =
    if (periods == 0.0) {
      0.0
    } else {
      val timeOverTimeConstant = periods * logDecayPerPeriod
      val decay = math.exp(timeOverTimeConstant)
      exponentialSmoothingType match {
        case ExponentialSmoothingType.PreviousPoint =>
          (1.0 - decay) * startVal
        case ExponentialSmoothingType.LinearInterpolation =>
          val interpolateDecay = (decay - 1.0) / timeOverTimeConstant
          (interpolateDecay - decay) * startVal + (1.0 - interpolateDecay) * endVal
        case ExponentialSmoothingType.CurrentPoint =>
          (1.0 - decay) * endVal
      }
    }

  override def zero(): ExponentialSmoothingState = ExponentialSmoothingState(
    primaryESValue = 0.0,
    auxiliaryESValue = 0.0,
    firstRow = None,
    prevRow = None,
    count = 0L
  )

  override def merge(
    u1: ExponentialSmoothingState,
    u2: ExponentialSmoothingState
  ): ExponentialSmoothingState = {
    if (u1.count == 0L) {
      u2
    } else if (u2.count == 0L) {
      u1
    } else {
      require(u1.prevRow.get.time <= u2.prevRow.get.time)
      val newU = zero()
      val gapPeriods = timestampsToPeriods(u1.prevRow.get.time, u2.firstRow.get.time)
      val u2Periods = timestampsToPeriods(u2.firstRow.get.time, u2.prevRow.get.time)

      // Add the first value of u2
      newU.primaryESValue = decayForInterval(u1.primaryESValue, gapPeriods) +
        interpolateForInterval(u1.prevRow.get.x, u2.firstRow.get.x, gapPeriods)
      newU.auxiliaryESValue = decayForInterval(u1.auxiliaryESValue, gapPeriods) +
        interpolateForInterval(1.0, 1.0, gapPeriods)

      // Decay over u2 period
      newU.primaryESValue = decayForInterval(newU.primaryESValue, u2Periods) + u2.primaryESValue
      newU.auxiliaryESValue = decayForInterval(newU.auxiliaryESValue, u2Periods) + u2.auxiliaryESValue

      newU.count = u2.count + u1.count
      newU.firstRow = u1.firstRow
      newU.prevRow = u2.prevRow
      newU
    }
  }

  override def render(u: ExponentialSmoothingState): ExponentialSmoothingOutput = {
    if (u.count > 0L) {
      // For legacy, we inject a point at time 0 instead of utilizing the primingPeriods parameter.
      val actualPrimingPeriods = exponentialSmoothingConvention match {
        case ExponentialSmoothingConvention.Legacy =>
          timestampsToPeriods(0L, u.firstRow.get.time)
        case _ =>
          primingPeriods
      }

      // Account for priming periods
      val primedPrimaryESValue = interpolateForInterval(0.0, u.firstRow.get.x, actualPrimingPeriods)
      val primedAuxiliaryESValue = interpolateForInterval(0.0, 1.0, actualPrimingPeriods)
      val periods = timestampsToPeriods(u.firstRow.get.time, u.prevRow.get.time) max 0
      val finalPrimaryESValue = decayForInterval(primedPrimaryESValue, periods) + u.primaryESValue
      val finalAuxiliaryESValue = decayForInterval(primedAuxiliaryESValue, periods) + u.auxiliaryESValue

      exponentialSmoothingConvention match {
        case ExponentialSmoothingConvention.Convolution =>
          ExponentialSmoothingOutput(finalPrimaryESValue)
        case ExponentialSmoothingConvention.Core =>
          ExponentialSmoothingOutput(finalPrimaryESValue / finalAuxiliaryESValue)
        case ExponentialSmoothingConvention.Legacy =>
          ExponentialSmoothingOutput(finalPrimaryESValue)
      }
    } else {
      ExponentialSmoothingOutput(Double.NaN)
    }
  }

  override def add(u: ExponentialSmoothingState, row: SmoothingRow): ExponentialSmoothingState = {
    // Don't update here, we will account for priming periods at the end
    if (u.count == 0L) {
      u.firstRow = Some(row)
    } else {
      val periods = timestampsToPeriods(u.prevRow.get.time, row.time)
      u.primaryESValue = decayForInterval(u.primaryESValue, periods) +
        interpolateForInterval(u.prevRow.get.x, row.x, periods)
      u.auxiliaryESValue = decayForInterval(u.auxiliaryESValue, periods) +
        interpolateForInterval(1.0, 1.0, periods)
    }
    u.prevRow = Some(row)
    u.count += 1L
    u
  }
}

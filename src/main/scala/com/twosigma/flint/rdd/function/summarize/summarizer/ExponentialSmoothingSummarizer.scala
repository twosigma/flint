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

import scala.collection.mutable.{ ArrayBuffer, ListBuffer }

case class SmoothingRow(time: Long, x: Double)

/**
 * @param primaryESValue     Current primary smoothing series value (up to prevTime if update has not been called)
 * @param auxiliaryESValue   Current auxiliary smoothing series value (up to prevTime if update has not been called)
 * @param firstXValue        The first data point in the series
 * @param lastXValue         The most recently seen data point in the series
 * @param time               Whenever a row is added, time is updated as well.
 * @param prevTime           The time at the data point prior to that in lastXValue
 * @param alpha              The decay value for the next update. A function of time - prevTime
 * @param count              The count of rows
 */
case class ExponentialSmoothingState(
  var primaryESValue: Double,
  var auxiliaryESValue: Double,
  var firstXValue: Option[SmoothingRow],
  var lastXValue: Option[SmoothingRow],
  var time: Long,
  var prevTime: Long,
  var alpha: Double,
  var count: Long
)

case class ExponentialSmoothingOutput(es: Double)

/**
 * @param decayPerPeriod       The proportion by which the average will decay over one period
 *                             A period is a duration of time defined by the function provided for timestampsToPeriods.
 *                             For instance, if the timestamps in the dataset are in nanoseconds, and the function
 *                             provided in timestampsToPeriods is (t2 - t1)/nanosecondsInADay, then the summarizer will
 *                             take the number of periods between rows to be the number of days elapsed between their
 *                             timestamps.
 * @param primingPeriods       Parameter used to find the initial decay parameter - taken to be the time elapsed
 *                             before the first data point
 * @param timestampsToPeriods  Function that given two timestamps, returns how many periods should be considered to
 *                             have passed between them
 */
class ExponentialSmoothingSummarizer(
  decayPerPeriod: Double,
  primingPeriods: Double,
  timestampsToPeriods: (Long, Long) => Double
) extends Summarizer[SmoothingRow, ExponentialSmoothingState, ExponentialSmoothingOutput] {

  private def getAlpha(periods: Double): Double = math.exp(periods * math.log(1.0 - decayPerPeriod))

  override def zero(): ExponentialSmoothingState = ExponentialSmoothingState(
    primaryESValue = 0.0,
    auxiliaryESValue = 0.0,
    firstXValue = None,
    lastXValue = None,
    time = 0L,
    prevTime = 0L,
    alpha = 0.0,
    count = 0L
  )

  override def merge(
    state1: ExponentialSmoothingState,
    state2: ExponentialSmoothingState
  ): ExponentialSmoothingState = {
    require(state1.time < state2.time || state1.count == 0 || state2.count == 0)
    if (state1.count == 0) {
      return state2
    }

    if (state2.count == 0) {
      return state1
    }

    if (state2.count == 1) {
      require(state2.alpha == 0 && state2.prevTime == 0)
      add(state1, state2.firstXValue.get)
    } else {
      val merged = zero()
      val updatedState1 = update(state1)
      val gapPeriods = timestampsToPeriods(updatedState1.time, state2.prevTime)
      val gapAlpha = getAlpha(gapPeriods)
      merged.primaryESValue = state2.primaryESValue + gapAlpha * updatedState1.primaryESValue
      merged.auxiliaryESValue = state2.auxiliaryESValue + gapAlpha * updatedState1.auxiliaryESValue
      merged.prevTime = state2.prevTime
      merged.alpha = state2.alpha
      merged.firstXValue = state1.firstXValue
      merged.lastXValue = state2.lastXValue
      merged.time = state2.time
      merged.count = state1.count + state2.count
      merged
    }
  }

  override def render(u: ExponentialSmoothingState): ExponentialSmoothingOutput = {
    var _u = u.copy()
    if (_u.count > 0) {
      // Account for priming periods
      val periods = timestampsToPeriods(_u.firstXValue.get.time, _u.prevTime) max 0
      val primingAlpha = getAlpha(primingPeriods)
      val tuningAlpha = getAlpha(periods)
      _u.primaryESValue = _u.primaryESValue + tuningAlpha * (1.0 - primingAlpha) * _u.firstXValue.get.x
      _u.auxiliaryESValue = _u.auxiliaryESValue + tuningAlpha * (1.0 - primingAlpha) * 1.0
      _u = update(_u)
      ExponentialSmoothingOutput(_u.primaryESValue / _u.auxiliaryESValue)
    } else {
      ExponentialSmoothingOutput(Double.NaN)
    }
  }

  def update(u: ExponentialSmoothingState): ExponentialSmoothingState = {
    u.primaryESValue = u.alpha * u.primaryESValue + (1.0 - u.alpha) * u.lastXValue.get.x
    u.auxiliaryESValue = u.alpha * u.auxiliaryESValue + (1.0 - u.alpha) * 1.0
    u
  }

  override def add(u: ExponentialSmoothingState, row: SmoothingRow): ExponentialSmoothingState = {
    var newU = u.copy()
    if (newU.count == 0) {
      newU.firstXValue = Some(row)
    } else if (newU.count == 1) {
      // Don't update here, we will account for priming periods at the end
      newU.alpha = getAlpha(timestampsToPeriods(newU.time, row.time))
    } else {
      newU = update(newU)
      newU.alpha = getAlpha(timestampsToPeriods(newU.time, row.time))
    }
    newU.lastXValue = Some(row)
    newU.prevTime = newU.time
    newU.time = row.time
    newU.count += 1
    newU
  }
}

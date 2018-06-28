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

package com.twosigma.flint.rdd.function.summarize.summarizer.subtractable

import com.twosigma.flint.timeseries.summarize.summarizer.ExponentialWeightedMovingAverageConvention

case class EWMARow(time: Long, x: Double)

/**
 * @param primaryESValue     Current primary smoothing series value (up to prevTime if update has not been called)
 * @param auxiliaryESValue   Current auxiliary smoothing series value (up to prevTime if update has not been called)
 * @param count              The count of rows
 */
case class ExponentialWeightedMovingAverageState(
  var primaryESValue: Double,
  var auxiliaryESValue: Double,
  var time: Long,
  var count: Long
)

case class ExponentialWeightedMovingAverageOutput(ewma: Double)

/**
 * @param alpha               The proportion by which the average will decay over one period
 *                            A period is a duration of time defined by the function provided for timestampsToPeriods.
 *                            For instance, if the timestamps in the dataset are in nanoseconds, and the function
 *                            provided in timestampsToPeriods is (t2 - t1)/nanosecondsInADay, then the summarizer will
 *                            take the number of periods between rows to be the number of days elapsed between their
 *                            timestamps.
 * @param timestampsToPeriods Function that given two timestamps, returns how many periods should be considered to
 *                            have passed between them
 * @param constantPeriods     Whether to assume that the number of periods between rows is a constant (c = 1), or use
 *                            timestampsToPeriods to calculate it.
 * @param convention          Parameter used to determine the convention. If it is "core", the result
 *                            primary exponential weighted moving average will be further divided by its auxiliary;
 *                            if it is "legacy", it will return the primary exponential weighted moving average.
 */
class ExponentialWeightedMovingAverageSummarizer(
  alpha: Double,
  timestampsToPeriods: (Long, Long) => Double,
  constantPeriods: Boolean,
  convention: ExponentialWeightedMovingAverageConvention.Value
) extends LeftSubtractableSummarizer[EWMARow, ExponentialWeightedMovingAverageState, ExponentialWeightedMovingAverageOutput] {
  private val logDecayPerPeriod = math.log(1.0 - alpha)

  def getDecay(periods: Double): Double = math.exp(periods * logDecayPerPeriod)

  override def zero(): ExponentialWeightedMovingAverageState =
    ExponentialWeightedMovingAverageState(
      primaryESValue = 0.0,
      auxiliaryESValue = 0.0,
      time = 0L,
      count = 0L
    )

  override def merge(
    u1: ExponentialWeightedMovingAverageState,
    u2: ExponentialWeightedMovingAverageState
  ): ExponentialWeightedMovingAverageState = {
    require(u1.time < u2.time || u1.count == 0L || u2.count == 0L)
    if (u1.count == 0L) {
      u2
    } else if (u2.count == 0L) {
      u1
    } else {
      val newU = zero()
      val periods = if (constantPeriods) {
        u2.count.toDouble
      } else {
        timestampsToPeriods(u1.time, u2.time)
      }
      val decay = getDecay(periods)
      newU.primaryESValue = decay * u1.primaryESValue + u2.primaryESValue
      newU.auxiliaryESValue = decay * u1.auxiliaryESValue + u2.auxiliaryESValue

      newU.time = u2.time
      newU.count = u2.count + u1.count
      newU
    }
  }

  override def render(u: ExponentialWeightedMovingAverageState): ExponentialWeightedMovingAverageOutput = {
    if (u.count > 0L) {
      convention match {
        case ExponentialWeightedMovingAverageConvention.Core =>
          ExponentialWeightedMovingAverageOutput(
            u.primaryESValue / u.auxiliaryESValue
          )
        case ExponentialWeightedMovingAverageConvention.Legacy =>
          ExponentialWeightedMovingAverageOutput(u.primaryESValue)
        case _ =>
          throw new IllegalArgumentException(s"Not supported convention $convention")
      }
    } else {
      ExponentialWeightedMovingAverageOutput(Double.NaN)
    }
  }

  override def add(
    u: ExponentialWeightedMovingAverageState,
    row: EWMARow
  ): ExponentialWeightedMovingAverageState = {
    if (u.count == 0L) {
      u.primaryESValue = row.x
      u.auxiliaryESValue = 1.0
    } else {
      val periods = if (constantPeriods) {
        1.0
      } else {
        timestampsToPeriods(u.time, row.time)
      }
      val decay = getDecay(periods)
      u.primaryESValue = decay * u.primaryESValue + row.x
      u.auxiliaryESValue = decay * u.auxiliaryESValue + 1.0
    }
    u.time = row.time
    u.count += 1L

    u
  }

  override def subtract(
    u: ExponentialWeightedMovingAverageState,
    row: EWMARow
  ): ExponentialWeightedMovingAverageState = {
    require(u.count > 0L)
    if (u.count == 1L) {
      zero()
    } else {
      val periods = if (constantPeriods) {
        u.count - 1.0
      } else {
        timestampsToPeriods(row.time, u.time)
      }
      val decay = getDecay(periods)
      u.primaryESValue -= decay * row.x
      u.auxiliaryESValue -= decay
      u.count -= 1L

      u
    }
  }
}

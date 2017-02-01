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

import scala.collection.mutable.{ ArrayBuffer, ListBuffer }

case class SmoothingRow(time: Long, x: Double)

/**
 * @param primaryESValues    A list of all values in the primary series
 * @param auxiliaryESValues  A list of all values in the auxiliary series
 * @param alphas             A list of all decay parameters used
 * @param firstXValue        The first data point in the series
 * @param time               Whenever a row is added, time is updated as well.
 * @param count              The count of rows
 */
case class ExponentialSmoothingState(
  var primaryESValues: ListBuffer[Double],
  var auxiliaryESValues: ListBuffer[Double],
  var alphas: ListBuffer[Double],
  var firstXValue: Option[SmoothingRow],
  var time: Long,
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
    primaryESValues = ListBuffer.empty,
    auxiliaryESValues = ListBuffer.empty,
    alphas = ListBuffer.empty,
    firstXValue = None,
    time = 0L,
    count = 0L
  )

  // TODO when we perform the merge on the driver side, all the data is aggregated onto the driver which may be
  // inefficient. In the future, we may look to implement this as a two-passes summarizer
  override def merge(
    state1: ExponentialSmoothingState,
    state2: ExponentialSmoothingState
  ): ExponentialSmoothingState = {
    require(state1.time <= state2.time)

    val (primaryESDeltas, auxiliaryESDeltas) = if (state1.count > 0) {
      (
        state2.alphas.scanLeft(state1.primaryESValues.last)(_ * _).tail,
        state2.alphas.scanLeft(state1.auxiliaryESValues.last)(_ * _).tail
      )
    } else {
      (
        List.fill(state2.count.toInt)(0.0),
        List.fill(state2.count.toInt)(0.0)
      )
    }
    require(primaryESDeltas.length == state2.primaryESValues.length)
    require(auxiliaryESDeltas.length == state2.auxiliaryESValues.length)

    state2.primaryESValues = state2.primaryESValues.zip(primaryESDeltas).map { v => v._1 + v._2 }
    state2.auxiliaryESValues = state2.auxiliaryESValues.zip(auxiliaryESDeltas).map { v => v._1 + v._2 }

    val (timePrimaryESDeltas, timeAuxiliaryESDeltas) = if (state1.count > 0 && state2.count > 0) {
      val alphaDelta = getAlpha(timestampsToPeriods(state1.time, state2.firstXValue.get.time)) - state2.alphas.head
      (
        state2.alphas.tail.scanLeft(alphaDelta * (state1.primaryESValues.last - state2.firstXValue.get.x))(_ * _),
        state2.alphas.tail.scanLeft(alphaDelta * (state1.auxiliaryESValues.last - 1.0))(_ * _)
      )
    } else {
      (
        List.fill(state2.count.toInt)(0.0),
        List.fill(state2.count.toInt)(0.0)
      )
    }
    require(timePrimaryESDeltas.length == state2.primaryESValues.length)
    require(timeAuxiliaryESDeltas.length == state2.auxiliaryESValues.length)

    state2.primaryESValues = state2.primaryESValues.zip(timePrimaryESDeltas).map { v => v._1 + v._2 }
    state2.auxiliaryESValues = state2.auxiliaryESValues.zip(timeAuxiliaryESDeltas).map { v => v._1 + v._2 }

    val merged = zero()
    val (primaryESValues, auxiliaryESValues, alphas, count, firstXValue) = (state1.count, state2.count) match {
      case (0, 0) => (
        ListBuffer[Double](),
        ListBuffer[Double](),
        ListBuffer[Double](),
        0L,
        None
      )
      case (_, 0) => (
        state1.primaryESValues.takeRight(1),
        state1.auxiliaryESValues.takeRight(1),
        state1.alphas.takeRight(1),
        1L,
        state1.firstXValue
      )
      case (_, _) => (
        state2.primaryESValues.takeRight(1),
        state2.auxiliaryESValues.takeRight(1),
        state2.alphas.takeRight(1),
        1L,
        state2.firstXValue
      )
    }
    merged.primaryESValues = primaryESValues
    merged.auxiliaryESValues = auxiliaryESValues
    merged.alphas = alphas
    merged.count = count
    merged.time = state1.time max state2.time
    merged.firstXValue = firstXValue

    merged
  }

  override def render(u: ExponentialSmoothingState): ExponentialSmoothingOutput = {
    if (u.count > 0) {
      ExponentialSmoothingOutput(u.primaryESValues.last / u.auxiliaryESValues.last)
    } else {
      ExponentialSmoothingOutput(Double.NaN)
    }
  }

  override def add(u: ExponentialSmoothingState, row: SmoothingRow): ExponentialSmoothingState = {
    val (alpha, prevPrimaryES, prevAuxiliaryES) = if (u.count == 0) {
      (getAlpha(primingPeriods), 0.0, 0.0)
    } else {
      (getAlpha(timestampsToPeriods(u.time, row.time)), u.primaryESValues.last, u.auxiliaryESValues.last)
    }
    u.alphas.append(alpha)
    u.primaryESValues.append(alpha * prevPrimaryES + (1 - alpha) * row.x)
    u.auxiliaryESValues.append(alpha * prevAuxiliaryES + (1 - alpha) * 1.0)
    if (u.firstXValue.isEmpty) {
      u.firstXValue = Some(row)
    }
    u.time = row.time
    u.count += 1
    u
  }
}

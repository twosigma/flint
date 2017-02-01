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

package com.twosigma.flint.timeseries.window

import com.twosigma.flint.rdd
import com.twosigma.flint.rdd.CountWindow

trait Window extends Serializable {
  // Name of the window. New window column is named s"window_${name}"
  val name: String
}

trait TimeWindow extends Window with rdd.Window[Long]

/**
 * A [[TimeWindow]] that supports `shift`.
 *
 * A [[ShiftTimeWindow]] is associated with a direction which could be forward or backward.
 * This decides the direction of time shifting of the window.
 *
 * A [[ShiftTimeWindow]] must also be monotonic:
 *  - if backward, window.of(t1)._1 >= window.of(t2)._1 must be true for all t1 >= t2
 *  - if not backward, window.of(t1)._2 >= window.of(t2)._2 must be true for all t1 >= t2
 *
 */
trait ShiftTimeWindow extends TimeWindow {
  protected val backward: Boolean
  /**
   * Given a time, return the length of the window
   */
  protected def length(time: Long): Long
  final protected def of(time: Long, length: Long): (Long, Long) =
    if (backward) (time - length, time) else (time, time + length)

  final override def of(time: Long): (Long, Long) = of(time, length(time))
  final def shift(time: Long): Long = if (backward) of(time, length(time))._1 else of(time, length(time))._2
}

trait RowCountWindow extends Window with CountWindow

case class AbsoluteTimeWindow(override val name: String, val length: Long, override val backward: Boolean = true)
  extends ShiftTimeWindow {
  def length(t: Long): Long = length
}

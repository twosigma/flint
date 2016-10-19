/*
 *  Copyright 2015-2016 TWO SIGMA OPEN SOURCE, LLC
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

package com.twosigma.flint.timeseries

import com.twosigma.flint.rdd
import com.twosigma.flint.rdd.CountWindow

import scala.concurrent.duration._

trait Window extends Serializable {
  // Name of the window. New window column is named s"window_${name}"
  val name: String
}

trait TimeWindow extends Window with rdd.Window[Long]

trait RowCountWindow extends Window with CountWindow

case class AbsoluteTimeWindow(override val name: String, val length: Long, val past: Boolean = true)
  extends TimeWindow {
  override def of(t: Long): (Long, Long) = if (past) (t - length, t) else (t, t + length)
}

object Window {

  /**
   * Provide an [[AbsoluteTimeWindow]] for a given length.
   *
   * @param length A string representation of window length, like `10s` or `10sec` etc. The
   * the unit could be one of the follows: `d day`, `h hour`, `min minute`, `s sec second`,
   * `ms milli millisecond`, `µs micro microsecond`, and `ns nano nanosecond`.
   * @return an [[AbsoluteTimeWindow]] for the given window length.
   */
  def pastAbsoluteTime(length: String): TimeWindow = {
    val ns = Duration(length).toNanos
    require(ns > 0)
    new AbsoluteTimeWindow(s"past_$length", ns, true)
  }

  def futureAbsoluteTime(length: String): TimeWindow = {
    val ns = Duration(length).toNanos
    require(ns > 0)
    new AbsoluteTimeWindow(s"future_$length", ns, false)
  }

  def row(count: Int): RowCountWindow = throw new IllegalStateException("To be implemented.")
}

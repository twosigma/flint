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

package com.twosigma.flint.timeseries

import com.twosigma.flint.timeseries.window.{ ShiftTimeWindow, AbsoluteTimeWindow }
import scala.concurrent.duration.Duration

object Windows {

  /**
   * Provide a past [[AbsoluteTimeWindow]] for a given length.
   *
   * @param length A string representation of window length, like `10s` or `10sec` etc. The
   * the unit could be one of the follows: `d day`, `h hour`, `min minute`, `s sec second`,
   * `ms milli millisecond`, `µs micro microsecond`, and `ns nano nanosecond`.
   * @return a past [[AbsoluteTimeWindow]] for the given window length.
   */
  def pastAbsoluteTime(length: String): ShiftTimeWindow = {
    val ns = Duration(length).toNanos
    require(ns > 0)
    new AbsoluteTimeWindow(s"past_$length", ns, true)
  }

  /**
   * Provide a future [[AbsoluteTimeWindow]] for a given length.
   *
   * @param length A string representation of window length, like `10s` or `10sec` etc. The
   * the unit could be one of the follows: `d day`, `h hour`, `min minute`, `s sec second`,
   * `ms milli millisecond`, `µs micro microsecond`, and `ns nano nanosecond`.
   * @return a future [[AbsoluteTimeWindow]] for the given window length.
   */
  def futureAbsoluteTime(length: String): ShiftTimeWindow = {
    val ns = Duration(length).toNanos
    require(ns > 0)
    new AbsoluteTimeWindow(s"future_$length", ns, false)
  }
}

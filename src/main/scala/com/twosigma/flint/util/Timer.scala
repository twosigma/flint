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

package com.twosigma.flint.util

object Timer {

  /**
   * Measure the average running time in milliseconds to execute a piece of codes.
   *
   * @param repeat The number of times expected to run the given piece of codes
   * @param block  The piece of codes expected to measure the running time
   * @return the average running time and the return value of the last execution.
   */
  def time[T](repeat: Int)(block: => T): (Long, T) = {
    val t1 = System.currentTimeMillis()
    var ret = block
    var i = 0
    while (i < repeat - 1) {
      ret = block
      i += 1
    }
    val elapsed = System.currentTimeMillis() - t1
    (elapsed / repeat, ret)
  }
}

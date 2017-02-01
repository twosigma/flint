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

package com.twosigma.flint.rdd.function.summarize.summarizer.regression

trait LagWindow {

  val maxSize: Int = Int.MaxValue

  def shouldKeep(currentTimestamp: Long, oldestTimestamp: Long, size: Int): Boolean
}

case class AbsoluteTimeLagWindow(duration: Long) extends LagWindow {
  override def shouldKeep(currentTimestamp: Long, oldestTimestamp: Long, size: Int): Boolean =
    currentTimestamp - oldestTimestamp < duration
}

case class CountLagWindow(count: Int, maxLookBack: Long) extends LagWindow {
  require(count >= 0, "The max size of `CountLagWindow` must be > 0")

  // `count = 0` means don't keep any *old* windows, but we do need room for the current one
  override val maxSize = count + 1

  override def shouldKeep(currentTimestamp: Long, oldestTimestamp: Long, size: Int): Boolean =
    currentTimestamp - oldestTimestamp <= maxLookBack && size <= maxSize
}

object LagWindow {

  def absolute(duration: Long): LagWindow = AbsoluteTimeLagWindow(duration)

  def count(count: Int, maxLookBack: Long = Long.MaxValue): LagWindow = CountLagWindow(count, maxLookBack)
}

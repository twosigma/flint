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

case class NthMomentState(var count: Long, val nthMoment: Kahan)

// This summarizer uses mutable state
case class NthMomentSummarizer(moment: Int) extends Summarizer[Double, NthMomentState, Double] {
  require(moment >= 0)
  override def zero(): NthMomentState = NthMomentState(0, Kahan())

  override def add(u: NthMomentState, t: Double): NthMomentState = {
    val newCount = u.count + 1
    val curMoment = u.nthMoment
    val data = scala.math.pow(t, moment.toDouble)
    if (newCount == 1) {
      curMoment.add(data)
    } else {
      val delta = data - curMoment.getValue()
      curMoment.add(delta / newCount)
    }

    u.count = newCount

    u
  }

  override def merge(u1: NthMomentState, u2: NthMomentState): NthMomentState = {
    if (u1.count == 0) {
      u2
    } else if (u2.count == 0) {
      u1
    } else {

      val newCount = u1.count + u2.count
      val delta = u2.nthMoment.subtract(u1.nthMoment)

      u1.nthMoment.add(u2.count * delta / newCount)
      u1.count = newCount

      u1
    }
  }

  override def render(u: NthMomentState): Double = u.nthMoment.getValue()
}

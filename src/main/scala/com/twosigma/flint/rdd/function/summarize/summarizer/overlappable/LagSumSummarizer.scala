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

package com.twosigma.flint.rdd.function.summarize.summarizer.overlappable

case class LagSumSummarizerState(
  lagSum: Double,
  sum: Double
)

class LagSumSummarizer
  extends OverlappableSummarizer[Double, LagSumSummarizerState, LagSumSummarizerState] {

  def zero(): LagSumSummarizerState = LagSumSummarizerState(0.0, 0.0)

  def add(u: LagSumSummarizerState, t: Double): LagSumSummarizerState = {
    LagSumSummarizerState(u.lagSum, u.sum + t)
  }

  def addOverlapped(u: LagSumSummarizerState, t: (Double, Boolean)): LagSumSummarizerState = {
    val (value, isOverlapped) = t
    if (isOverlapped) {
      LagSumSummarizerState(u.lagSum + value, u.sum)
    } else {
      LagSumSummarizerState(u.lagSum, u.sum + value)
    }
  }

  def merge(u1: LagSumSummarizerState, u2: LagSumSummarizerState): LagSumSummarizerState =
    LagSumSummarizerState(u1.lagSum + u2.lagSum, u1.sum + u2.sum)

  def render(u: LagSumSummarizerState): LagSumSummarizerState = u

}

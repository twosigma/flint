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

package com.twosigma.flint.rdd.function.summarize.summarizer

case class WeightedCorrelationState(
  var sx: WeightedCovarianceState,
  var sy: WeightedCovarianceState,
  var sxy: WeightedCovarianceState
)

case class WeightedCorrelationOutput(
  correlation: Double,
  count: Long
)

class WeightedCorrelationSummarizer
  extends FlippableSummarizer[(Double, Double, Double), WeightedCorrelationState, WeightedCorrelationOutput] {
  private[this] val weightedCovarianceSummarizer =
    new WeightedCovarianceSummarizer()

  override def zero(): WeightedCorrelationState = WeightedCorrelationState(
    weightedCovarianceSummarizer.zero(),
    weightedCovarianceSummarizer.zero(),
    weightedCovarianceSummarizer.zero()
  )

  override def add(
    u: WeightedCorrelationState,
    t: (Double, Double, Double)
  ): WeightedCorrelationState = {
    val (x, y, w) = t
    u.sx = weightedCovarianceSummarizer.add(u.sx, (x, x, w))
    u.sy = weightedCovarianceSummarizer.add(u.sy, (y, y, w))
    u.sxy = weightedCovarianceSummarizer.add(u.sxy, t)

    u
  }

  override def merge(
    u1: WeightedCorrelationState,
    u2: WeightedCorrelationState
  ): WeightedCorrelationState = {

    u1.sx = weightedCovarianceSummarizer.merge(u1.sx, u2.sx)
    u1.sy = weightedCovarianceSummarizer.merge(u1.sy, u2.sy)
    u1.sxy = weightedCovarianceSummarizer.merge(u1.sxy, u2.sxy)

    u1
  }

  override def render(u: WeightedCorrelationState): WeightedCorrelationOutput =
    WeightedCorrelationOutput(
      u.sxy.coMoment / Math.sqrt(u.sx.coMoment * u.sy.coMoment),
      u.sx.count
    )
}

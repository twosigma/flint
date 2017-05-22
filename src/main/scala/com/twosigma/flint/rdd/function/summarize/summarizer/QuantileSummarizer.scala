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

package com.twosigma.flint.rdd.function.summarize.summarizer

import scala.collection.mutable
import org.apache.commons.math3.stat.descriptive.rank.Percentile

/**
 * The state of [[QuantileSummarizer]].
 *
 * The reason why we have both `buffer` and `values` here is because Array is much compact than ArrayBuffer in memory.
 * The contract here is that only one of them could be non empty.
 *
 * @param buffer The buffer expected to use when `add`.
 * @param values The values expected to use when `merge` and `render`.
 */
case class QuantileState[T](
  var buffer: mutable.ArrayBuffer[T],
  var values: Array[T]
)

/**
 * Return a list of quantiles for a given list of quantile probabilities.
 *
 * @note The implementation of this summarizer is not quite in a streaming and parallel fashion as there
 *       is no way to compute exact quantile using one-pass streaming algorithm. When this summarizer is
 *       used in summarize() API, it will collect all the data under the `column` to the driver and thus may not
 *       be that efficient in the sense of IO and memory intensive. However, it should be fine to use
 *       it in the other summarize APIs like summarizeWindows(), summarizeIntervals(), summarizeCycles() etc.
 * @param p The list of quantile probabilities. The probabilities must be great than 0.0 and less than or equal
 *          to 1.0.
 */
case class QuantileSummarizer(
  p: Array[Double]
) extends Summarizer[Double, QuantileState[Double], Array[Double]] {

  require(p.nonEmpty, "The list of quantiles must be non-empty.")

  override def zero(): QuantileState[Double] = QuantileState[Double](
    buffer = new mutable.ArrayBuffer[Double](0),
    values = new Array[Double](0)
  )

  override def merge(u1: QuantileState[Double], u2: QuantileState[Double]): QuantileState[Double] = {
    require(u1.buffer.isEmpty || u1.values.isEmpty)
    require(u2.buffer.isEmpty || u2.values.isEmpty)
    QuantileState(
      buffer = new mutable.ArrayBuffer[Double](0),
      values = toValues(u1).values ++ toValues(u2).values
    )
  }

  private def toValues(u: QuantileState[Double]): QuantileState[Double] =
    if (u.buffer.isEmpty) {
      u
    } else {
      u.values = u.buffer.toArray
      u.buffer.clear()
      u
    }

  override def render(u: QuantileState[Double]): Array[Double] = {
    require(u.buffer.isEmpty || u.values.isEmpty)
    // Using R-7 to be consistent with Pandas. See https://en.wikipedia.org/wiki/Quantile
    val percentileEstimator =
      new Percentile().withEstimationType(Percentile.EstimationType.R_7)
    percentileEstimator.setData(toValues(u).values)
    // Convert scale from (0.0, 1.0] to (0.0, 100.0]
    p.map { x =>
      percentileEstimator.evaluate(x * 100.0)
    }
  }

  override def add(
    u: QuantileState[Double],
    t: Double
  ): QuantileState[Double] = {
    u.buffer += t
    u
  }
}

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

import scala.collection.mutable.ArrayBuffer
import org.apache.commons.math3.stat.descriptive.rank.Percentile

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
case class QuantileSummarizer(p: Array[Double]) extends Summarizer[Double, ArrayBuffer[Double], Array[Double]] {

  require(p.nonEmpty, "The list of quantiles must be non-empty.")

  override def zero(): ArrayBuffer[Double] = new ArrayBuffer[Double](1024)

  override def merge(u1: ArrayBuffer[Double], u2: ArrayBuffer[Double]): ArrayBuffer[Double] = u1 ++ u2

  override def render(u: ArrayBuffer[Double]): Array[Double] = {
    val percentileEstimator = new Percentile()
    percentileEstimator.setData(u.toArray)
    // Convert scale from (0.0, 1.0] to (0.0, 100.0]
    p.map { x => percentileEstimator.evaluate(x * 100.0) }
  }

  override def add(u: ArrayBuffer[Double], t: Double): ArrayBuffer[Double] = u += t
}

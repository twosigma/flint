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

package com.twosigma.flint.rdd.function.summarize.summarizer.subtractable

import breeze.linalg.max
import org.apache.commons.math3.stat.descriptive.rank.Percentile

import scala.reflect.ClassTag

/**
 * A resizing array queue that does not utilize wraparound. Instead, when begin is halfway through the array, the queue
 * shifts all elements back to the beginning of the underlying array. This is not thread-safe.
 */
protected[flint] class SequentialArrayQueue[@specialized(Double) T: ClassTag] extends Serializable {
  private var begin: Int = 0
  private var end: Int = 0
  private var values: Array[T] = new Array[T](32)

  def add(value: T): Unit = {
    if (end == values.length) {
      doubleCapacity()
    }
    values(end) = value
    end += 1
  }

  def remove(): Unit = {
    require(size > 0)
    begin += 1
    if (begin >= (values.length >> 1)) {
      shift()
    }
  }

  def size(): Int = end - begin

  private def doubleCapacity(): Unit = {
    val newCapacity = values.length << 1
    require(newCapacity > 0)
    val newValues = new Array[T](newCapacity)
    System.arraycopy(values, begin, newValues, 0, size)
    end = size
    begin = 0
    values = newValues
  }

  private def shift(): Unit = {
    System.arraycopy(values, begin, values, 0, size)
    end = size
    begin = 0
  }

  def addAll(other: SequentialArrayQueue[T]): Unit = {
    val newSize = size + other.size
    require(newSize >= 0)

    // Find the next power of two necessary to hold all the values
    val (otherBegin, _, otherValues) = other.view()
    val currCapacity = max(values.length, otherValues.length)
    val newCapacity = if (newSize < currCapacity) {
      currCapacity
    } else {
      currCapacity << 1
    }
    require(newCapacity > 0)
    val newValues = new Array[T](newCapacity)
    System.arraycopy(values, begin, newValues, 0, size)
    System.arraycopy(otherValues, otherBegin, newValues, size, other.size)
    end = newSize
    begin = 0
    values = newValues
  }

  def view(): (Int, Int, Array[T]) = (begin, end, values)
}

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
) extends LeftSubtractableSummarizer[Double, SequentialArrayQueue[Double], Array[Double]] {

  require(p.nonEmpty, "The list of quantiles must be non-empty.")

  override def zero(): SequentialArrayQueue[Double] = new SequentialArrayQueue[Double]()

  override def merge(
    u1: SequentialArrayQueue[Double],
    u2: SequentialArrayQueue[Double]
  ): SequentialArrayQueue[Double] = {
    u1.addAll(u2)
    u1
  }

  override def render(u: SequentialArrayQueue[Double]): Array[Double] = {
    // Using R-7 to be consistent with Pandas. See https://en.wikipedia.org/wiki/Quantile
    val percentileEstimator =
      new Percentile().withEstimationType(Percentile.EstimationType.R_7)
    val (begin, end, values) = u.view()
    percentileEstimator.setData(values, begin, u.size)
    // Convert scale from (0.0, 1.0] to (0.0, 100.0]
    p.map { x =>
      percentileEstimator.evaluate(x * 100.0)
    }
  }

  override def add(u: SequentialArrayQueue[Double], t: Double): SequentialArrayQueue[Double] = {
    u.add(t)
    u
  }

  override def subtract(u: SequentialArrayQueue[Double], t: Double): SequentialArrayQueue[Double] = {
    u.remove()
    u
  }
}

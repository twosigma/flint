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

package com.twosigma.flint.rdd.function.summarize

import com.twosigma.flint.rdd.OverlappedOrderedRDD
import com.twosigma.flint.rdd.function.summarize.summarizer.subtractable.LeftSubtractableSummarizer
import org.apache.spark.{ OneToOneDependency, Logging }
import com.twosigma.flint.rdd.{ OrderedRDD, Range, CloseClose }
import scala.reflect.ClassTag
import scala.collection.mutable
import com.twosigma.flint.rdd.function.summarize.summarizer.Summarizer

object SummarizeWindows {

  def apply[K: Ordering: ClassTag, SK, V: ClassTag, U, V2](
    rdd: OrderedRDD[K, V],
    window: K => (K, K),
    summarizer: Summarizer[V, U, V2],
    skFn: V => SK
  ): OrderedRDD[K, (V, V2)] = {
    val overlappedRdd = OverlappedOrderedRDD(rdd, window)
    val splits = overlappedRdd.rangeSplits
    overlappedRdd.mapPartitionsWithIndexOverlapped(
      (partitionIndex, iterator) =>
        new WindowSummarizerIterator(iterator.buffered, splits(partitionIndex).range, window, summarizer, skFn)
    ).nonOverlapped()
  }
}

/**
 * We use the following example to illustrate how our algorithm works. Assuming we have
 * an input RDD with 3 partitions:<br>
 * [1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]<br>
 * where a "[]" represents a partition, the numbers in a "[]" represent keys of rows and we are processing a "core"
 * partition, i.e. [5, 6, 7, 8]. The expected output of this "core" partition is:<br>
 * [(5, [4, 5, 6]) (6, [5, 6, 7]) (7, [6, 7, 8]) (8, [7, 8, 9])]<br>
 *
 * We define the partition of [5, 6, 7, 8] as a core partition, and the left of a tuple in the output is a core row.
 * The algorithm works as follows.
 *
 * We keep two pointers:
 * - one points to the next core row to output,
 * - one points to the next row in parent iterator.
 *
 * If the parent iterator pointer passes the core row pointer, i.e. when the window of the core row
 * extends to the future, we will put the row in core row buffer. When all core rows has been output,
 * the iterator ends.
 *
 * @note
 * This algorithm currently assumes windowFn(k)._1 <= k <= windowFn(k)._2, it should not be too
 * hard to extend it to support the other cases, but it is more complicated, we might need to keep
 * one extra pointer to the next row to add to the window.
 *
 * Imagine the output is: [(5, [3, 4]) (6, [4, 5]) (7, [5, 6]) (8, [6, 7])] and to output
 * (6, [4, 5]), we need to remember we have processed 4 the next row to add to the window is 5.
 */
private[rdd] class WindowSummarizerIterator[K, SK, V, U, V2](
  iter: BufferedIterator[(K, V)],
  coreRange: Range[K],
  windowFn: K => (K, K),
  summarizer: Summarizer[V, U, V2],
  skFn: V => SK
)(implicit ord: Ordering[K]) extends Iterator[(K, (V, V2))] with Logging {

  val windows = mutable.Map[SK, Vector[(K, V)]]()
  val summarizerStates = mutable.Map[SK, U]()
  val coreRowBuffer = mutable.Queue[(K, V)]()

  def getWindowRange(k: K): Range[K] = {
    val window = windowFn(k)
    CloseClose(window._1, Some(window._2))
  }

  lazy val rampUp = {
    val initWindowRange = getWindowRange(coreRange.begin)
    logDebug(s"Initial window range in rampUp: $initWindowRange")
    if (iter.hasNext) {
      logDebug(s"rampUp: head: ${iter.head}")
    }
    while (iter.hasNext && ord.lt(iter.head._1, coreRange.begin)) {
      val (k, v) = iter.next
      val sk = skFn(v)
      logDebug(s"rampUp: reading: ($k, $sk, $v)")

      if (initWindowRange.contains(k)) {
        val window = windows.getOrElseUpdate(sk, Vector.empty)
        val u = summarizerStates.getOrElseUpdate(sk, summarizer.zero())
        windows.put(sk, window :+ (k, v))
        summarizerStates.put(sk, summarizer.add(u, v))
      }
    }
  }

  override def hasNext: Boolean = coreRowBuffer.nonEmpty || (iter.hasNext && coreRange.endGteq(iter.head._1))

  override def next(): (K, (V, V2)) = {
    // Only invoke once.
    rampUp
    val (coreK, coreV) = coreRowBuffer.headOption.getOrElse(iter.head)
    val coreSk = skFn(coreV)
    val windowRange = getWindowRange(coreK)
    logDebug(s"Invoking next() with core row: ($coreK, $coreSk, $coreV) and the window of $coreK: $windowRange")

    // Drop rows.
    val window = windows.getOrElse(coreSk, Vector.empty)
    val priorState = summarizerStates.getOrElse(coreSk, summarizer.zero())
    val (droppedWindow, currentWindow) = window.span { case ((k, _)) => !windowRange.contains(k) }

    val currentState: U = summarizer match {
      case lss: LeftSubtractableSummarizer[V, U, V2] =>
        droppedWindow.foldLeft(priorState) { case (u, (_, v)) => lss.subtract(u, v) }
      case s: Summarizer[V, U, V2] =>
        currentWindow.foldLeft(s.zero()) { case (u, (k, v)) => s.add(u, v) }
    }

    windows.put(coreSk, currentWindow)
    summarizerStates.put(coreSk, currentState)

    // Iterating through the rest of rows until reaching the boundary of "core row" window, it
    // (1) updates the window for the "core row";
    // (2) updates the summarizer state for the window of rows that have been iterated through so far;
    // (3) puts iterated rows into "core row" buffer for future use, i.e. the next().
    while (iter.hasNext && windowRange.contains(iter.head._1)) {
      val (k, v) = iter.next
      val sk = skFn(v)
      if (coreRange.contains(k)) {
        coreRowBuffer.enqueue((k, v))
      }

      // Update windows
      val window = windows.getOrElse(sk, Vector.empty)
      val u = summarizerStates.getOrElse(sk, summarizer.zero())
      windows.put(sk, window :+ (k, v))
      summarizerStates.put(sk, summarizer.add(u, v))
    }

    val dequed = coreRowBuffer.dequeue
    logDebug(s"Invoking next() deque: $dequed")

    (coreK,
      (
        coreV,
        summarizer.render(summarizerStates.getOrElse(coreSk, sys.error(s"Summarize state is empty for $coreSk")))
      ))
  }

}

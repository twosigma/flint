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

import com.twosigma.flint.rdd._
import com.twosigma.flint.rdd.function.summarize.summarizer.overlappable.OverlappableSummarizer
import com.twosigma.flint.rdd.function.summarize.summarizer.subtractable.{
  LeftSubtractableOverlappableSummarizer,
  LeftSubtractableSummarizer
}
import com.twosigma.flint.rdd.function.summarize.summarizer.Summarizer
import com.twosigma.flint.util.collection.Implicits._

import scala.reflect.ClassTag
import java.util.{
  HashMap => JHashMap,
  LinkedList => JLinkedList
}

import grizzled.slf4j.Logger
import org.apache.spark.OneToOneDependency

object SummarizeWindows {

  def apply[K: Ordering: ClassTag, SK, V: ClassTag, U, V2](
    rdd: OrderedRDD[K, V],
    window: K => (K, K),
    summarizer: Summarizer[V, U, V2],
    skFn: V => SK
  ): OrderedRDD[K, (V, V2)] = {
    val windowFn = getWindowRange(window) _
    val overlappedRdd = OverlappedOrderedRDD(rdd, window)
    val splits = overlappedRdd.rangeSplits
    overlappedRdd.mapPartitionsWithIndexOverlapped(
      (partitionIndex, iterator) =>
        new WindowSummarizerIterator(iterator.buffered, splits(partitionIndex).range, windowFn, summarizer, skFn)
    ).nonOverlapped()
  }

  def applyOverlapped[K: ClassTag, SK, V: ClassTag, U, V2: ClassTag](
    rdd: OrderedRDD[K, V],
    window: K => (K, K),
    summarizer: OverlappableSummarizer[V, U, V2],
    skFn: V => SK,
    lagWindow: K => (K, K)
  )(
    implicit
    ord: Ordering[K]
  ): OrderedRDD[K, (V, V2)] = {
    val windowFn = getWindowRange(window) _
    val lagWindowFn = getCloseOpenWindowRange(lagWindow) _

    val composedWindow: K => (K, K) = { k =>
      val lagWindowRange = lagWindowFn(windowFn(k).begin)
      require(
        ord.equiv(lagWindowRange.end.getOrElse(lagWindowRange.begin), windowFn(k).begin),
        "Lag window and data window must be contiguous"
      )
      (lagWindowFn(windowFn(k).begin).begin, windowFn(k).end.get)
    }
    if (otherRdd == null) {
      val overlappedRdd = OverlappedOrderedRDD(rdd, composedWindow)
      val splits = overlappedRdd.rangeSplits
      overlappedRdd.mapPartitionsWithIndexOverlapped(
        (partitionIndex, iterator) =>
          new OverlappableWindowSummarizerIterator(
            iterator.buffered, splits(partitionIndex).range, windowFn, lagWindowFn, summarizer, skFn
          )
      ).nonOverlapped()
    } else {
      throw new scala.NotImplementedError("Join iterator not implemented for overlappable summarizers")
    }
  }

  private[rdd] def getCloseOpenWindowRange[K](windowFn: K => (K, K))(k: K)(implicit ord: Ordering[K]): Range[K] = {
    val (b, e) = windowFn(k)
    val range = Range.closeOpen(b, Some(e))
    require(
      range.contains(k) || ord.equiv(k, e),
      s"The window function produces a window [$b, $e) which doesn't include key $k and $k is not equal to $e."
    )
    range
  }

  private[rdd] def getWindowRange[K](windowFn: K => (K, K))(k: K)(implicit ord: Ordering[K]): Range[K] = {
    val (b, e) = windowFn(k)
    val range = Range.closeClose(b, e)
    require(range.contains(k), s"The window function produces a window [$b, $e] which doesn't include key $k.")
    range
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
 * Core range is range of the core partition.
 *
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
  windowFn: K => Range[K],
  summarizer: Summarizer[V, U, V2],
  skFn: V => SK
)(implicit ord: Ordering[K]) extends Iterator[(K, (V, V2))] {

  val logger = Logger(this.getClass)

  private val INITIAL_MAP_CAPACITY = 1024

  private val windows = new JHashMap[SK, JLinkedList[(K, V)]](INITIAL_MAP_CAPACITY)
  private val summarizerStates = new JHashMap[SK, U](INITIAL_MAP_CAPACITY)
  private val coreRowBuffer = new JLinkedList[(K, V)]()

  lazy val rampUp = {
    val initWindowRange = windowFn(coreRange.begin)
    logger.debug(s"Initial window range in rampUp: $initWindowRange")
    if (iter.hasNext) {
      logger.debug(s"rampUp: head: ${iter.head}")
    }

    // Iterate rows until just before the core partition and initialize `windows` and `summarizerStates`
    while (iter.hasNext && coreRange.beginGt(iter.head._1)) {
      val (k, v) = iter.next
      val sk = skFn(v)
      logger.debug(s"rampUp: reading: ($k, $sk, $v)")

      // Put rows that `could` be in first coreRow's window in `windows` and `summarizerStates`
      // Because it's possible that coreRange.begin < first coreRow.begin, some rows here might
      // not actually by in the first coreRow's window. In this case, those rows will be dropped
      // later.
      if (initWindowRange.contains(k)) {
        val window = windows.getOrDefault(sk, new JLinkedList())
        val u = summarizerStates.getOrDefault(sk, summarizer.zero())
        window.add((k, v))
        windows.put(sk, window)
        summarizerStates.put(sk, summarizer.add(u, v))
      }
    }
  }

  override def hasNext: Boolean = {
    // rampUp is only invoke once.
    // rampUp read all the rows before the coreRange and initialize windows
    rampUp
    !coreRowBuffer.isEmpty || (iter.hasNext && coreRange.contains(iter.head._1))
  }

  override def next(): (K, (V, V2)) = if (hasNext) computeNext() else Iterator.empty.next

  private def computeNext(): (K, (V, V2)) = {
    // This is a little bit tricky. After `rampUp`, `iter.head` is guaranteed to point
    // to the first row in the `coreRange` even if the `coreK` is far far from the `coreRange.begin`.
    // This is because `iter.head._1 >= coreK` after `rampUp` and `coreK >= windowRange.begin` always
    // holds by the definition of window function.
    val (coreK, coreV) = Option(coreRowBuffer.peekFirst()).getOrElse(iter.head)
    val coreSk = skFn(coreV)
    val windowRange = windowFn(coreK)
    logger.debug(s"Invoking next() with core row: ($coreK, $coreSk, $coreV) and the window of $coreK: $windowRange")

    // Drop rows.
    val window = windows.getOrDefault(coreSk, new JLinkedList())
    val priorState = summarizerStates.getOrDefault(coreSk, summarizer.zero())
    val (droppedWindow, _) = window.dropWhile { kv => !windowRange.contains(kv._1) }

    val currentState: U = summarizer match {
      case lss: LeftSubtractableSummarizer[V, U, V2] =>
        droppedWindow.foldLeft(priorState) { case (u, (_, v)) => lss.subtract(u, v) }
      case s: Summarizer[V, U, V2] =>
        window.foldLeft(s.zero()) { case (u, (_, v)) => s.add(u, v) }
    }

    windows.put(coreSk, window)
    summarizerStates.put(coreSk, currentState)

    // Iterating through the rest of rows until reaching the boundary of "core row" window, it
    // (1) updates the window for the "core row";
    // (2) updates the summarizer state for the window of rows that have been iterated through so far;
    // (3) puts iterated rows into "core row" buffer for future use, i.e. the next().
    while (iter.hasNext && windowRange.contains(iter.head._1)) {
      val (k, v) = iter.next
      val sk = skFn(v)
      if (coreRange.contains(k)) {
        coreRowBuffer.addLast((k, v))
      }

      // Update windows
      val window = windows.getOrDefault(sk, new JLinkedList())
      val u = summarizerStates.getOrDefault(sk, summarizer.zero())
      window.add((k, v))
      windows.put(sk, window)
      summarizerStates.put(sk, summarizer.add(u, v))
    }

    val removed = coreRowBuffer.removeFirst()
    logger.debug(s"Invoking next() to remove: $removed")

    if (!summarizerStates.containsKey(coreSk)) {
      sys.error(s"Summarize state is empty for $coreSk")
    }
    val state = summarizerStates.get(coreSk)
    val rendered = summarizer.render(state)

    (coreK, (coreV, rendered))
  }
}

/**
 * The algorithm proceeds identically to the iterator in [[SummarizeWindows]] with the extension that we also
 * track a lag window for each key and the summarizer must be overlappable. The lag window defines the range of
 * rows that need to be added as overlapped during the ramp-up. By definition, the lag window is "before" the window,
 * and the window contains the core row, so when scanning left to right after rampUp, rows should always be added
 * to the window, i.e. added as non-overlapped.
 *
 * The algorithm guarantees the following when `summarizer` is a [[LeftSubtractableOverlappableSummarizer]]:
 * 1. When a row passes out of the window, `subtractOverlapped` will be called with the row and the boolean flag
 *    set to false. This occurs regardless of whether or not the row passed into the lag window
 * 2. When a row passes out of the lag window, `subtractOverlapped` will be called with the row and the boolean flag
 *    set to false.
 */
private[rdd] class OverlappableWindowSummarizerIterator[K, SK, V, U, V2](
  iter: BufferedIterator[(K, V)],
  coreRange: Range[K],
  windowFn: K => Range[K],
  lagWindowFn: K => Range[K],
  summarizer: OverlappableSummarizer[V, U, V2],
  skFn: V => SK
)(implicit ord: Ordering[K]) extends Iterator[(K, (V, V2))] {

  val logger = Logger(this.getClass)

  private val INITIAL_MAP_CAPACITY = 1024

  private val lagWindows = new JHashMap[SK, JLinkedList[(K, V)]](INITIAL_MAP_CAPACITY)
  private val windows = new JHashMap[SK, JLinkedList[(K, V)]](INITIAL_MAP_CAPACITY)
  private val summarizerStates = new JHashMap[SK, U](INITIAL_MAP_CAPACITY)
  private val coreRowBuffer = new JLinkedList[(K, V)]()

  private val initWindowRange = windowFn(coreRange.begin)
  private val initLagWindowRange = lagWindowFn(initWindowRange.begin)

  private lazy val rampUp = {
    // initLagWindowRange must be empty (i.e. [k, k)) or contiguous with the initWindowRange
    require(
      initLagWindowRange.endEquals(Option(initLagWindowRange.begin))
        || initLagWindowRange.endEquals(Some(initWindowRange.begin))
    )
    logger.debug(s"Initial window range in rampUp: $initWindowRange")
    logger.debug(s"Initial lag window range in rampUp: $initLagWindowRange")
    if (iter.hasNext) {
      logger.debug(s"rampUp: head: ${iter.head}")
    }

    // Iterate rows until just before the core partition and initialize `windows`, `lagWindows` and `summarizerStates`
    while (iter.hasNext && coreRange.beginGt(iter.head._1)) {
      val (k, v) = iter.next
      val sk = skFn(v)
      logger.debug(s"rampUp: reading: ($k, $sk, $v)")

      // Put rows that `could` be in first coreRow's window in `windows`, `lagWindows` and `summarizerStates`
      if (initWindowRange.contains(k) || initLagWindowRange.contains(k)) {
        val lagWindow = lagWindows.getOrDefault(sk, new JLinkedList())
        val window = windows.getOrDefault(sk, new JLinkedList())
        val u = summarizerStates.getOrDefault(sk, summarizer.zero())
        val overlapped = initLagWindowRange.contains(k)
        if (overlapped) {
          lagWindow.add((k, v))
          lagWindows.put(sk, lagWindow)
        } else {
          window.add((k, v))
          windows.put(sk, window)
        }

        summarizerStates.put(sk, summarizer.addOverlapped(u, (v, overlapped)))
      }
    }
  }

  override def hasNext: Boolean = {
    // rampUp is only invoke once.
    rampUp
    !coreRowBuffer.isEmpty || (iter.hasNext && coreRange.endGteq(iter.head._1))
  }

  override def next(): (K, (V, V2)) = if (hasNext) computeNext() else Iterator.empty.next

  private def computeNext(): (K, (V, V2)) = {
    // This is a little bit tricky. After `rampUp`, `iter.head` is guaranteed to point
    // to the first row in the `coreRange` even if the `coreK` is far far from the `coreRange.begin`.
    // This is because `iter.head._1 >= coreK` after `rampUp` and `coreK >= windowRange.begin` always
    // holds by the definition of window function.
    val (coreK, coreV) = Option(coreRowBuffer.peekFirst()).getOrElse(iter.head)
    val coreSk = skFn(coreV)
    val windowRange = windowFn(coreK)
    val lagWindowRange = lagWindowFn(windowRange.begin)
    // lagWindowRange must be empty (i.e. [k, k)) or contiguous with the windowRange
    require(lagWindowRange.endEquals(Option(lagWindowRange.begin)) || lagWindowRange.endEquals(Some(windowRange.begin)))
    logger.debug(s"Invoking next() with core row: ($coreK, $coreSk, $coreV) " +
      s"and the window of $coreK: $windowRange, $lagWindowRange")

    val priorState = summarizerStates.getOrDefault(coreSk, summarizer.zero())

    // The slices here are
    //   droppedLagWindow:       the rows that were in the lag window last iteration but are no longer in the lag
    //                           window
    //   existingLagWindow:      the rows that were in the lag window last iteration and are still in the lag window
    //   freshlyLaggedWindow:    the rows that were in the window last iteration and are now in the lag window
    //   notLaggedWindow:        the rows that were in the window last iteration and are not in the lag window
    //   droppedNotLaggedWindow: the rows that were in the window last iteration and are now in neither the lag window
    //                           or window. Either this or the lag window range should be empty since we constrain the
    //                           lag window and the window to be contiguous
    //   currentWindow:          the rows that were in the window last iteration and are still in the window

    val (droppedLagWindow, existingLagWindow) =
      lagWindows.getOrDefault(coreSk, new JLinkedList()).dropWhile { kv => !lagWindowRange.contains(kv._1) }
    val (freshlyLaggedWindow, notLaggedWindow) =
      windows.getOrDefault(coreSk, new JLinkedList()).dropWhile { kv => lagWindowRange.contains(kv._1) }
    val (droppedNotLaggedWindow, currentWindow) =
      notLaggedWindow.dropWhile { kv => !windowRange.contains(kv._1) }

    val fullLagWindow = new JLinkedList[(K, V)](existingLagWindow)
    fullLagWindow.addAll(freshlyLaggedWindow)

    val currentState: U = summarizer match {
      case lss: LeftSubtractableOverlappableSummarizer[V, U, V2] =>
        // Subtract rows that were dropped from the left side of the lag window
        val droppedLaggedState =
          droppedLagWindow.foldLeft(priorState) {
            case (u, (_, v)) => lss.subtractOverlapped(u, (v, true))
          }
        // Subtract rows that have passed from the left side of the window. Since these rows were added
        // as non-overlapped, we set the overlap flag to false
        val subtractedLaggedWindow = new JLinkedList[(K, V)](freshlyLaggedWindow)
        subtractedLaggedWindow.addAll(droppedNotLaggedWindow)
        subtractedLaggedWindow.foldLeft(droppedLaggedState) {
          case (u, (_, v)) => lss.subtractOverlapped(u, (v, false))
        }
      case os: OverlappableSummarizer[V, U, V2] =>
        val withLagged = fullLagWindow.foldLeft(summarizer.zero()) {
          case (u, (_, v)) =>
            os.addOverlapped(u, (v, true))
        }
        currentWindow.foldLeft(withLagged) {
          case (u, (_, v)) =>
            os.addOverlapped(u, (v, false))
        }
    }
    lagWindows.put(coreSk, fullLagWindow)
    windows.put(coreSk, currentWindow)
    summarizerStates.put(coreSk, currentState)

    // Iterating through the rest of rows until reaching the boundary of "core row" window, it
    // (1) updates the window for the "core row";
    // (2) updates the summarizer state for the window of rows that have been iterated through so far;
    // (3) puts iterated rows into "core row" buffer for future use, i.e. the next().
    while (iter.hasNext && (lagWindowRange.contains(iter.head._1) || windowRange.contains(iter.head._1))) {
      val (k, v) = iter.next
      val sk = skFn(v)
      if (coreRange.contains(k)) {
        coreRowBuffer.addLast((k, v))
      }

      // Update windows
      val window = windows.getOrDefault(sk, new JLinkedList())
      val u = summarizerStates.getOrDefault(sk, summarizer.zero())
      val overlapped = lagWindowRange.contains(k)
      require(!overlapped)
      window.add((k, v))
      windows.put(sk, window)
      val state = summarizer.addOverlapped(u, (v, overlapped))

      summarizerStates.put(sk, state)
    }

    val removed = coreRowBuffer.removeFirst()
    logger.debug(s"Invoking next() to remove: $removed")
    if (!summarizerStates.containsKey(coreSk)) {
      sys.error(s"Summarize state is empty for $coreSk")
    }
    val state = summarizerStates.get(coreSk)
    val rendered = summarizer.render(state)

    (coreK, (coreV, rendered))
  }
}
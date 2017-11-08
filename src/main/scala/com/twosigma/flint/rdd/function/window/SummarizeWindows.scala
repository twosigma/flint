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

package com.twosigma.flint.rdd.function.window

import java.util.{ ArrayList => JArrayList, HashMap => JHashMap, LinkedList => JLinkedList }

import com.twosigma.flint.rdd._
import com.twosigma.flint.rdd.function.summarize.summarizer.overlappable.OverlappableSummarizer
import com.twosigma.flint.rdd.function.summarize.summarizer.subtractable.{ LeftSubtractableOverlappableSummarizer, LeftSubtractableSummarizer }
import com.twosigma.flint.rdd.function.summarize.summarizer.{ FlippableSummarizer, Summarizer }
import com.twosigma.flint.rdd.function.window.summarizer.WindowBatchSummarizer
import com.twosigma.flint.util.collection.Implicits._
import grizzled.slf4j.Logger
import org.apache.spark.OneToOneDependency

import scala.reflect.ClassTag

object SummarizeWindows {

  def apply[K: Ordering: ClassTag, SK, V: ClassTag, U, V2](
    rdd: OrderedRDD[K, V],
    window: K => (K, K),
    summarizer: Summarizer[V, U, V2],
    skFn: V => SK
  ): OrderedRDD[K, (V, V2)] = {
    val windowFn = getWindowRange(window) _

    val otherRdd: OrderedRDD[K, V] = null
    val otherSkFn: V => SK = null

    if (otherRdd == null) {
      val overlappedRdd = OverlappedOrderedRDD(rdd, window)
      val splits = overlappedRdd.rangeSplits
      summarizer match {
        case fs: FlippableSummarizer[V, U, V2] =>
          overlappedRdd.mapPartitionsWithIndexOverlapped(
            (partitionIndex, iterator) =>
              new WindowFlipperIterator(
                iterator.buffered,
                splits(partitionIndex).range,
                windowFn,
                fs,
                skFn
              )
          ).nonOverlapped()
        // In WindowSummarizerIterator, a LeftSubtractableSummarizer will use subtract to remove rows while moving
        // the window whereas a normal Summarizer will recompute a new window from a zero state.
        case _: Summarizer[V, U, V2] =>
          overlappedRdd.mapPartitionsWithIndexOverlapped(
            (partitionIndex, iterator) =>
              new WindowIterator(
                iterator.buffered,
                splits(partitionIndex).range,
                windowFn,
                summarizer,
                skFn
              )
          ).nonOverlapped()
      }
    } else {
      val overlappedRdd = OverlappedOrderedRDD(rdd, otherRdd, window)

      val leftDeps = new OneToOneDependency(rdd)
      val rightDeps = new OneToOneDependency(overlappedRdd)

      new OrderedRDD[K, (V, V2)](rdd.sc, rdd.rangeSplits, Seq(leftDeps, rightDeps))(
        (part, context) => {
          val rightPart = overlappedRdd.partitions(part.index)
          val leftIter = rdd.iterator(part, context)
          val rightIter = overlappedRdd.iterator(rightPart, context)
          summarizer match {
            case fs: FlippableSummarizer[V, U, V2] =>
              new WindowJoinFlipperIterator(leftIter, rightIter, windowFn, fs, skFn, otherSkFn)
            // In WindowJoinIterator, a LeftSubtractableSummarizer will use subtract to remove rows while moving
            // the window whereas a normal Summarizer will recompute a new window from a zero state.
            case _: Summarizer[V, U, V2] =>
              new WindowJoinIterator(leftIter, rightIter, windowFn, summarizer, skFn, otherSkFn)
          }
        }
      )
    }
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

    val otherRdd: OrderedRDD[K, V] = null

    if (otherRdd == null) {
      val overlappedRdd = OverlappedOrderedRDD(rdd, composedWindow)
      val splits = overlappedRdd.rangeSplits
      overlappedRdd.mapPartitionsWithIndexOverlapped(
        (partitionIndex, iterator) =>
          new OverlappableWindowIterator(
            iterator.buffered, splits(partitionIndex).range, windowFn, lagWindowFn, summarizer, skFn
          )
      ).nonOverlapped()
    } else {
      throw new scala.NotImplementedError("Join iterator not implemented for overlappable summarizers")
    }
  }

  def applyBatch[K: ClassTag, SK: ClassTag, V: ClassTag, U, V2: ClassTag](
    rdd: OrderedRDD[K, V],
    window: K => (K, K),
    summarizer: WindowBatchSummarizer[K, SK, V, U, V2],
    skFn: V => SK,
    otherRdd: OrderedRDD[K, V],
    otherSkFn: V => SK,
    batchSize: Int
  )(
    implicit
    ord: Ordering[K]
  ): OrderedRDD[K, V2] = {
    require(
      otherRdd == null && otherSkFn == null || otherRdd != null && otherSkFn != null,
      "Must specify both otherRdd and otherSkFn or neither."
    )
    val overlappedRdd = OverlappedOrderedRDD(rdd, otherRdd, window)
    val windowFn = getWindowRange(window) _

    val leftDeps = new OneToOneDependency(rdd)
    val rightDeps = new OneToOneDependency(overlappedRdd)

    new OrderedRDD[K, V2](rdd.sc, rdd.rangeSplits, Seq(leftDeps, rightDeps))(
      (part, context) => {
        val rightPart = overlappedRdd.partitions(part.index)
        val leftIter = rdd.iterator(part, context)
        val rightIter = overlappedRdd.iterator(rightPart, context)
        new WindowBatchIterator(leftIter, rightIter, windowFn, summarizer, skFn, otherSkFn, batchSize)
      }
    )
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
 * Summarize windows into batches.
 *
 * Usually, how summarize window works is that for each left row, we compute a value for the row using a Summarizer over
 * its window from right.
 * This function works differently. Instead of computing a value for each left row, we group left rows into multiple
 * batches.
 * For each batch of left rows, we also compute a batch of right rows that covers all windows of the left batch.
 * For each left row, we also compute the begin (inclusive) and end (exclusive) index which are used to identify
 * its window in the right batch:
 *
 *     window(leftBatch(i)) = rightBatch(begin(i), end(i))
 *
 * Left batch, right batch and indices are presented as V2 and for each V2, we use the K of the first left row as
 * its K.
 *
 * To give an example,
 *
 * Left rows: [(1000,1), (1000,2), (2000,2), (2000,1), (3000,1), (3000,2), (4000,1), (4000,2), (5000,1), (5000,2)]
 * Right rows: [(0,1), (0,2), (1000,1), (1000,2), (2000,1), (2000,2), (3000,1), (3000,2), (4000,1), (4000,2)]
 * Window: [t - 1000, t]
 * Batch size: 4
 *
 * The result will be three (K, V2), each presenting one batch:
 *
 * Batch 1: K = 1000
 * Left batch: [(1000,1), (1000,2), (2000,2), (2000,1)]
 * Right batch: [(0,1), (1000,1), (2000,1), (0,2), (1000,2), (2000,2)]
 * Indices: [(0,2), (3,5), (4,6), (1,3)]
 *
 * Batch 2:
 * Left batch: [(3000,1), (3000,2), (4000,1), (4000,2)]
 * Right batch: [(2000,1), (3000,1), (4000,1), (2000,2), (3000,2), (4000,2)]
 * Indices: [(0,2), (3,5), (1,3), (4,6)]
 *
 * Batch 3:
 * Left batch: [(5000,1), (5000,2)]
 * Right batch: [(4000,1), (4000,2)]
 * Indices: [(0,1), (1,2)]
 *
 * This function maintains invariants:
 *
 * (1) Rows in the left batch have the same order as the left iter. Rows in the right batch does NOT have the same
 *     order as the right iter. The indices have the same ordering as the left batch.
 * (2) Rows in the right batch are grouped by SK. With in each group, rows are ordered by input order from right iter.
 *     Ordering of different groups is undefined.
 * (3) Right batch MUST include all windows of the left batch. Right batch is allowed to have rows that is not in any
 *     window of the left batch (a gap in left batch, for instance).
 *     Although for performance reasons, number of such rows should be minimized.
 *
 * See also: [[WindowBatchSummarizer]] [[com.twosigma.flint.timeseries.window.summarizer.BaseWindowBatchSummarizer]]
 *
 */
class WindowBatchIterator[K, SK, V, U, V2](
  leftIter: Iterator[(K, V)],
  rightIter: Iterator[(K, V)],
  windowFn: K => Range[K],
  summarizer: WindowBatchSummarizer[K, SK, V, U, V2],
  leftSkFn: V => SK,
  rightSkFn: V => SK,
  batchSize: Int
)(implicit ord: Ordering[K]) extends Iterator[(K, V2)] {

  private[this] val INITIAL_MAP_CAPACITY = 1024
  private[this] val leftIterBuffered = leftIter.buffered
  private[this] val rightIterBuffered = rightIter.buffered
  private val windows = new JHashMap[SK, JLinkedList[(K, V)]](INITIAL_MAP_CAPACITY)

  override def hasNext: Boolean = leftIterBuffered.hasNext

  override def next(): (K, V2) = {
    var count = 0
    var firstK: Option[K] = None
    val state = summarizer.zero()

    // Initialize window state and update windows.
    // Iterate through all current windows. For each window, remove rows that is out of the first left window range.
    // Then, add all remaining rows into the new state for the batch.
    val (firstLeftK, _) = leftIterBuffered.head
    val firstWindowRange = windowFn(firstLeftK)
    val windowsIter = windows.entrySet().iterator()
    while (windowsIter.hasNext) {
      val entry = windowsIter.next()
      val sk = entry.getKey
      val window = entry.getValue
      val (_, remainingWindow) = window.dropWhile(kv => !firstWindowRange.contains(kv._1))

      val remainingWindowIter = remainingWindow.iterator()
      while (remainingWindowIter.hasNext) {
        val kv = remainingWindowIter.next()
        summarizer.addRight(state, sk, kv._2)
      }
    }

    // Compute window state
    while (leftIterBuffered.hasNext && count < batchSize) {
      val (k, leftV) = leftIterBuffered.next()

      val leftSk = leftSkFn(leftV)
      val windowRange = windowFn(k)

      var currentWindow = windows.putIfAbsent(leftSk, new JLinkedList())
      if (currentWindow == null) { currentWindow = windows.get(leftSk) }

      summarizer.addLeft(state, leftSk, leftV)

      // Drop rows in window that is not used for the current window
      {
        val (droppedWindow, _) = currentWindow.dropWhile(kv => !windowRange.contains(kv._1))
        droppedWindow.foldLeft(state) { case (u, (_, v)) => summarizer.subtractRight(u, leftSk, v); u }
      }

      // Iterate right iter and drop rows that is before the current window range
      while (rightIterBuffered.hasNext && windowRange.beginGt(rightIterBuffered.head._1)) {
        rightIterBuffered.next()
      }

      // Iterate right iter and add all rows in the current window range into the state
      // Note this will add rows of other SK into the state as well. This is fine.
      // This also drops rows from windows for other SK if they are passed by the current window.
      while (rightIterBuffered.hasNext && windowRange.contains(rightIterBuffered.head._1)) {
        val (rightK, rightV) = rightIterBuffered.next()
        val rightSk = rightSkFn(rightV)

        var windowForRight = windows.putIfAbsent(rightSk, new JLinkedList())
        if (windowForRight == null) { windowForRight = windows.get(rightSk) }

        windowForRight.add((rightK, rightV))
        summarizer.addRight(state, rightSk, rightV)

        // Drop rows in window that is passed by current window
        // Each window row will only be add/dropped once and the inner while will at most visit one undropped row.
        // So this is linear rather than quadratic.
        val (droppedWindow, _) = windowForRight.dropWhile(kv => windowRange.beginGt(kv._1))
        droppedWindow.foldLeft(state) {
          case (u, (_, droppedRightV)) =>
            summarizer.subtractRight(u, rightSk, droppedRightV)
            u
        }
      }
      summarizer.commitLeft(state, leftSk, leftV)

      if (firstK.isEmpty) firstK = Some(k)
      count += 1
    }

    (firstK.get, summarizer.render(state))
  }
}

/**
 * The following two iterators are based off of the algorithm Flipper.
 *
 * This implementation requires that the summarizer is associative, and also that the merge operation does not
 * mutate the state on the righthand side.
 *
 * If we imagine the data partitioned into non-overlapping "anchor" windows, then each actual window we compute can be
 * seen as the sum of the suffix of a left anchor window and the prefix of a right anchor window.
 * This algorithm works by maintaining the left and right window, adding new
 * rows to the right window and utilizing the appropriate suffix sum of the left window in order to create the new
 * window. When necessary, it will flip the right window, computing suffix sums and setting it as the new left window.
 *
 * To provide an example, imagine a table as follows:
 * [1, 2, 3, 4, 5, 6]
 * with a past 3 unit window. This example will be on just one table, but the algorithm for two tables is similar.
 * For 1: We add 1 to the right window.
 * L: [], R: [1]
 *
 * For 2: We flip 1 to the left window, and add 2 to the right window.
 * L: [(1, [1])], R: [2]
 *
 * For 3: We add 3 to the right window.
 * L: [(1, [1])], R: [2, 3]
 *
 * For 4: 1 exits the left window, so we flip the right window to become the new left window. Then, we add 4 to the
 * right window.
 * L: [(2, [2, 3]), (3, [3])], R: [4]
 *
 * For 5: 2 exits the left window, and then we add 5 to the right window.
 * L: [(3, [3])], R: [4, 5]
 *
 * For 6: 3 exits the left window, we flip the right window, then we add 6 to the new right window.
 * L: [(4, [4, 5]), (5, [5])], R: [6]
 *
 * If n is the size of the table and w is the size of the largest window:
 * Runtime: O(n)
 * Space: O(w)
 */
private[rdd] class WindowJoinFlipperIterator[K, SK, V, U, V2](
  leftIter: Iterator[(K, V)],
  rightIter: Iterator[(K, V)],
  windowFn: K => Range[K],
  summarizer: FlippableSummarizer[V, U, V2],
  leftSkFn: V => SK,
  rightSkFn: V => SK
)(implicit ord: Ordering[K]) extends Iterator[(K, (V, V2))] {
  private[this] val INITIAL_MAP_CAPACITY = 1024
  private[this] val rightIterBuffered = rightIter.buffered

  // For each secondary key, stores a list of key to suffix pairs, in increasing order of key.
  private[this] val skToLeftWindowSuffixSums = new JHashMap[SK, JLinkedList[(K, U)]](INITIAL_MAP_CAPACITY)
  // For each secondary key, stores the current prefix sum.
  private[this] val skToRightWindowPrefixSum = new JHashMap[SK, U](INITIAL_MAP_CAPACITY)
  // For each secondary key, stores the current rows in the prefix as key value pairs.
  private[this] val skToRightWindow = new JHashMap[SK, JLinkedList[(K, V)]](INITIAL_MAP_CAPACITY)

  // For a given secondary key, flips the right window to become the new left window and computes its suffixes.
  // After this function, the left window will contain suffix sums within the range and the right window will be empty.
  private[this] def flip(sk: SK, windowRange: Range[K]): Unit = {
    val leftWindowSuffixSums = skToLeftWindowSuffixSums.getOrDefault(sk, new JLinkedList())
    assert(leftWindowSuffixSums.isEmpty)

    // Get rows in the right window (within the range) in order to flip them.
    val rightWindow = skToRightWindow.getOrDefault(sk, new JLinkedList())
    rightWindow.dropWhile { kv => !windowRange.contains(kv._1) }

    // Calculate suffix sums.
    val newLeftWindowSuffixSums = rightWindow.foldRight(new JLinkedList[(K, U)]()) {
      case (currWindowSuffixSums, (k, v)) =>
        val newRow = summarizer.add(summarizer.zero(), v)
        if (currWindowSuffixSums.isEmpty) {
          currWindowSuffixSums.add((k, newRow))
        } else {
          // This requires immutability of the right state during the merge operation.
          currWindowSuffixSums.addFirst((k, summarizer.merge(newRow, currWindowSuffixSums.getFirst._2)))
        }
        currWindowSuffixSums
    }

    // Flip right window into left window and reset right window to zero.
    skToLeftWindowSuffixSums.put(sk, newLeftWindowSuffixSums)
    skToRightWindowPrefixSum.put(sk, summarizer.zero())
    skToRightWindow.put(sk, new JLinkedList())
  }

  override def hasNext: Boolean = leftIter.hasNext

  override def next(): (K, (V, V2)) = {
    val (k, leftV) = leftIter.next
    val leftSk = leftSkFn(leftV)
    val windowRange = windowFn(k)

    // Drop rows.
    var leftWindowSuffixSums = skToLeftWindowSuffixSums.getOrDefault(leftSk, new JLinkedList())
    leftWindowSuffixSums.dropWhile { kv => windowRange.beginGt(kv._1) }
    skToLeftWindowSuffixSums.put(leftSk, leftWindowSuffixSums)

    // If we have exhausted our current suffixes, we flip the right window to become the new left window.
    if (leftWindowSuffixSums.isEmpty) {
      flip(leftSk, windowRange)
    }

    while (rightIterBuffered.hasNext && windowRange.beginGt(rightIterBuffered.head._1)) {
      rightIterBuffered.next()
    }

    // Add rows that are within the window range.
    while (rightIterBuffered.hasNext && windowRange.contains(rightIterBuffered.head._1)) {
      val (k, v) = rightIterBuffered.next()
      val sk = rightSkFn(v)

      // Updates the right window by adding the row.
      val rightWindow = skToRightWindow.getOrDefault(sk, new JLinkedList())
      rightWindow.addLast((k, v))
      skToRightWindow.put(sk, rightWindow)

      // Updates the right window sum by adding the row to the prefix sum.
      val rightWindowPrefixSum = skToRightWindowPrefixSum.getOrDefault(sk, summarizer.zero())
      skToRightWindowPrefixSum.put(sk, summarizer.add(rightWindowPrefixSum, v))
    }

    // Merge the corresponding suffix of the left window with the prefix of the right window.
    leftWindowSuffixSums = skToLeftWindowSuffixSums.getOrDefault(leftSk, new JLinkedList())
    val leftWindowSuffixSum = if (leftWindowSuffixSums.isEmpty) summarizer.zero() else leftWindowSuffixSums.getFirst._2
    val rightWindowPrefixSum = skToRightWindowPrefixSum.getOrDefault(leftSk, summarizer.zero())
    val state = summarizer.merge(leftWindowSuffixSum, rightWindowPrefixSum)
    val rendered = summarizer.render(state)

    (k, (leftV, rendered))
  }
}

/**
 * See WindowSummarizerIterator for an explanation of the core rows. See WindowJoinFlipperIterator for an explanation
 * of the Flipper algorithm.
 */
private[rdd] class WindowFlipperIterator[K, SK, V, U, V2](
  iter: BufferedIterator[(K, V)],
  coreRange: Range[K],
  windowFn: K => Range[K],
  summarizer: FlippableSummarizer[V, U, V2],
  skFn: V => SK
)(implicit ord: Ordering[K]) extends Iterator[(K, (V, V2))] {

  val logger = Logger(this.getClass)

  private[this] val INITIAL_MAP_CAPACITY = 1024
  // For each secondary key, stores a list of key to suffix pairs, in increasing order of key.
  private[this] val skToLeftWindowSuffixSums = new JHashMap[SK, JLinkedList[(K, U)]](INITIAL_MAP_CAPACITY)
  private[this] val coreRowBuffer = new JLinkedList[(K, V)]()
  // For each secondary key, stores the current prefix sum.
  private[this] val skToRightWindowPrefixSum = new JHashMap[SK, U](INITIAL_MAP_CAPACITY)
  // For each secondary key, stores the current rows in the prefix as key value pairs.
  private[this] val skToRightWindow = new JHashMap[SK, JLinkedList[(K, V)]](INITIAL_MAP_CAPACITY)

  // For a given secondary key, flips the right window to become the new left window and computes its suffixes.
  // After this function, the left window will contain suffix sums within the range and the right window will be empty.
  private[this] def flip(sk: SK, windowRange: Range[K]): Unit = {
    val leftWindowSuffixSums = skToLeftWindowSuffixSums.getOrDefault(sk, new JLinkedList())
    assert(leftWindowSuffixSums.isEmpty)

    // Get rows in the right window (within the range) in order to flip them.
    val rightWindow = skToRightWindow.getOrDefault(sk, new JLinkedList())
    rightWindow.dropWhile { kv => !windowRange.contains(kv._1) }

    // Calculate suffix sums.
    val newLeftWindowSuffixSums = rightWindow.foldRight(new JLinkedList[(K, U)]()) {
      case (currWindowSuffixSums, (k, v)) =>
        val newRow = summarizer.add(summarizer.zero(), v)
        if (currWindowSuffixSums.isEmpty) {
          currWindowSuffixSums.add((k, newRow))
        } else {
          // This requires immutability of the right state during the merge operation.
          currWindowSuffixSums.addFirst((k, summarizer.merge(newRow, currWindowSuffixSums.getFirst._2)))
        }
        currWindowSuffixSums
    }

    // Flip right window into left window and reset right window to zero.
    skToLeftWindowSuffixSums.put(sk, newLeftWindowSuffixSums)
    skToRightWindowPrefixSum.put(sk, summarizer.zero())
    skToRightWindow.put(sk, new JLinkedList())
  }

  private lazy val rampUp = {
    val initWindowRange = windowFn(coreRange.begin)
    logger.debug(s"Initial window range in rampUp: $initWindowRange")
    if (iter.hasNext) {
      logger.debug(s"rampUp: head: ${iter.head}")
    }

    // Iterate rows until just before the core partition and initialize `windows` and `summarizerStates`.
    while (iter.hasNext && coreRange.beginGt(iter.head._1)) {
      val (k, v) = iter.next
      val sk = skFn(v)
      logger.debug(s"rampUp: reading: ($k, $sk, $v)")

      // Put rows that `could` be in first coreRow's window in `skToRightWindow` and `skToRightWindowPrefixSum`
      // Because it's possible that coreRange.begin < first coreRow.begin, some rows here might
      // not actually by in the first coreRow's window. In this case, those rows will be dropped
      // later while flipping the window.
      if (initWindowRange.contains(k)) {
        val rightWindow = skToRightWindow.getOrDefault(sk, new JLinkedList())
        val rightWindowPrefixSum = skToRightWindowPrefixSum.getOrDefault(sk, summarizer.zero())
        rightWindow.addLast((k, v))
        skToRightWindow.put(sk, rightWindow)
        skToRightWindowPrefixSum.put(sk, summarizer.add(rightWindowPrefixSum, v))
      }
    }
  }

  override def hasNext: Boolean = {
    // rampUp is only invoke once.
    // rampUp read all the rows before the coreRange and initialize windows.
    rampUp
    !coreRowBuffer.isEmpty || (iter.hasNext && coreRange.contains(iter.head._1))
  }

  override def next(): (K, (V, V2)) = if (hasNext) computeNext() else Iterator.empty.next

  private def computeNext(): (K, (V, V2)) = {
    // This is a little bit tricky. After `rampUp`, `iter.head` is guaranteed to point
    // to the first row in the `coreRange` even if the `coreK` is far far from the `coreRange.begin`.
    // This is because `iter.head._1 >= coreK` after `rampUp` and `coreK >= windowRange.begin` always
    // holds by the definition of window function.

    // We do not need to add this core row to the right window because it was either already encountered and added to
    // the buffer, or it will be the next in the iterator.
    val (coreK, coreV) = Option(coreRowBuffer.peekFirst()).getOrElse(iter.head)
    val coreSk = skFn(coreV)
    val windowRange = windowFn(coreK)
    logger.debug(s"Invoking next() with core row: ($coreK, $coreSk, $coreV) and the window of $coreK: $windowRange")

    // Drop rows.
    var leftWindowSuffixSums = skToLeftWindowSuffixSums.getOrDefault(coreSk, new JLinkedList())
    leftWindowSuffixSums.dropWhile { kv => windowRange.beginGt(kv._1) }
    skToLeftWindowSuffixSums.put(coreSk, leftWindowSuffixSums)

    // If we have exhausted our current suffixes, we flip the right window to become the new left window.
    if (leftWindowSuffixSums.isEmpty) {
      flip(coreSk, windowRange)
    }

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

      // Updates the right window by adding the row.
      val rightWindow = skToRightWindow.getOrDefault(sk, new JLinkedList())
      rightWindow.addLast((k, v))
      skToRightWindow.put(sk, rightWindow)

      // Updates the right window sum by adding the row to the prefix sum.
      val rightWindowPrefixSum = skToRightWindowPrefixSum.getOrDefault(sk, summarizer.zero())
      skToRightWindowPrefixSum.put(sk, summarizer.add(rightWindowPrefixSum, v))
    }

    val removed = coreRowBuffer.removeFirst()
    logger.debug(s"Invoking next() to remove: $removed")

    // Merge the corresponding suffix of the left window with the prefix of the right window.
    leftWindowSuffixSums = skToLeftWindowSuffixSums.getOrDefault(coreSk, new JLinkedList())
    val leftWindowSuffixSum = if (leftWindowSuffixSums.isEmpty) summarizer.zero() else leftWindowSuffixSums.getFirst._2
    val rightWindowPrefixSum = skToRightWindowPrefixSum.getOrDefault(coreSk, summarizer.zero())
    val state = summarizer.merge(leftWindowSuffixSum, rightWindowPrefixSum)
    val rendered = summarizer.render(state)

    (coreK, (coreV, rendered))
  }
}

private[rdd] class WindowJoinIterator[K, SK, V, U, V2](
  leftIter: Iterator[(K, V)],
  rightIter: Iterator[(K, V)],
  windowFn: K => Range[K],
  summarizer: Summarizer[V, U, V2],
  leftSkFn: V => SK,
  rightSkFn: V => SK
)(implicit ord: Ordering[K]) extends Iterator[(K, (V, V2))] {
  private val INITIAL_MAP_CAPACITY = 1024

  private val windows = new JHashMap[SK, JLinkedList[(K, V)]](INITIAL_MAP_CAPACITY)
  private val summarizerStates = new JHashMap[SK, U](INITIAL_MAP_CAPACITY)
  private val rightIterBuffered = rightIter.buffered

  override def hasNext: Boolean = leftIter.hasNext

  override def next(): (K, (V, V2)) = {
    val (k, leftV) = leftIter.next
    val leftSk = leftSkFn(leftV)
    val windowRange = windowFn(k)

    // Drop rows from the current window.
    val window = windows.getOrDefault(leftSk, new JLinkedList())
    val priorState = summarizerStates.getOrDefault(leftSk, summarizer.zero())
    val (droppedWindow, _) = window.dropWhile { kv => !windowRange.contains(kv._1) }
    val currentState: U = summarizer match {
      case lss: LeftSubtractableSummarizer[V, U, V2] =>
        droppedWindow.foldLeft(priorState) { case (u, (_, v)) => lss.subtract(u, v) }
      case s: Summarizer[V, U, V2] =>
        window.foldLeft(s.zero()) { case (u, (_, v)) => s.add(u, v) }
    }

    windows.put(leftSk, window)
    summarizerStates.put(leftSk, currentState)

    while (rightIterBuffered.hasNext && windowRange.beginGt(rightIterBuffered.head._1)) {
      rightIterBuffered.next()
    }

    // Add rows
    while (rightIterBuffered.hasNext && windowRange.contains(rightIterBuffered.head._1)) {
      val (k, v) = rightIterBuffered.next()
      val sk = rightSkFn(v)

      // Update windows
      val window = windows.getOrDefault(sk, new JLinkedList())
      val u = summarizerStates.getOrDefault(sk, summarizer.zero())

      // This is a little bit tricky here. Consider this case:
      //
      // window: [t, t + 50]
      // left: [[1000, sk1], [2000, sk2]] right: [[1000, sk1], [1000, sk2]]
      // current left row: [1000, sk1]
      //
      // Here when we visit [1000, sk2], we will add it to the window and state of sk2,
      // however [1000, sk2] should not be in any window.
      //
      // This is NOT a bug because when computing [1000, sk2], we will either
      // (1) subtract [1000, sk2] from the window state or
      // (2) recompute window state.
      // Therefore, [1000, sk2] is not included in the final state.

      window.add((k, v))
      windows.put(sk, window)
      summarizerStates.put(sk, summarizer.add(u, v))
    }

    val state = summarizerStates.get(leftSk)
    require(state != null, s"Unexpected state: Summarize state is empty for $leftSk")
    val rendered = summarizer.render(state)

    (k, (leftV, rendered))
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
private[rdd] class WindowIterator[K, SK, V, U, V2](
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
 * set to false. This occurs regardless of whether or not the row passed into the lag window
 * 2. When a row passes out of the lag window, `subtractOverlapped` will be called with the row and the boolean flag
 * set to false.
 */
private[rdd] class OverlappableWindowIterator[K, SK, V, U, V2](
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

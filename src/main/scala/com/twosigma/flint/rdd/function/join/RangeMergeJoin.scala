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

package com.twosigma.flint.rdd.function.join

import com.twosigma.flint.rdd.{ Range, CloseOpen, RangeSplit }
import org.apache.spark.Partition

protected[flint] object RangeMergeJoin {

  /**
   * For each unique [[Range]] begin, return intersecting [[RangeSplit]]s from both thisSplits and thatSplits.
   * Can optionally use the toleranceFn to extend the time range in searching for intersections.
   */
  def mergeSplits[K](thisSplits: IndexedSeq[RangeSplit[K]], thatSplits: IndexedSeq[RangeSplit[K]],
    toleranceFn: K => K = { x: K => x })(
    implicit
    ord: Ordering[K]
  ): Seq[RangeMergeJoin[K]] = {
    require(RangeSplit.isSortedByRange(thisSplits))
    require(RangeSplit.isSortedByRange(thatSplits))

    if (thatSplits.isEmpty) {
      thisSplits.map {
        split => RangeMergeJoin(split.range, Seq(split), Seq())
      }
    } else if (thisSplits.isEmpty) {
      thatSplits.map { split => RangeMergeJoin(split.range, Seq(), Seq(split)) }
    } else {
      val begins = (thisSplits ++ thatSplits).map(_.range.begin).sorted
      mergeSplits(toleranceFn, begins.headOption, thisSplits, thatSplits, begins, Seq()).reverse
    }
  }

  @annotation.tailrec
  private def mergeSplits[K: Ordering](
    toleranceFn: K => K,
    begin: Option[K],
    thisSplits: IndexedSeq[RangeSplit[K]],
    thatSplits: IndexedSeq[RangeSplit[K]],
    begins: IndexedSeq[K],
    mergedSplits: Seq[RangeMergeJoin[K]]
  ): Seq[RangeMergeJoin[K]] = begin match {
    // The algorithm works as follows.
    // It splits the ordering space into disjoint ranges [a_1, a_2), [a_2, a_3), [a_3, a_4) ...
    // where a_i < a_{i+1}. For a specific range [a_i, a_{i+1}), it finds all splits from both side
    // that intersect with it and then assembles them together as a `RangeMergeJoin`.
    // That is, given a begin b0, it scans thisSplits and thatSplits split-by-split until
    // it finds a split whose begin b1 is different from the given one. The interval [b0, b1)
    // defines a range and it will be used to find splits (from both `thisSplits` and `thatSplits`)
    // that intersect with.
    case Some(b) =>
      // The end could be None which implies that the merge process will be completed.
      val end = RangeSplit.getNextBegin(b, begins)
      val searchRange = CloseOpen(toleranceFn(b), end)
      val mergedJoin = RangeMergeJoin(
        CloseOpen(b, end),
        RangeSplit.getIntersectingSplits(searchRange, thisSplits),
        RangeSplit.getIntersectingSplits(searchRange, thatSplits)
      )
      mergeSplits(toleranceFn, end, thisSplits, thatSplits, begins, mergedJoin +: mergedSplits)
    case None => mergedSplits
  }

  /**
   * Similar to [[leftJoinSplits]], but takes window function instead
   */
  // TODO: window function should probably returns a range instead of tuple so that
  //       we could support inclusiveness/exclusiveness on both sides.
  protected[rdd] def windowJoinSplits[K](
    windowFn: K => (K, K),
    leftSplits: IndexedSeq[RangeSplit[K]],
    rightSplits: IndexedSeq[RangeSplit[K]]
  )(implicit ord: Ordering[K]): Seq[(RangeSplit[K], Seq[Partition])] = {
    require(RangeSplit.isSortedByRange(leftSplits))
    require(RangeSplit.isSortedByRange(rightSplits))

    leftSplits.map { left =>
      (left, RangeSplit.getIntersectingSplits(left.range.expand(windowFn), rightSplits).map(_.partition))
    }
  }

  /**
   * For each [[RangeSplit]] in the left, return intersecting partitions from right
   *
   * @param toleranceFn A function provides look-back tolerance inclusively.
   * @param leftSplits  The splits from the left-hand side.
   * @param rightSplits The splits from the right-hand side.
   */
  protected[rdd] def leftJoinSplits[K](
    toleranceFn: K => K,
    leftSplits: IndexedSeq[RangeSplit[K]],
    rightSplits: IndexedSeq[RangeSplit[K]]
  )(implicit ord: Ordering[K]): Seq[(RangeSplit[K], Seq[Partition])] = {
    require(RangeSplit.isSortedByRange(leftSplits))
    require(RangeSplit.isSortedByRange(rightSplits))

    leftSplits.map { left =>
      val toleranceBegin = toleranceFn(left.range.begin)
      require(ord.gteq(left.range.begin, toleranceBegin), "It should be a look-back tolerance.")
      (left, RangeSplit.getIntersectingSplits(
        CloseOpen(toleranceBegin, left.range.end), rightSplits
      ).map(_.partition))
    }
  }

  protected[rdd] def futureLeftJoinSplits[K](
    toleranceFn: K => K,
    leftSplits: IndexedSeq[RangeSplit[K]],
    rightSplits: IndexedSeq[RangeSplit[K]]
  )(implicit ord: Ordering[K]): Seq[(RangeSplit[K], Seq[Partition])] = {
    require(RangeSplit.isSortedByRange(leftSplits))
    require(RangeSplit.isSortedByRange(rightSplits))

    leftSplits.map { left =>
      val toleranceEnd = left.range.end.map(toleranceFn(_))
      toleranceEnd.foreach { te =>
        left.range.end.foreach { e =>
          require(ord.lteq(e, te), "It should be a look-forward tolerance.")
        }
      }
      (left, RangeSplit.getIntersectingSplits(
        // This excludes the end as it is close-open range.
        CloseOpen(left.range.begin, toleranceEnd), rightSplits
      ).map(_.partition))
    }
  }
}

case class RangeMergeJoin[K](
  range: CloseOpen[K],
  left: Seq[RangeSplit[K]],
  right: Seq[RangeSplit[K]]
) extends Serializable

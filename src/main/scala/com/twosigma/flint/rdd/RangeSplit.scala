/*
 *  Copyright 2015-2016 TWO SIGMA OPEN SOURCE, LLC
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

package com.twosigma.flint.rdd

import com.twosigma.flint.rdd.{ CloseOpen, Range }
import org.apache.spark.Partition

import scala.collection.Searching._

object RangeSplit {

  /**
   * Find the first begin among the `begins` such that it is strictly greater than the given `begin`.
   * Return None if not found.
   *
   * The `begins` must be sorted.
   */
  private[rdd] def getNextBegin[K](begin: K, begins: IndexedSeq[K])(
    implicit
    ord: Ordering[K]
  ): Option[K] = {
    val result: SearchResult = begins.search(begin)
    var i = result.insertionPoint
    while (i < begins.length && !ord.gt(begins(i), begin)) {
      i = i + 1
    }
    if (i < begins.length) {
      Some(begins(i))
    } else {
      None
    }
  }

  /**
   * Find all [[RangeSplit]]s that intersect with a given range.
   *
   * The `splits` has been sorted by their ranges. See [[isSortedByRange]] for more details.
   */
  private[rdd] def getIntersectingSplits[K: Ordering](
    range: CloseOpen[K],
    splits: IndexedSeq[RangeSplit[K]]
  ): Seq[RangeSplit[K]] = {
    val split = RangeSplit(OrderedRDDPartition(0), range)
    Range.intersect(split, splits, { r: RangeSplit[K] => r.range }, true).map(splits(_))
  }

  /**
   * Test whether `splits` are sorted by their ranges.
   */
  private[rdd] def isSortedByRange[K: Ordering](splits: Seq[RangeSplit[K]]): Boolean =
    Range.isSorted(splits, { r: RangeSplit[K] => r.range })
}

/**
 * The range of a split implies that all records of a partition must have their keys within that range.
 *
 * @param partition The partition of this split.
 * @param range     The range associated with this split.
 */
case class RangeSplit[K](partition: Partition, range: CloseOpen[K])

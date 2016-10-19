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

object RangeSplit {

  /**
   * Find the first begin from among the [[RangeSplit]]s that is strictly greater than
   * the given begin. This new begin can be None if not found.
   */
  private[rdd] def getNextBegin[K](begin: K, splits: Seq[RangeSplit[K]])(
    implicit
    ord: Ordering[K]
  ): Option[K] = {
    splits.map { _.range.begin }.sorted.find { ord.gt(_, begin) }
  }

  /**
   * Find all [[RangeSplit]]s that intersect with a given range.
   */
  private[rdd] def getSplitsWithinRange[K: Ordering](
    range: Range[K],
    splits: Seq[RangeSplit[K]]
  ): Seq[RangeSplit[K]] = {
    // TODO: it should do a binary search instead of a linear scan.
    splits.filter { _.range.intersects(range) }.sortBy { _.range.begin }
  }
}

/**
 * The range of a split implies that all records of a partition must have their keys within that range.
 *
 * @param partition The partition of this split.
 * @param range     The range associated with this split.
 */
case class RangeSplit[K](partition: Partition, range: CloseOpen[K])

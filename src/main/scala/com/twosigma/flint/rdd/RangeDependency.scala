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

package com.twosigma.flint.rdd

import org.apache.spark.Partition
import scala.reflect.ClassTag

/**
 * :: DeveloperApi ::
 */
private[rdd] object RangeDependency {

  /**
   * Normalize the ranges of partitions from a sorted [[org.apache.spark.rdd.RDD]].
   *
   * @param headers               A sequence of [[OrderedPartitionHeader]]s which provide the header information
   *                              like partition instance and first two distinct keys etc. for all the partitions from
   *                              an [[org.apache.spark.rdd.RDD]].
   * @param normalizationStrategy The strategy expected to use for partition normalization. By default, it is
   *                              [[BasicNormalizationStrategy]].
   * @return A sequence of [[RangeDependency]].
   */
  def normalize[K, P <: Partition: ClassTag](
    headers: Seq[OrderedPartitionHeader[K, P]],
    normalizationStrategy: PartitionNormalizationStrategy = HeavyKeysNormalizationStrategy
  )(implicit ord: Ordering[K]): Seq[RangeDependency[K, P]] = {
    require(headers.nonEmpty, "Need at least one partition")

    val sortedHeaders = headers.sortBy(_.partition.index).toArray
    // Assume partitions are sorted, i.e. the keys of ith partition are less or equal than those of (i + 1)th partition.
    sortedHeaders.reduceOption {
      (h1, h2) =>
        if (ord.lteq(h1.firstKey, h2.firstKey)) {
          h2
        } else {
          sys.error(s"Partitions are not sorted. " +
            s"The partition ${h1.partition} has the first key ${h1.firstKey} and " +
            s"the partition ${h2.partition} has the first key ${h2.firstKey}.")
        }
    }

    val (nonNormalizedPartitions, nonNormalizedRanges) = sortedHeaders.zipWithIndex.map {
      case (hdr, idx) =>
        val range = if (idx < sortedHeaders.length - 1) {
          // It must be close-close range except the last one.
          Range.closeClose(hdr.firstKey, sortedHeaders(idx + 1).firstKey)
        } else {
          // This is by the best of our knowledge to tell the range of records in each partition.
          Range.closeOpen(hdr.firstKey, None)
        }
        (hdr.partition, range)
    }.unzip

    require(Range.isSorted(nonNormalizedRanges))

    val normalizedRanges = normalizationStrategy.normalize(sortedHeaders)
    normalizedRanges.sortBy(_.begin).zipWithIndex.map {
      case (normalizedRange, idx) =>
        val parents = normalizedRange.intersectsWith(nonNormalizedRanges, true).map(nonNormalizedPartitions(_))
        RangeDependency[K, P](idx, normalizedRange, parents)
    }
  }
}

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

/**
 * Represent the partition, the first key, and possibly the distinct second key of a partition.
 * Note that the type of partition must be the same type of partition where those keys come from.
 */
private[rdd] case class OrderedPartitionHeader[K, P <: Partition](
  partition: P,
  firstKey: K,
  secondKey: Option[K]
)

/**
 * :: DeveloperApi ::
 * Base class for range dependency.
 *
 * @param index   The index of a partition which it represents.
 * @param range   The range for all the records in the partition.
 * @param parents The sequence of parent partitions.
 */
private[rdd] case class RangeDependency[K, P <: Partition](
  index: Int,
  range: CloseOpen[K],
  parents: Seq[P]
) extends Serializable

private[rdd] trait PartitionNormalizationStrategy {
  /**
   * Return a sequence of close-open ranges from a sequence of [[OrderedPartitionHeader]]s that could be
   * used to define the ranges of normalized partitions.
   */
  def normalize[K, P <: Partition](
    headers: Seq[OrderedPartitionHeader[K, P]]
  )(
    implicit
    ord: Ordering[K]
  ): Seq[CloseOpen[K]]
}

private[rdd] object BasicNormalizationStrategy extends PartitionNormalizationStrategy {

  /**
   * This strategy works as follows:
   *
   * Consider an [[org.apache.spark.rdd.RDD]] and its partitions illustrated as follows where the numbers in `[]`
   * represent keys of rows.
   *
   * - partition 0: [1, 1, 2, ..., 4];
   * - partition 1: [4, ..., 4];
   * - partition 2: [4, 4, 5, ..., 7];
   * - partition 3: [7, 8, 8, ..., 12];
   * - partition 4: [13, 14, ..., 20];
   *
   * From the the first two distinct keys information per partition, we split the key space into non-overlap close-open ranges:
   * `[1, 5), [5, 8), [8, 14), [14, +infinity)`
   *
   * Then the [[RangeDependency]] for each of above range will be
   * - `[1, 5)` depends on partitions 0, 1, 2;
   * - `[5, 8)` depends on partitions 2, 3;
   * - `[8, 14)` depends on partitions 3, 4;
   * - `[14, +infinity)` depends on partitions 4.
   */
  override def normalize[K, P <: Partition](
    headers: Seq[OrderedPartitionHeader[K, P]]
  )(
    implicit
    ord: Ordering[K]
  ): Seq[CloseOpen[K]] = {
    // Collect all existing second keys which could be an empty set.
    val secondKeys = headers.filter(_.secondKey.isDefined).map {
      _.secondKey.get
    }.sorted.zipWithIndex.map(_.swap).toMap

    val firstKey = headers.head.firstKey
    if (headers.head.secondKey.isDefined) {
      // For the case that the first partition has multiple keys.
      CloseOpen(firstKey, secondKeys.get(1)) +: secondKeys.toSeq.filter(_._1 > 0).map {
        case (idx, secondKey) => CloseOpen(secondKey, secondKeys.get(idx + 1))
      }
    } else {
      // For the case that the first partition has only a single key.
      CloseOpen(firstKey, secondKeys.get(0)) +: secondKeys.toSeq.map {
        case (idx, secondKey) => CloseOpen(secondKey, secondKeys.get(idx + 1))
      }
    }
  }
}

private[rdd] object HeavyKeysNormalizationStrategy extends PartitionNormalizationStrategy {

  /**
   * This strategy is similar [[BasicNormalizationStrategy]], but it allocates a separate partition to rows with
   * key `k` if the original RDD has a separate partition for key `k`. This helps to preserve the original partition
   * split, but if you want to repartition your data - call repartition directly.
   *
   * Consider an [[org.apache.spark.rdd.RDD]] and it's partitions illustrated as follows where the numbers in `[]`
   * represent keys.
   *
   * - partition 0: [1, 1, 2, ..., 4];
   * - partition 1: [4, ..., 4];
   * - partition 2: [4, 4, 5, ..., 7];
   * - partition 3: [7, 8, 8, ..., 12];
   * - partition 4: [13, 14, ..., 20];
   *
   * To create a list of boundaries we take one boundary element from each partition: if a partition has at least
   * two different keys - then we take the second key, otherwise we take the first key:
   * 1, 4, 4, 5, 8, 14
   *
   * and filter out repeating numbers:
   * 1, 4, 5, 8, 14
   *
   * Then we split the key space into non-overlapping close-open ranges:
   * [1, 4)
   * [4, 5)
   * [5, 8)
   * [8, 14)
   * [14, +infinity)
   */
  override def normalize[K, P <: Partition](
    headers: Seq[OrderedPartitionHeader[K, P]]
  )(
    implicit
    ord: Ordering[K]
  ): Seq[CloseOpen[K]] = {
    val sortedHeaders = headers.sortBy(_.partition.index)

    val partitionBoundaries = sortedHeaders.head.firstKey +: sortedHeaders.tail.map {
      header => header.secondKey.getOrElse(header.firstKey)
    }

    val distinctBoundaries = partitionBoundaries.distinct
    val lastElement = distinctBoundaries.last

    val partitionIntervals = if (distinctBoundaries.size > 1) {
      distinctBoundaries.sliding(2).map {
        case Seq(begin, end) => CloseOpen(begin, Some(end))
      }.toSeq
    } else {
      Seq.empty
    }

    partitionIntervals :+ CloseOpen(lastElement, None)
  }
}
